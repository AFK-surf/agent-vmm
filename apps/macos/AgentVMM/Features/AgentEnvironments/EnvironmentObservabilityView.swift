import AppKit
import Charts
import SwiftUI

struct EnvironmentOverviewView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var samples: [UsageSnapshot] = []
    @State private var streamError: String?
    @State private var metricsReloadID = UUID()

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                inventoryNotice
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 280), spacing: 12)], spacing: 12) {
                    chartCard("CPU", value: cpuValue, hasData: !cpuPoints.isEmpty) {
                        Chart(cpuPoints) { point in
                            AreaMark(x: .value("Time", point.date), y: .value("Percent", point.value)).foregroundStyle(.purple.opacity(0.12))
                            LineMark(x: .value("Time", point.date), y: .value("Percent", point.value)).foregroundStyle(.purple)
                        }
                        .chartYAxis { AxisMarks(format: Decimal.FormatStyle.Percent.percent.scale(1)) }
                    }
                    chartCard("Memory", value: memoryValue, hasData: !freshSamples.isEmpty) {
                        Chart(freshSamples, id: \.sampledAt) { sample in
                            AreaMark(x: .value("Time", sample.sampledAt), y: .value("Bytes", sample.memoryBytes)).foregroundStyle(.blue.opacity(0.12))
                            LineMark(x: .value("Time", sample.sampledAt), y: .value("Bytes", sample.memoryBytes)).foregroundStyle(.blue)
                        }
                        .chartYAxis { AxisMarks(format: .byteCount(style: .memory)) }
                    }
                    rateCard("Disk I/O", value: diskValue, points: diskPoints, colors: ["Read": .cyan, "Written": .indigo])
                    rateCard("Network I/O", value: networkValue, points: networkPoints, colors: ["Received": .green, "Sent": .orange])
                }
                accessSummary
            }
            .frame(maxWidth: 1_050, alignment: .leading)
            .padding(24)
        }
        .task(id: "\(environment.id):\(model.snapshot.bootID):\(metricsReloadID)") { await observeMetrics() }
    }

    @ViewBuilder private var inventoryNotice: some View {
        if environment.inventoryAvailability != .available {
            GroupBox {
                Label(environment.inventoryMessage ?? "Live environment details are unavailable.", systemImage: environment.inventoryAvailability == .vmStopped ? "power" : "exclamationmark.triangle")
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .foregroundStyle(.secondary)
            }
        } else if let streamError {
            GroupBox {
                HStack {
                    Label(streamError, systemImage: "waveform.path.ecg").foregroundStyle(.secondary)
                    Spacer()
                    Button("Retry") { metricsReloadID = UUID() }
                }
            }
        } else if let sample = samples.last, sample.isStale || sample.unavailableReason != nil {
            GroupBox {
                Label(sample.unavailableReason ?? "Resource metrics are stale.", systemImage: "waveform.path.ecg")
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var accessSummary: some View {
        GroupBox("Environment") {
            VStack(alignment: .leading, spacing: 10) {
                LabeledContent("Owner", value: environment.origin.rawValue)
                LabeledContent("Containers", value: "\(environment.workloads.filter { $0.state == .running }.count) running · \(environment.workloads.count) total")
                LabeledContent("Images and volumes", value: "\(environment.volumes.count) retained volume\(environment.volumes.count == 1 ? "" : "s")")
                LabeledContent("Outbound access", value: environment.egressSummary)
            }
            .padding(.top, 6)
        }
    }

    private func observeMetrics() async {
        samples.removeAll()
        streamError = nil
        guard let client = model.client, model.snapshot.vmState == "Running" else { return }
        do {
            for try await sample in client.metrics(environmentID: environment.id) {
                if Task.isCancelled { return }
                samples.append(sample)
                if samples.count > 900 { samples.removeFirst(samples.count - 900) }
            }
        } catch {
            if !Task.isCancelled { streamError = hostErrorMessage(error) }
        }
    }

    private var cpuPoints: [MetricPoint] {
        zip(samples, samples.dropFirst()).compactMap { previous, current in
            current.rate(after: previous, for: \.cpuNanos).map { MetricPoint(date: current.sampledAt, value: $0 / 1_000_000_000) }
        }
    }
    private var freshSamples: [UsageSnapshot] { samples.filter { !$0.isStale && $0.unavailableReason == nil } }
    private var diskPoints: [SeriesPoint] { ratePoints(read: \.readBytes, write: \.writeBytes, readName: "Read", writeName: "Written") }
    private var networkPoints: [SeriesPoint] { ratePoints(read: \.networkRXBytes, write: \.networkTXBytes, readName: "Received", writeName: "Sent") }
    private func ratePoints(read: KeyPath<UsageSnapshot, UInt64>, write: KeyPath<UsageSnapshot, UInt64>, readName: String, writeName: String) -> [SeriesPoint] {
        zip(samples, samples.dropFirst()).flatMap { previous, current -> [SeriesPoint] in
            guard let readRate = current.rate(after: previous, for: read),
                  let writeRate = current.rate(after: previous, for: write) else { return [] }
            return [
                SeriesPoint(date: current.sampledAt, series: readName, value: readRate),
                SeriesPoint(date: current.sampledAt, series: writeName, value: writeRate)
            ]
        }
    }

    private var cpuValue: String { cpuPoints.last.map { ($0.value * 100).formatted(.number.precision(.fractionLength(1))) + "%" } ?? unavailableValue }
    private var memoryValue: String { freshSamples.last.map { DisplayFormatting.bytes.string(fromByteCount: Int64($0.memoryBytes)) } ?? unavailableValue }
    private var diskValue: String { latestRate(diskPoints) }
    private var networkValue: String { latestRate(networkPoints) }
    private var unavailableValue: String { environment.inventoryAvailability == .vmStopped ? "VM stopped" : "Waiting for data" }
    private func latestRate(_ points: [SeriesPoint]) -> String {
        guard let date = points.last?.date else { return unavailableValue }
        let value = points.filter { $0.date == date }.reduce(0) { $0 + $1.value }
        return byteRate(value)
    }
    private func byteRate(_ value: Double) -> String { DisplayFormatting.bytes.string(fromByteCount: Int64(value)) + "/s" }

    private func rateCard(_ title: String, value: String, points: [SeriesPoint], colors: KeyValuePairs<String, Color>) -> some View {
        chartCard(title, value: value, hasData: !points.isEmpty) {
            Chart(points) { point in
                LineMark(x: .value("Time", point.date), y: .value("Bytes per second", point.value)).foregroundStyle(by: .value("Direction", point.series))
            }
            .chartForegroundStyleScale(colors)
            .chartYAxis { AxisMarks { value in AxisGridLine(); AxisValueLabel { if let rate = value.as(Double.self) { Text(byteRate(rate)) } } } }
            .chartLegend(position: .bottom)
        }
    }

    private func chartCard<Content: View>(_ title: String, value: String, hasData: Bool, @ViewBuilder content: () -> Content) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 8) {
                HStack { Text(title).font(.headline); Spacer(); Text(value).font(.caption.monospacedDigit()).foregroundStyle(.secondary) }
                if !hasData {
                    ContentUnavailableView("Waiting for samples", systemImage: "chart.xyaxis.line").frame(height: 145)
                } else {
                    content().frame(height: 145)
                }
            }
            .padding(6)
        }
    }
}

struct EnvironmentContainersView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var selection: Workload.ID?
    @State private var mutatingIDs: Set<Workload.ID> = []
    @State private var pendingConfirmation: ContainerConfirmation?

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Containers").font(.headline)
                Spacer()
                Text("\(environment.workloads.filter { $0.state == .running }.count) running · \(environment.workloads.count) total").foregroundStyle(.secondary)
            }
            if environment.inventoryAvailability == .unknown {
                ProgressView("Loading container inventory…").frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if environment.inventoryAvailability != .available && environment.workloads.isEmpty {
                ContentUnavailableView("Container inventory unavailable", systemImage: "shippingbox", description: Text(environment.inventoryMessage ?? "Try again after the runtime is available."))
            } else {
                Table(environment.workloads, selection: $selection) {
                    TableColumn("Name", value: \.id)
                    TableColumn("State") { workload in Label(workload.state.rawValue.capitalized, systemImage: workload.state == .running ? "play.fill" : "stop.fill") }
                    TableColumn("Image", value: \.image)
                    TableColumn("Started") { workload in Text(workload.startedAt?.formatted(date: .abbreviated, time: .shortened) ?? "—") }
                    TableColumn("Exit") { Text($0.exitCode.map(String.init) ?? "—") }
                    TableColumn("Actions") { workload in
                        HStack(spacing: 6) {
                            primaryAction(for: workload)
                            Menu { actionMenu(for: workload) } label: { Image(systemName: "ellipsis") }
                                .menuStyle(.borderlessButton)
                                .frame(width: 28)
                                .accessibilityLabel("More actions for \(workload.id)")
                        }
                    }
                    .width(min: 115, ideal: 130)
                }
                .contextMenu(forSelectionType: Workload.ID.self) { ids in
                    if let id = ids.first, let workload = environment.workloads.first(where: { $0.id == id }) {
                        actionMenu(for: workload)
                    }
                } primaryAction: { _ in }
                .overlay {
                    if environment.workloads.isEmpty {
                        ContentUnavailableView("No containers", systemImage: "shippingbox", description: Text("Containers created in this environment will appear here."))
                    }
                }
                if environment.inventoryAvailability == .stale {
                    Label(environment.inventoryMessage ?? "Showing the last available container inventory.", systemImage: "clock.arrow.circlepath")
                        .font(.caption).foregroundStyle(.secondary)
                }
                if let selected = selectedWorkload { workloadDetails(selected) }
            }
        }
        .padding(20)
        .confirmationDialog(confirmationTitle, isPresented: confirmationPresented, titleVisibility: .visible) {
            if let confirmation = pendingConfirmation {
                Button(confirmation.buttonTitle, role: confirmation.isDestructive ? .destructive : nil) {
                    confirm(confirmation)
                }
                Button("Cancel", role: .cancel) { pendingConfirmation = nil }
            }
        } message: { Text(confirmationMessage) }
    }

    private var selectedWorkload: Workload? {
        selection.flatMap { id in environment.workloads.first { $0.id == id } }
    }

    @ViewBuilder private func primaryAction(for workload: Workload) -> some View {
        if mutatingIDs.contains(workload.id) {
            ProgressView().controlSize(.small).frame(width: 58)
        } else if workload.state == .running {
            Button("Stop") { mutate(workload, start: false) }
        } else if workload.state == .stopped || workload.state == .created {
            Button("Start") { mutate(workload, start: true) }
        }
    }

    @ViewBuilder private func actionMenu(for workload: Workload) -> some View {
        Button("View Details") { selection = workload.id }
        if mutatingIDs.contains(workload.id) {
            Text("Operation in progress…")
        } else {
        Divider()
        if workload.state == .running {
            Button("Stop") { mutate(workload, start: false) }
            Button("Interrupt") { signal(workload, .interrupt) }
            Button("Force Stop…") { pendingConfirmation = .forceStop(workload) }
        } else if workload.state == .stopped || workload.state == .created {
            Button("Start") { mutate(workload, start: true) }
            Divider()
            Button("Delete Container…", role: .destructive) { pendingConfirmation = .delete(workload) }
                .disabled(!canDelete(workload))
        }
        }
    }

    private func workloadDetails(_ workload: Workload) -> some View {
        GroupBox("Container details") {
            Grid(alignment: .leading, horizontalSpacing: 14, verticalSpacing: 8) {
                detailRow("Name", workload.id)
                detailRow("Image", workload.image)
                detailRow("Command", workload.argv.isEmpty ? "Image default" : workload.argv.joined(separator: " "))
                detailRow("Working directory", workload.workdir ?? "Image default")
                detailRow("User", workload.user ?? "Image default")
                detailRow("Environment", workload.environmentKeys.isEmpty ? "None" : workload.environmentKeys.joined(separator: ", "))
                detailRow("Volumes", workload.mounts.isEmpty ? "None" : workload.mounts.joined(separator: ", "))
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.top, 4)
        }
    }

    private func detailRow(_ label: String, _ value: String) -> some View {
        GridRow { Text(label).foregroundStyle(.secondary); Text(value).textSelection(.enabled) }
    }

    private func canDelete(_ workload: Workload) -> Bool {
        environment.inventoryAvailability == .available && !workload.generationID.isEmpty && (workload.state == .stopped || workload.state == .created)
    }

    private func mutate(_ workload: Workload, start: Bool) {
        mutatingIDs.insert(workload.id)
        Task {
            if start { await model.continueWorkload(environmentID: environment.id, containerID: workload.id) }
            else { await model.stop(environmentID: environment.id, containerID: workload.id) }
            mutatingIDs.remove(workload.id)
        }
    }

    private func signal(_ workload: Workload, _ signal: ContainerSignal) {
        mutatingIDs.insert(workload.id)
        Task {
            await model.signal(environmentID: environment.id, containerID: workload.id, signal: signal)
            mutatingIDs.remove(workload.id)
        }
    }

    private var confirmationPresented: Binding<Bool> {
        Binding(get: { pendingConfirmation != nil }, set: { if !$0 { pendingConfirmation = nil } })
    }
    private var confirmationTitle: String { pendingConfirmation?.title ?? "Confirm container action" }
    private var confirmationMessage: String { pendingConfirmation?.message ?? "" }
    private func confirm(_ confirmation: ContainerConfirmation) {
        pendingConfirmation = nil
        switch confirmation {
        case .forceStop(let workload): signal(workload, .kill)
        case .delete(let workload):
            guard canDelete(workload) else { return }
            mutatingIDs.insert(workload.id)
            Task {
                await model.deleteWorkload(environmentID: environment.id, containerID: workload.id, expectedGenerationID: workload.generationID)
                mutatingIDs.remove(workload.id)
                if selection == workload.id { selection = nil }
            }
        }
    }
}

private enum ContainerConfirmation {
    case forceStop(Workload)
    case delete(Workload)

    var title: String { switch self { case .forceStop(let value): "Force stop \(value.id)?"; case .delete(let value): "Delete \(value.id)?" } }
    var message: String { switch self {
    case .forceStop: "The container process will be killed immediately and may not finish writing data."
    case .delete: "This permanently deletes the container's writable snapshot and logs. Named volumes and image content are retained."
    } }
    var buttonTitle: String { switch self { case .forceStop: "Force Stop"; case .delete: "Delete Container" } }
    var isDestructive: Bool { true }
}

struct EnvironmentStorageView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var images: [EnvironmentImageModel] = []
    @State private var imageError: String?
    @State private var loadingImages = false
    @State private var imageReloadID = UUID()
    @State private var imagePendingRemoval: EnvironmentImageModel?
    @State private var removingImageIDs: Set<String> = []
    @State private var failedImageRemoval: EnvironmentImageModel?
    @State private var imageRemovalError: String?
    @State private var imageInventoryFresh = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Images available to this environment").font(.headline)
                Text("Environment namespace").font(.caption).padding(.horizontal, 8).padding(.vertical, 3)
                    .background(.quaternary, in: Capsule())
                Spacer()
                Button { imageReloadID = UUID() } label: { Label("Refresh", systemImage: "arrow.clockwise") }
                    .disabled(loadingImages)
            }
            Text("Image records belong to this environment. Content blobs are stored and deduplicated across the virtual machine, so removing a record may not free disk space.")
                .font(.caption).foregroundStyle(.secondary)
            if let imageError, !images.isEmpty {
                Label(imageError, systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.red)
            }
            if let imageRemovalError {
                HStack {
                    Label(imageRemovalError, systemImage: "exclamationmark.triangle")
                        .font(.caption).foregroundStyle(.red)
                    Spacer()
                    if let failedImageRemoval, !imageRemovalDisabled(failedImageRemoval) {
                        Button("Retry") { remove(failedImageRemoval) }
                    }
                    Button("Dismiss") {
                        self.imageRemovalError = nil
                        failedImageRemoval = nil
                    }
                }
            }
            Table(images) {
                TableColumn("Reference", value: \.reference)
                TableColumn("Content") { Text(shortDigest($0.digest)).font(.system(.body, design: .monospaced)) }
                TableColumn("Logical size") { Text(DisplayFormatting.bytes.string(fromByteCount: Int64($0.logicalBytes))) }
                TableColumn("Used by") { Text($0.usedByContainerIDs.isEmpty ? "Not in use" : $0.usedByContainerIDs.joined(separator: ", ")) }
                TableColumn("") { image in
                    Menu {
                        Button("Copy Reference") { copy(image.reference) }
                        Button("Copy Digest") { copy(image.digest) }
                        Divider()
                        Button("Remove from Environment…", role: .destructive) { imagePendingRemoval = image }
                            .disabled(imageRemovalDisabled(image))
                    } label: {
                        if removingImageIDs.contains(image.id) { ProgressView().controlSize(.small) }
                        else { Image(systemName: "ellipsis.circle") }
                    }
                    .menuStyle(.borderlessButton)
                    .help(imageRemovalHelp(image))
                }.width(38)
            }
            .overlay {
                if loadingImages || environment.inventoryAvailability == .unknown { ProgressView("Loading images…") }
                else if let imageError, images.isEmpty {
                    ContentUnavailableView {
                        Label("Image inventory unavailable", systemImage: "photo.stack")
                    } description: { Text(imageError) } actions: { Button("Retry") { imageReloadID = UUID() } }
                }
                else if environment.inventoryAvailability == .stale && images.isEmpty {
                    ContentUnavailableView("Last image inventory unavailable", systemImage: "clock.arrow.circlepath", description: Text(environment.inventoryMessage ?? "Refresh after the runtime is available."))
                }
                else if environment.inventoryAvailability == .available && images.isEmpty && imageError == nil { ContentUnavailableView("No images", systemImage: "photo.stack") }
            }
            Text("Retained volumes").font(.headline)
            Table(environment.volumes) {
                TableColumn("Name", value: \.id)
                TableColumn("Used") { volume in
                    Text(volume.usageIsStale ? (volume.usageUnavailableReason ?? "Sampling…") : DisplayFormatting.bytes.string(fromByteCount: Int64(volume.bytesUsed)))
                }
                TableColumn("Mounted by") { Text($0.mountedBy.isEmpty ? "Not mounted" : $0.mountedBy.joined(separator: ", ")) }
            }
            .frame(minHeight: 150, idealHeight: 210)
            .overlay {
                if environment.inventoryAvailability == .unknown {
                    ProgressView("Loading retained volumes…")
                } else if environment.inventoryAvailability != .available && environment.volumes.isEmpty {
                    ContentUnavailableView("Volume inventory unavailable", systemImage: "externaldrive", description: Text(environment.inventoryMessage ?? "Try again after the runtime is available."))
                } else if environment.volumes.isEmpty {
                    ContentUnavailableView("No retained volumes", systemImage: "externaldrive")
                }
            }
            if environment.inventoryAvailability == .stale {
                HStack {
                    Label(staleInventoryMessage, systemImage: "clock.arrow.circlepath")
                        .font(.caption).foregroundStyle(.secondary)
                    Spacer()
                    Button("Retry") { Task { await loadImages(force: true) } }
                }
            }
        }
        .padding(20)
        .task(id: "\(environment.id):\(environment.inventoryAvailability):\(model.snapshot.bootID):\(imageReloadID)") { await loadImages(force: false) }
        .alert("Remove image record from this environment?", isPresented: removalAlertPresented, presenting: imagePendingRemoval) { image in
            Button("Cancel", role: .cancel) { imagePendingRemoval = nil }
            Button("Remove Image Record", role: .destructive) { remove(image) }
        } message: { _ in
            Text("This removes the reference from this environment’s namespace. Containers and volumes are not deleted. The underlying content may remain because it can be shared elsewhere in the virtual machine. Reclaimed disk space is unknown.")
        }
    }

    private var staleInventoryMessage: String {
        let observed = environment.inventoryObservedAt.map { " Last updated \($0.formatted(date: .abbreviated, time: .shortened))." } ?? ""
        return (environment.inventoryMessage ?? "Showing the last available storage inventory.") + observed
    }

    private func loadImages(force: Bool) async {
        if environment.inventoryAvailability == .unknown {
            loadingImages = true
            imageInventoryFresh = false
            imageError = nil
            return
        }
        if environment.inventoryAvailability == .stale && !force {
            loadingImages = false
            imageInventoryFresh = false
            return
        }
        guard let client = model.client, environment.inventoryAvailability == .available || force && environment.inventoryAvailability == .stale else {
            loadingImages = false
            imageInventoryFresh = false
            images.removeAll()
            imageError = environment.inventoryMessage ?? (environment.inventoryAvailability == .vmStopped ? "Start the virtual machine to inspect images." : "The runtime image inventory is unavailable.")
            return
        }
        loadingImages = true
        imageInventoryFresh = false
        defer { loadingImages = false }
        do {
            images = try await client.images(environmentID: environment.id)
            imageError = nil
            imageInventoryFresh = true
            if let failedImageRemoval { self.failedImageRemoval = images.first { $0.id == failedImageRemoval.id } }
        } catch {
            imageError = hostErrorMessage(error)
            imageInventoryFresh = false
        }
    }
    private var removalAlertPresented: Binding<Bool> {
        Binding(get: { imagePendingRemoval != nil }, set: { if !$0 { imagePendingRemoval = nil } })
    }
    private func imageRemovalDisabled(_ image: EnvironmentImageModel) -> Bool {
        imageRemovalAvailability(for: image, inventory: effectiveImageInventory, isRemoving: removingImageIDs.contains(image.id)) != .allowed
    }
    private func imageRemovalHelp(_ image: EnvironmentImageModel) -> String {
        switch imageRemovalAvailability(for: image, inventory: effectiveImageInventory, isRemoving: removingImageIDs.contains(image.id)) {
        case .inUse(let count):
            return "Used by \(count) container\(count == 1 ? "" : "s"). Delete or replace those containers first."
        case .inventoryUnavailable:
            return "Refresh the image inventory before removing this record."
        case .inProgress:
            return "This image record is being removed."
        case .allowed:
            return "Copy image identity or remove its record from this environment."
        }
    }
    private var effectiveImageInventory: InventoryAvailability {
        environment.inventoryAvailability == .available && imageInventoryFresh && !loadingImages ? .available : .stale
    }
    private func remove(_ image: EnvironmentImageModel) {
        imagePendingRemoval = nil
        removingImageIDs.insert(image.id)
        Task {
            defer { removingImageIDs.remove(image.id) }
            do {
                try await model.deleteImage(environmentID: environment.id, image: image)
                images.removeAll { $0.id == image.id }
                imageError = nil
                imageRemovalError = nil
                failedImageRemoval = nil
            } catch {
                imageRemovalError = hostErrorMessage(error)
                failedImageRemoval = image
                imageReloadID = UUID()
            }
        }
    }
    private func copy(_ value: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }
    private func shortDigest(_ value: String) -> String { value.count > 20 ? String(value.prefix(20)) + "…" : value }
}

private struct MetricPoint: Identifiable { let date: Date; let value: Double; var id: Date { date } }
private struct SeriesPoint: Identifiable { let date: Date; let series: String; let value: Double; var id: String { "\(date.timeIntervalSince1970)-\(series)" } }
