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
                }
                .contextMenu(forSelectionType: Workload.ID.self) { ids in
                    if let id = ids.first, let workload = environment.workloads.first(where: { $0.id == id }) {
                        if mutatingIDs.contains(id) {
                            Text(workload.state == .running ? "Stopping…" : "Starting…")
                        } else if workload.state == .running {
                            Button("Stop") { mutate(workload, start: false) }
                        } else if workload.state == .stopped || workload.state == .created {
                            Button("Start") { mutate(workload, start: true) }
                        }
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
            }
        }
        .padding(20)
    }

    private func mutate(_ workload: Workload, start: Bool) {
        mutatingIDs.insert(workload.id)
        Task {
            if start { await model.continueWorkload(environmentID: environment.id, containerID: workload.id) }
            else { await model.stop(environmentID: environment.id, containerID: workload.id) }
            mutatingIDs.remove(workload.id)
        }
    }
}

struct EnvironmentStorageView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var images: [EnvironmentImageModel] = []
    @State private var imageError: String?
    @State private var loadingImages = false
    @State private var imageReloadID = UUID()

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Images").font(.headline)
            Table(images) {
                TableColumn("Reference", value: \.reference)
                TableColumn("Content") { Text(shortDigest($0.digest)).font(.system(.body, design: .monospaced)) }
                TableColumn("Logical size") { Text(DisplayFormatting.bytes.string(fromByteCount: Int64($0.logicalBytes))) }
                TableColumn("Used by") { Text($0.usedByContainerIDs.isEmpty ? "Not in use" : $0.usedByContainerIDs.joined(separator: ", ")) }
            }
            .overlay {
                if loadingImages { ProgressView("Loading images…") }
                else if let imageError {
                    ContentUnavailableView {
                        Label("Image inventory unavailable", systemImage: "photo.stack")
                    } description: { Text(imageError) } actions: { Button("Retry") { imageReloadID = UUID() } }
                }
                else if images.isEmpty { ContentUnavailableView("No images", systemImage: "photo.stack") }
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
                Label(environment.inventoryMessage ?? "Showing the last available storage inventory.", systemImage: "clock.arrow.circlepath")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
        .padding(20)
        .task(id: "\(environment.id):\(environment.inventoryAvailability):\(model.snapshot.bootID):\(imageReloadID)") { await loadImages() }
    }

    private func loadImages() async {
        loadingImages = true
        defer { loadingImages = false }
        guard let client = model.client, environment.inventoryAvailability == .available else {
            imageError = environment.inventoryMessage
            return
        }
        do { images = try await client.images(environmentID: environment.id); imageError = nil }
        catch { imageError = hostErrorMessage(error) }
    }
    private func shortDigest(_ value: String) -> String { value.count > 20 ? String(value.prefix(20)) + "…" : value }
}

private struct MetricPoint: Identifiable { let date: Date; let value: Double; var id: Date { date } }
private struct SeriesPoint: Identifiable { let date: Date; let series: String; let value: Double; var id: String { "\(date.timeIntervalSince1970)-\(series)" } }
