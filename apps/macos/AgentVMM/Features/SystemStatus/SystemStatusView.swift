import Charts
import SwiftUI

struct SystemStatusView: View {
    @Bindable var model: AppModel
    @State private var requestedStop: Bool?

    private let metricColumns = [GridItem(.adaptive(minimum: 180), spacing: 12)]

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                vmCard

                if let lifecycle = model.lifecycleStatus, !model.servicesHealthy {
                    serviceRepair(lifecycle)
                }

                accessSummary

                LazyVGrid(columns: metricColumns, alignment: .leading, spacing: 12) {
                    metric("Agent environments", "\(model.snapshot.environments.count)", "Available to local agents", symbol: "shippingbox")
                    metric(
                        "Memory in use",
                        model.snapshot.usage.map { DisplayFormatting.bytes.string(fromByteCount: Int64($0.memoryBytes)) } ?? "—",
                        unavailableMetricCaption,
                        symbol: "memorychip"
                    )
                    metric(
                        "Retained data",
                        model.snapshot.usage.map { DisplayFormatting.bytes.string(fromByteCount: Int64($0.diskBytes)) } ?? "—",
                        unavailableMetricCaption,
                        symbol: "externaldrive"
                    )
                }

                if !model.snapshot.components.isEmpty {
                    componentSection
                }

                if !model.metrics.isEmpty {
                    trendsSection
                }
            }
            .frame(maxWidth: 1040, alignment: .leading)
            .padding(28)
        }
        .navigationTitle("Overview")
        .overlay { if model.isLoading { ProgressView("Connecting to Agent VMM…") } }
        .alert(requestedStop == true ? "Force stop virtual machine?" : "Stop virtual machine?", isPresented: Binding(
            get: { requestedStop != nil }, set: { if !$0 { requestedStop = nil } }
        )) {
            Button("Cancel", role: .cancel) { requestedStop = nil }
            Button(requestedStop == true ? "Force Stop" : "Stop", role: .destructive) {
                let force = requestedStop == true
                requestedStop = nil
                Task { await model.stopVM(force: force) }
            }
        } message: {
            Text(vmStopImpact)
        }
    }

    private func serviceRepair(_: LifecycleStatus) -> some View {
        GroupBox {
            HStack(spacing: 14) {
                Image(systemName: "wrench.and.screwdriver.fill").foregroundStyle(.orange)
                VStack(alignment: .leading, spacing: 3) {
                    Text("Agent VMM services need repair").font(.headline)
                    Text("One or more signed helpers or LaunchAgents are missing or stopped.").foregroundStyle(.secondary)
                }
                Spacer()
                Button("Repair Services") { Task { await model.repairServices() } }
                    .buttonStyle(.borderedProminent)
                    .disabled(model.lifecycleOperationInProgress)
            }.padding(6)
        }
    }

    private var accessSummary: some View {
        GroupBox("Agent access") {
            VStack(spacing: 0) {
                Button { model.showAccess(.local) } label: {
                    HStack {
                        Label("Local agents", systemImage: "bolt.horizontal.circle")
                        Spacer()
                        Text("\(readyIntegrationCount) ready · \(inUseIntegrationCount) in use")
                            .foregroundStyle(.secondary)
                        Image(systemName: "chevron.right").foregroundStyle(.tertiary)
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .padding(.vertical, 9)
                Divider()
                Button { model.showAccess(.remote) } label: {
                    HStack {
                        Label("Remote controller", systemImage: "network")
                        Spacer()
                        Text(remoteAccessSummary).foregroundStyle(.secondary)
                        Image(systemName: "chevron.right").foregroundStyle(.tertiary)
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .padding(.vertical, 9)
            }
            .padding(.horizontal, 6)
        }
    }

    private var remoteAccessSummary: String {
        guard let remote = model.snapshot.remoteRegistration else { return "Not configured" }
        return "\(remote.connectionState.rawValue) · \(remote.state.rawValue)"
    }

    private var readyIntegrationCount: Int {
        model.integrations.filter { $0.configState == .ready }.count
    }

    private var inUseIntegrationCount: Int {
        Set(model.sessions.filter { $0.activeConnections > 0 }.compactMap(\.integrationID)).count
    }

    private var vmCard: some View {
        GroupBox {
            HStack(spacing: 18) {
                Image(systemName: vmSymbol)
                    .font(.system(size: 30, weight: .medium))
                    .foregroundStyle(vmColor)
                    .frame(width: 42)
                    .symbolEffect(.pulse, isActive: isTransitioning)

                VStack(alignment: .leading, spacing: 5) {
                    HStack(spacing: 8) {
                        Text("Virtual machine").font(.title2.weight(.semibold))
                        Text(vmDisplayState)
                            .font(.caption.weight(.semibold))
                            .padding(.horizontal, 8)
                            .padding(.vertical, 3)
                            .foregroundStyle(vmColor)
                            .background(vmColor.opacity(0.12), in: Capsule())
                    }
                    Text(vmExplanation).foregroundStyle(.secondary)
                    Text(DisplayFormatting.age(of: model.snapshot.collectedAt))
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }

                Spacer(minLength: 24)
                vmAction
            }
            .padding(8)
        }
        .accessibilityElement(children: .contain)
    }

    @ViewBuilder
    private var vmAction: some View {
        if isTransitioning {
            Button(action: {}) {
                HStack(spacing: 7) {
                    ProgressView().controlSize(.small)
                    Text(vmDisplayState)
                }
            }
            .disabled(true)
        } else if model.snapshot.vmState == "Stopped" || model.snapshot.vmState == "Failed" {
            Button("Start", systemImage: "play.fill") { Task { await model.startVM() } }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.return, modifiers: [.command])
        } else if model.snapshot.vmState == "Running" {
            Button("Stop…", systemImage: "stop.fill") { requestedStop = false }
                .buttonStyle(.bordered)
        }

        if model.gracefulStopFailed && !isTransitioning {
            Button("Force Stop…", role: .destructive) { requestedStop = true }
        }
    }

    private var componentSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Components").font(.title2.weight(.semibold))
            GroupBox {
                VStack(spacing: 0) {
                    ForEach(Array(model.snapshot.components.enumerated()), id: \.element.id) { index, component in
                        HStack(spacing: 12) {
                            HealthLabel(level: component.level)
                            Text(componentName(component.id)).font(.headline)
                            Spacer()
                            Text(component.reason ?? DisplayFormatting.age(of: component.observedAt))
                                .foregroundStyle(.secondary)
                                .lineLimit(2)
                        }
                        .padding(.vertical, 10)
                        if index != model.snapshot.components.count - 1 { Divider() }
                    }
                }
                .padding(.horizontal, 6)
            }
        }
    }

    private var trendsSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Recent resource use").font(.title2.weight(.semibold))
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 220), spacing: 12)], alignment: .leading, spacing: 12) {
                trendCard("CPU", value: latestCPUDescription) {
                    Chart(cpuTrend) { point in
                        AreaMark(x: .value("Time", point.timestamp), y: .value("Percent", point.value))
                            .foregroundStyle(.purple.opacity(0.12))
                        LineMark(x: .value("Time", point.timestamp), y: .value("Percent", point.value))
                            .foregroundStyle(.purple)
                    }
                    .chartYAxis {
                        AxisMarks { value in
                            AxisGridLine()
                            AxisValueLabel { if let percent = value.as(Double.self) { Text("\(percent, format: .number.precision(.fractionLength(0)))%") } }
                        }
                    }
                }

                trendCard("Memory", value: latestMemoryDescription) {
                    Chart(recentMetrics, id: \.sampledAt) { sample in
                        AreaMark(x: .value("Time", sample.sampledAt), y: .value("Bytes", sample.memoryBytes))
                            .foregroundStyle(.blue.opacity(0.12))
                        LineMark(x: .value("Time", sample.sampledAt), y: .value("Bytes", sample.memoryBytes))
                            .foregroundStyle(.blue)
                    }
                    .chartYAxis { AxisMarks(format: .byteCount(style: .memory)) }
                }

                ioTrendCard("Disk I/O", value: latestDiskDescription, points: diskTrend, colors: ["Read": .cyan, "Written": .indigo])
                ioTrendCard("Network I/O", value: latestNetworkDescription, points: networkTrend, colors: ["Received": .green, "Sent": .orange])
            }
        }
    }

    private func ioTrendCard(_ title: String, value: String, points: [TrendPoint], colors: KeyValuePairs<String, Color>) -> some View {
        trendCard(title, value: value) {
            Chart(points) { point in
                LineMark(x: .value("Time", point.timestamp), y: .value("Bytes per second", point.value))
                    .foregroundStyle(by: .value("Direction", point.series))
            }
            .chartForegroundStyleScale(colors)
            .chartYAxis {
                AxisMarks { value in
                    AxisGridLine()
                    AxisValueLabel { if let rate = value.as(Double.self) { Text(byteRate(rate)) } }
                }
            }
            .chartLegend(position: .bottom, spacing: 8)
        }
    }

    private func trendCard<Content: View>(_ title: String, value: String, @ViewBuilder content: () -> Content) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 8) {
                HStack(alignment: .firstTextBaseline) {
                    Text(title).font(.headline)
                    Spacer()
                    Text(value).font(.caption.monospacedDigit()).foregroundStyle(.secondary)
                }
                content().frame(height: 150)
            }
            .padding(6)
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Recent \(title.lowercased()) use")
        .accessibilityValue(value)
    }

    private func metric(_ title: String, _ value: String, _ caption: String, symbol: String) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 8) {
                Label(title, systemImage: symbol).foregroundStyle(.secondary)
                Text(value).font(.title2.weight(.semibold))
                Text(caption).font(.caption).foregroundStyle(.tertiary).lineLimit(1)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(4)
        }
        .accessibilityElement(children: .combine)
    }

    private var isTransitioning: Bool {
        switch vmPhase {
        case .starting, .stopping, .forceStopping: true
        default: false
        }
    }

    private var vmDisplayState: String {
        switch vmPhase {
        case .running: "Running"
        case .stopped: "Stopped"
        case .starting: "Starting…"
        case .stopping: "Stopping…"
        case .forceStopping: "Force stopping…"
        case .failed: "Failed"
        case .unknown: model.snapshot.vmState
        }
    }

    private var vmExplanation: String {
        switch vmPhase {
        case .running: "Agent environments and retained data are available."
        case .stopped: "Ready to start when an agent needs an environment."
        case .starting: "Preparing the guest runtime and restoring policies."
        case .stopping: "Stopping workloads and flushing retained data."
        case .forceStopping: "Stopping the virtual machine immediately."
        case .failed: "The virtual machine needs attention before it can run."
        case .unknown: "Waiting for the local control service."
        }
    }

    private var vmSymbol: String {
        switch vmPhase {
        case .running: model.snapshot.health.symbolName
        case .starting, .stopping, .forceStopping: "arrow.trianglehead.2.clockwise.rotate.90"
        case .failed: "xmark.octagon.fill"
        case .stopped: "power"
        case .unknown: "questionmark.circle"
        }
    }

    private var vmColor: Color {
        switch vmPhase {
        case .running: model.snapshot.health.color
        case .starting: .blue
        case .stopping, .forceStopping: .orange
        case .failed: .red
        case .stopped, .unknown: .secondary
        }
    }

    private var unavailableMetricCaption: String {
        vmPhase == .stopped ? "Available while the VM is running" : "Waiting for a fresh sample"
    }

    private var vmPhase: VMPhase {
        if let action = model.vmAction {
            return switch action {
            case .starting: .starting
            case .stopping: .stopping
            case .forceStopping: .forceStopping
            }
        }
        return switch model.snapshot.vmState {
        case "Running": .running
        case "Stopped": .stopped
        case "Starting": .starting
        case "Stopping": .stopping
        case "Failed": .failed
        default: .unknown
        }
    }

    private var latestMemoryDescription: String {
        guard let sample = model.metrics.last else { return "No samples" }
        return DisplayFormatting.bytes.string(fromByteCount: Int64(sample.memoryBytes))
    }

    private var recentMetrics: [UsageSnapshot] {
        Array(model.metrics.suffix(120))
    }

    private var cpuTrend: [TrendPoint] {
        rates(for: \UsageSnapshot.cpuNanos).map {
            TrendPoint(timestamp: $0.timestamp, value: $0.value / 1_000_000_000 * 100)
        }
    }

    private var diskTrend: [TrendPoint] {
        rates(for: \UsageSnapshot.readBytes).map { TrendPoint(timestamp: $0.timestamp, value: $0.value, series: "Read") }
            + rates(for: \UsageSnapshot.writeBytes).map { TrendPoint(timestamp: $0.timestamp, value: $0.value, series: "Written") }
    }

    private var networkTrend: [TrendPoint] {
        rates(for: \UsageSnapshot.networkRXBytes).map { TrendPoint(timestamp: $0.timestamp, value: $0.value, series: "Received") }
            + rates(for: \UsageSnapshot.networkTXBytes).map { TrendPoint(timestamp: $0.timestamp, value: $0.value, series: "Sent") }
    }

    private var latestCPUDescription: String {
        guard let value = cpuTrend.last?.value else { return "Collecting…" }
        return "\(value.formatted(.number.precision(.fractionLength(0))))%"
    }

    private var latestDiskDescription: String {
        latestRateDescription(diskTrend, inbound: "Read", outbound: "Written")
    }

    private var latestNetworkDescription: String {
        latestRateDescription(networkTrend, inbound: "Received", outbound: "Sent")
    }

    private func latestRateDescription(_ points: [TrendPoint], inbound: String, outbound: String) -> String {
        let incoming = points.last { $0.series == inbound }?.value ?? 0
        let outgoing = points.last { $0.series == outbound }?.value ?? 0
        return "↓ \(byteRate(incoming))  ↑ \(byteRate(outgoing))"
    }

    private func rates(for keyPath: KeyPath<UsageSnapshot, UInt64>) -> [TrendPoint] {
        zip(recentMetrics, recentMetrics.dropFirst()).compactMap { previous, current in
            guard let value = current.rate(after: previous, for: keyPath) else { return nil }
            return TrendPoint(timestamp: current.sampledAt, value: value)
        }
    }

    private func byteRate(_ bytesPerSecond: Double) -> String {
        "\(DisplayFormatting.bytes.string(fromByteCount: Int64(bytesPerSecond)))⁄s"
    }

    private func componentName(_ identifier: String) -> String {
        switch identifier.lowercased() {
        case "vmmd": "Environment service"
        case "containerd", "runsc", "containerd/runsc": "Workload runtime"
        case "network": "Network"
        case "egress": "Guest egress"
        case "host-egress": "Host egress"
        case "quota": "Storage quotas"
        case "state-disk", "state_disk": "Retained data disk"
        default: "System component"
        }
    }

    private var vmStopImpact: String {
        let running = model.snapshot.environments.flatMap { environment in
            environment.workloads.filter { $0.state == .running }.map { "\(environment.name)/\($0.id)" }
        }
        let workloads = running.isEmpty ? "No running workloads are currently reported." : "Running workloads: \(running.joined(separator: ", "))."
        if requestedStop == true {
            return "Graceful shutdown failed. Force stop can leave workspace data inconsistent; named retained data remains. \(workloads)"
        }
        return "The guest will stop running workloads and flush retained data before shutdown. \(workloads)"
    }
}

private struct TrendPoint: Identifiable {
    let timestamp: Date
    let value: Double
    var series = ""

    var id: String { "\(series)-\(timestamp.timeIntervalSinceReferenceDate)" }
}

private enum VMPhase {
    case running
    case stopped
    case starting
    case stopping
    case forceStopping
    case failed
    case unknown
}
