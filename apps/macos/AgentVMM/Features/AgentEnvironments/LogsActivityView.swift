import AppKit
import SwiftUI

struct LogsActivityView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel

    @State private var selectedWorkloadID: String?
    @State private var records: [LogEntry] = []
    @State private var rendered: [String: AttributedString] = [:]
    @State private var ansiParsers: [String: ANSIParser] = [:]
    @State private var search = ""
    @State private var presentation = LogPresentation.logs
    @State private var range = LogRange.live
    @State private var streamFilter = LogStreamFilter.all
    @State private var newestFirst = false
    @State private var caseSensitive = false
    @State private var paused = false
    @State private var pausedCount = 0
    @State private var evictedRecords = 0
    @State private var follow = true
    @State private var searchNavigation = 0
    @State private var connectionStatus = "Choose a container"
    @State private var streamTask: Task<Void, Never>?

    private var visibleRecords: [LogEntry] {
        let source = paused ? Array(records.prefix(pausedCount)) : records
        let cutoff = range.cutoff
        var result = source.filter { record in
            (cutoff == nil || record.timestamp >= cutoff!) &&
            (presentation == .terminal || streamFilter.matches(record.stream)) &&
            (presentation == .terminal || search.isEmpty || matchesSearch(record.text))
        }
        if presentation == .logs && newestFirst { result.reverse() }
        return result
    }

    var body: some View {
        Group {
            if environment.inventoryAvailability == .unknown {
                ProgressView("Loading containers…")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if environment.inventoryAvailability == .vmStopped || environment.inventoryAvailability == .unavailable {
                runtimeUnavailableState
            } else if environment.workloads.isEmpty {
                noContainersState
            } else {
                VStack(alignment: .leading, spacing: 10) {
                    toolbar
                    statusBar
                    warningBanners
                    content
                }
            }
        }
        .onAppear {
            if logRuntimeAvailable, selectedWorkloadID == nil { selectedWorkloadID = environment.workloads.first?.id }
        }
        .onChange(of: selectedWorkloadID) { _, value in start(workloadID: value) }
        .onChange(of: range) { _, _ in start(workloadID: selectedWorkloadID) }
        .onChange(of: newestFirst) { _, value in if value { follow = false } }
        .onChange(of: paused) { _, value in if value { pausedCount = records.count } }
        .onDisappear { streamTask?.cancel() }
        .onChange(of: environment.workloads.map(\.id)) { _, ids in
            if let selectedWorkloadID, !ids.contains(selectedWorkloadID) { self.selectedWorkloadID = ids.first }
            else if logRuntimeAvailable, selectedWorkloadID == nil { selectedWorkloadID = ids.first }
        }
        .onChange(of: environment.inventoryAvailability) { _, _ in
            if logRuntimeAvailable {
                if selectedWorkloadID == nil { selectedWorkloadID = environment.workloads.first?.id }
                else { start(workloadID: selectedWorkloadID) }
            } else {
                streamTask?.cancel()
                records.removeAll()
                rendered.removeAll()
                ansiParsers.removeAll()
                connectionStatus = "Runtime unavailable"
            }
        }
    }

    private var logRuntimeAvailable: Bool {
        environment.inventoryAvailability == .available || environment.inventoryAvailability == .stale
    }

    @ViewBuilder private var noContainersState: some View {
        ContentUnavailableView {
            Label("No containers", systemImage: "shippingbox")
        } description: {
            Text("Logs and terminal output appear after a container is created in this environment.")
        } actions: {
            Button("Open Containers") { model.environmentSection = .containers }
        }
        .frame(maxWidth: 420, maxHeight: .infinity)
        .frame(maxWidth: .infinity)
    }

    private var runtimeUnavailableState: some View {
        ContentUnavailableView {
            Label(environment.inventoryAvailability == .vmStopped ? "Virtual machine is stopped" : "Container runtime unavailable", systemImage: "power")
        } description: {
            Text(environment.inventoryMessage ?? "Logs are available after the runtime reconnects.")
        } actions: {
            if environment.inventoryAvailability == .vmStopped {
                Button("Start Virtual Machine") { Task { await model.startVM() } }
                    .disabled(model.vmAction != nil)
            }
        }
        .frame(maxWidth: 420, maxHeight: .infinity)
        .frame(maxWidth: .infinity)
    }

    private var toolbar: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 12) {
                Picker("Container", selection: $selectedWorkloadID) {
                    Text("Choose…").tag(String?.none)
                    ForEach(environment.workloads) { Text($0.id).tag(Optional($0.id)) }
                }
                .frame(minWidth: 220, idealWidth: 280, maxWidth: 340)
                Picker("Output view", selection: $presentation) {
                    ForEach(LogPresentation.allCases) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .accessibilityLabel("Output view")
                .frame(width: 210)
                Spacer(minLength: 0)
            }
            if selectedWorkloadID != nil {
                HStack(spacing: 10) {
                    Picker("Time range", selection: $range) {
                        ForEach(LogRange.allCases) { Text($0.rawValue).tag($0) }
                    }
                    .frame(width: 145)
                    TextField(presentation == .logs ? "Filter logs" : "Find in terminal output", text: $search)
                        .textFieldStyle(.roundedBorder)
                        .frame(minWidth: 160)
                    Toggle("Match case", isOn: $caseSensitive).toggleStyle(.button).help("Match uppercase and lowercase exactly")
                    if presentation == .terminal {
                        Button { searchNavigation -= 1 } label: { Image(systemName: "chevron.up") }
                            .help("Previous match").disabled(search.isEmpty)
                        Button { searchNavigation += 1 } label: { Image(systemName: "chevron.down") }
                            .help("Next match").disabled(search.isEmpty)
                    }
                }
                ViewThatFits(in: .horizontal) {
                    HStack(spacing: 10) {
                        presentationControls
                        Spacer(minLength: 8)
                        playbackControls
                    }
                    VStack(alignment: .leading, spacing: 8) {
                        presentationControls
                        HStack { Spacer(); playbackControls }
                    }
                }
            }
        }
    }

    @ViewBuilder private var presentationControls: some View {
        if presentation == .logs {
                Picker("Stream", selection: $streamFilter) {
                    ForEach(LogStreamFilter.allCases) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.segmented)
                .frame(width: 230)
                Toggle(newestFirst ? "Newest first" : "Oldest first", isOn: $newestFirst).toggleStyle(.button)
        } else {
            HStack(spacing: 8) {
                Label("Read-only", systemImage: "lock").foregroundStyle(.secondary)
                if !search.isEmpty {
                    Text(terminalMatchCount == 0 ? "No matches" : "\(terminalMatchCount) match\(terminalMatchCount == 1 ? "" : "es")")
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    @ViewBuilder private var playbackControls: some View {
        Group {
            Toggle("Follow", isOn: $follow).toggleStyle(.button).disabled(newestFirst || paused || connectionStatus == "Loading history…")
            Toggle(paused ? "Resume" : "Pause", isOn: $paused).toggleStyle(.button).disabled(connectionStatus == "Loading history…")
            Button("Export Visible…", action: export).disabled(records.isEmpty)
        }
        .fixedSize()
    }

    private var statusBar: some View {
        HStack(spacing: 14) {
            Text(paused ? "Paused · \(max(0, records.count - pausedCount)) new" : connectionStatus)
            Spacer()
            Text("\(visibleRecords.count) shown · \(records.count) buffered")
        }
        .font(.caption)
        .foregroundStyle(.secondary)
    }

    @ViewBuilder private var warningBanners: some View {
        let dropped = records.map(\.droppedRecords).max() ?? 0
        if records.contains(where: \.truncated) || dropped > 0 || evictedRecords > 0 {
            VStack(alignment: .leading, spacing: 6) {
                if records.contains(where: \.truncated) {
                    warning("One or more log records were truncated at the source.")
                }
                if dropped > 0 { warning("The source dropped \(dropped) log record\(dropped == 1 ? "" : "s").") }
                if evictedRecords > 0 { warning("This window removed \(evictedRecords) older record\(evictedRecords == 1 ? "" : "s") from its local buffer.") }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func warning(_ message: String) -> some View {
        Label(message, systemImage: "exclamationmark.triangle.fill")
            .font(.caption).foregroundStyle(.orange)
            .fixedSize(horizontal: false, vertical: true)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 10).padding(.vertical, 6)
            .background(.orange.opacity(0.09), in: RoundedRectangle(cornerRadius: 7))
    }

    private var terminalMatchCount: Int {
        guard !search.isEmpty else { return 0 }
        let source = visibleRecords.map { ANSIParser.plain($0.text) }.joined(separator: "\n") as NSString
        var count = 0
        var remaining = NSRange(location: 0, length: source.length)
        let options: NSString.CompareOptions = caseSensitive ? [] : [.caseInsensitive]
        while remaining.length > 0 {
            let match = source.range(of: search, options: options, range: remaining)
            guard match.location != NSNotFound else { break }
            count += 1
            let next = NSMaxRange(match)
            remaining = NSRange(location: next, length: source.length - next)
        }
        return count
    }

    @ViewBuilder private var content: some View {
        if selectedWorkloadID == nil {
            ContentUnavailableView("Choose a container", systemImage: "text.alignleft")
        } else if records.isEmpty && connectionStatus == "Loading history…" {
            ProgressView("Loading history…").frame(maxWidth: .infinity, maxHeight: .infinity)
        } else if records.isEmpty && connectionStatus.hasPrefix("Failed") {
            ContentUnavailableView {
                Label("Logs unavailable", systemImage: "exclamationmark.triangle")
            } description: {
                Text(connectionStatus.replacingOccurrences(of: "Failed · ", with: ""))
            } actions: {
                Button("Retry") { start(workloadID: selectedWorkloadID) }
            }
        } else if visibleRecords.isEmpty && presentation == .logs && !search.isEmpty {
            ContentUnavailableView {
                Label("No matching logs", systemImage: "magnifyingglass")
            } description: {
                Text("No visible log records contain “\(search)”.")
            } actions: {
                Button("Clear Search") { search = "" }
            }
        } else if visibleRecords.isEmpty {
            ContentUnavailableView("No logs in this range", systemImage: "text.alignleft", description: Text("Change the time range or filter."))
        } else if presentation == .logs {
            structuredLogList
        } else {
            terminalLog
        }
    }

    private var structuredLogList: some View {
        List(visibleRecords) { record in
            HStack(alignment: .top, spacing: 10) {
                Text(record.timestamp, format: .dateTime.hour().minute().second()).foregroundStyle(.secondary).monospacedDigit()
                Label(record.stream, systemImage: record.stream == "stderr" ? "exclamationmark.circle" : "arrow.right.circle")
                    .foregroundStyle(record.stream == "stderr" ? .red : .secondary)
                    .frame(width: 82, alignment: .leading)
                Text(rendered[record.id] ?? AttributedString(record.text)).textSelection(.enabled)
                    .font(.system(.body, design: .monospaced))
            }
            .accessibilityElement(children: .combine)
            .accessibilityLabel("\(record.timestamp.formatted(date: .omitted, time: .standard)), \(record.stream), \(ANSIParser.plain(record.text))")
        }
    }

    private var terminalLog: some View {
        TerminalOutputTextView(content: terminalContent, search: search, caseSensitive: caseSensitive, follow: follow && !paused, searchNavigation: searchNavigation)
            .background(Color(nsColor: .textBackgroundColor))
    }

    private var terminalContent: NSAttributedString {
        let result = NSMutableAttributedString()
        for (index, record) in visibleRecords.enumerated() {
            result.append(NSAttributedString(rendered[record.id] ?? AttributedString(record.text)))
            if index + 1 < visibleRecords.count { result.append(NSAttributedString(string: "\n")) }
        }
        result.addAttribute(.font, value: NSFont.monospacedSystemFont(ofSize: NSFont.systemFontSize, weight: .regular), range: NSRange(location: 0, length: result.length))
        return result
    }

    private func matchesSearch(_ text: String) -> Bool {
        if caseSensitive { return text.contains(search) }
        return text.localizedCaseInsensitiveContains(search)
    }

    private func start(workloadID: String?) {
        streamTask?.cancel()
        records.removeAll()
        rendered.removeAll()
        ansiParsers.removeAll()
        paused = false
        evictedRecords = 0
        guard logRuntimeAvailable else { connectionStatus = "Runtime unavailable"; return }
        guard let workloadID else { connectionStatus = "Choose a container"; return }
        connectionStatus = "Loading history…"
        streamTask = Task {
            await followLogStream(
                subscribe: { cursor in
                    try model.logStream(environmentID: environment.id, containerID: workloadID, since: range.cutoff, after: cursor)
                },
                onStatus: { connectionStatus = $0 },
                onRecords: { batch in
                    var nextParsers = ansiParsers
                    var nextRendered = rendered
                    for record in batch {
                        var parser = nextParsers[record.stream] ?? ANSIParser()
                        nextRendered[record.id] = parser.consume(record.text)
                        nextParsers[record.stream] = parser
                    }
                    ansiParsers = nextParsers
                    rendered = nextRendered
                    records.append(contentsOf: batch)
                    if records.count > 5_000 {
                        let removed = records.count - 5_000
                        evictedRecords += removed
                        for item in records.prefix(removed) { rendered.removeValue(forKey: item.id) }
                        records.removeFirst(removed)
                        pausedCount = max(0, pausedCount - removed)
                    }
                }
            )
        }
    }

    private func export() {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = "\(environment.name)-\(presentation == .logs ? "logs" : "terminal").txt"
        guard panel.runModal() == .OK, let url = panel.url else { return }
        let text = visibleRecords.map { record in
            let plain = ANSIParser.plain(record.text)
            return presentation == .logs ? "\(record.timestamp.formatted(.iso8601)) [\(record.stream)] \(plain)" : plain
        }.joined(separator: "\n")
        do { try text.write(to: url, atomically: true, encoding: .utf8) }
        catch { model.errorMessage = error.localizedDescription }
    }
}

@MainActor
func followLogStream(
    subscribe: (String?) throws -> AsyncThrowingStream<LogEntry, Error>,
    onStatus: (String) -> Void,
    onRecords: @escaping ([LogEntry]) -> Void
) async {
    var cursor: String?
    var retry = 0
    let accumulator = LogBatchAccumulator(onBatch: onRecords)
    defer { accumulator.finish() }
    while !Task.isCancelled {
        do {
            let stream = try subscribe(cursor)
            onStatus(retry == 0 ? "Live" : "Reconnected")
            retry = 0
            for try await record in stream {
                cursor = record.id
                accumulator.append(record)
            }
            accumulator.flush()
            onStatus("Ended")
            return
        } catch is CancellationError {
            return
        } catch {
            retry += 1
            onStatus(retry < 5 ? "Reconnecting…" : "Failed · \(hostErrorMessage(error))")
            try? await Task.sleep(for: .seconds(min(8, 1 << min(retry, 3))))
        }
    }
}

@MainActor
private final class LogBatchAccumulator {
    private var records: [LogEntry] = []
    private var scheduledFlush: Task<Void, Never>?
    private let onBatch: ([LogEntry]) -> Void

    init(onBatch: @escaping ([LogEntry]) -> Void) { self.onBatch = onBatch }

    func append(_ record: LogEntry) {
        records.append(record)
        if records.count >= 100 {
            flush()
        } else if scheduledFlush == nil {
            scheduledFlush = Task { [weak self] in
                try? await Task.sleep(for: .milliseconds(50))
                guard !Task.isCancelled else { return }
                self?.deliver()
            }
        }
    }

    func flush() {
        scheduledFlush?.cancel()
        deliver()
    }

    func finish() { flush() }

    private func deliver() {
        scheduledFlush = nil
        guard !records.isEmpty else { return }
        let batch = records
        records.removeAll(keepingCapacity: true)
        onBatch(batch)
    }
}

private enum LogPresentation: String, CaseIterable, Identifiable { case logs = "Logs", terminal = "Terminal Output"; var id: Self { self } }
private enum LogRange: String, CaseIterable, Identifiable {
    case live = "Live", fifteenMinutes = "15 min", oneHour = "1 hour"
    var id: Self { self }
    var cutoff: Date? { switch self { case .live: nil; case .fifteenMinutes: .now.addingTimeInterval(-900); case .oneHour: .now.addingTimeInterval(-3_600) } }
}
private enum LogStreamFilter: String, CaseIterable, Identifiable {
    case all = "All", stdout, stderr
    var id: Self { self }
    func matches(_ value: String) -> Bool { self == .all || rawValue == value }
}
