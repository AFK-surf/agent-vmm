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
    @State private var follow = true
    @State private var connectionStatus = "Choose a container"
    @State private var streamTask: Task<Void, Never>?

    private var visibleRecords: [LogEntry] {
        let source = paused ? Array(records.prefix(pausedCount)) : records
        let cutoff = range.cutoff
        var result = source.filter { record in
            (cutoff == nil || record.timestamp >= cutoff!) &&
            (presentation == .terminal || streamFilter.matches(record.stream)) &&
            (search.isEmpty || matchesSearch(record.text))
        }
        if presentation == .logs && newestFirst { result.reverse() }
        return result
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            primaryToolbar
            secondaryToolbar
            statusBar
            content
        }
        .onAppear {
            if selectedWorkloadID == nil { selectedWorkloadID = environment.workloads.first?.id }
        }
        .onChange(of: selectedWorkloadID) { _, value in start(workloadID: value) }
        .onChange(of: range) { _, _ in start(workloadID: selectedWorkloadID) }
        .onChange(of: newestFirst) { _, value in if value { follow = false } }
        .onChange(of: paused) { _, value in if value { pausedCount = records.count } }
        .onDisappear { streamTask?.cancel() }
    }

    private var primaryToolbar: some View {
        ViewThatFits(in: .horizontal) {
            HStack { selectionControls; searchControls }
            VStack(alignment: .leading, spacing: 8) {
                HStack { selectionControls }
                HStack { searchControls }
            }
        }
    }

    @ViewBuilder private var selectionControls: some View {
            Picker("Container", selection: $selectedWorkloadID) {
                Text("Choose…").tag(String?.none)
                ForEach(environment.workloads) { Text($0.id).tag(Optional($0.id)) }
            }
            .frame(minWidth: 180)
            Picker("Presentation", selection: $presentation) {
                ForEach(LogPresentation.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)
            .frame(width: 190)
            Picker("Time range", selection: $range) {
                ForEach(LogRange.allCases) { Text($0.rawValue).tag($0) }
            }
            .frame(width: 120)
    }

    @ViewBuilder private var searchControls: some View {
            TextField(presentation == .logs ? "Filter logs" : "Find", text: $search)
                .textFieldStyle(.roundedBorder)
                .frame(minWidth: 180)
            Toggle("Match case", isOn: $caseSensitive).toggleStyle(.button).help("Match uppercase and lowercase exactly")
    }

    private var secondaryToolbar: some View {
        ViewThatFits(in: .horizontal) {
            HStack { presentationControls; Spacer(); playbackControls }
            VStack(alignment: .leading, spacing: 8) {
                HStack { presentationControls }
                HStack { Spacer(); playbackControls }
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
            Label("Read-only terminal view", systemImage: "lock").foregroundStyle(.secondary)
        }
    }

    @ViewBuilder private var playbackControls: some View {
        Toggle("Follow", isOn: $follow).toggleStyle(.button).disabled(newestFirst || paused)
        Toggle(paused ? "Resume" : "Pause", isOn: $paused).toggleStyle(.button)
        Button("Export Visible…", action: export)
    }

    private var statusBar: some View {
        HStack(spacing: 14) {
            Text(paused ? "Paused · \(max(0, records.count - pausedCount)) new" : connectionStatus)
            if records.contains(where: \.truncated) {
                Label("A record was truncated", systemImage: "exclamationmark.triangle")
            }
            let dropped = records.map(\.droppedRecords).max() ?? 0
            if dropped > 0 { Label("\(dropped) records dropped", systemImage: "exclamationmark.triangle.fill") }
            Spacer()
            Text("\(visibleRecords.count) shown · \(records.count) buffered")
        }
        .font(.caption)
        .foregroundStyle(.secondary)
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
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 1) {
                    ForEach(visibleRecords) { record in
                        Text(rendered[record.id] ?? AttributedString(record.text))
                            .font(.system(.body, design: .monospaced))
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .id(record.id)
                    }
                }
                .padding(12)
            }
            .background(Color(nsColor: .textBackgroundColor))
            .onChange(of: records.count) { _, _ in
                if follow, !paused, let last = visibleRecords.last { proxy.scrollTo(last.id, anchor: .bottom) }
            }
        }
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
        guard let workloadID else { connectionStatus = "Choose a container"; return }
        connectionStatus = "Loading history…"
        streamTask = Task {
            await followLogStream(
                subscribe: { cursor in
                    try model.logStream(environmentID: environment.id, containerID: workloadID, since: range.cutoff, after: cursor)
                },
                onStatus: { connectionStatus = $0 },
                onRecord: { record in
                    var parser = ansiParsers[record.stream] ?? ANSIParser()
                    rendered[record.id] = parser.consume(record.text)
                    ansiParsers[record.stream] = parser
                    records.append(record)
                    if records.count > 5_000 {
                        let removed = records.count - 5_000
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
    onRecord: (LogEntry) -> Void
) async {
    var cursor: String?
    var retry = 0
    while !Task.isCancelled {
        do {
            let stream = try subscribe(cursor)
            onStatus(retry == 0 ? "Live" : "Reconnected")
            retry = 0
            for try await record in stream {
                cursor = record.id
                onRecord(record)
            }
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

private enum LogPresentation: String, CaseIterable, Identifiable { case logs = "Logs", terminal = "Terminal"; var id: Self { self } }
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
