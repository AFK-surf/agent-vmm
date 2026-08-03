import AppKit
import SwiftUI

struct LogsActivityView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel

    @State private var selectedWorkloadID: String?
    @State private var records: [LogEntry] = []
    @State private var search = ""
    @State private var paused = false
    @State private var streamTask: Task<Void, Never>?

    private var filtered: [LogEntry] {
        search.isEmpty ? records : records.filter { $0.text.localizedCaseInsensitiveContains(search) }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Picker("Workspace or service", selection: $selectedWorkloadID) {
                    Text("Choose…").tag(String?.none)
                    ForEach(environment.workloads) { Text($0.id).tag(Optional($0.id)) }
                }
                TextField("Search logs", text: $search).textFieldStyle(.roundedBorder)
                Toggle("Pause", isOn: $paused).toggleStyle(.button)
                Button("Export…", action: export)
            }
            if records.contains(where: \.truncated) {
                Label("Earlier records were truncated.", systemImage: "exclamationmark.triangle").foregroundStyle(.orange)
            }
            List(filtered) { record in
                HStack(alignment: .top) {
                    Text(record.timestamp, style: .time).foregroundStyle(.secondary)
                    Text(record.stream).foregroundStyle(record.stream == "stderr" ? .red : .secondary)
                    Text(record.text).textSelection(.enabled).font(.system(.body, design: .monospaced))
                }
                .accessibilityElement(children: .combine)
            }
            .overlay { if selectedWorkloadID == nil { ContentUnavailableView("Choose a workspace or service", systemImage: "text.alignleft") } }
        }
        .onChange(of: selectedWorkloadID) { _, value in start(workloadID: value) }
        .onDisappear { streamTask?.cancel() }
    }

    private func start(workloadID: String?) {
        streamTask?.cancel()
        records.removeAll()
        guard let workloadID else { return }
        streamTask = Task {
            var cursor: String?
            while !Task.isCancelled {
                do {
                    let stream = try model.logStream(environmentID: environment.id, containerID: workloadID, after: cursor)
                    for try await record in stream {
                        cursor = record.id
                        while paused && !Task.isCancelled { try await Task.sleep(for: .milliseconds(100)) }
                        records.append(record)
                        if records.count > 5_000 { records.removeFirst(records.count - 5_000) }
                    }
                } catch is CancellationError { return }
                catch { try? await Task.sleep(for: .milliseconds(500)) }
            }
        }
    }

    private func export() {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = "\(environment.name)-logs.txt"
        guard panel.runModal() == .OK, let url = panel.url else { return }
        let text = filtered.map { "\($0.timestamp.formatted(.iso8601)) [\($0.stream)] \($0.text)" }.joined(separator: "\n")
        try? text.write(to: url, atomically: true, encoding: .utf8)
    }
}
