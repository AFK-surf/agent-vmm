import SwiftUI

struct DiagnosticsView: View {
    @Bindable var model: AppModel
    @State private var supportItems: [SupportItem] = []
    @State private var selectedSensitive: Set<String> = []
    @State private var showingPreview = false
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                HStack {
                    Text("Diagnostics & Activity").font(.title2.weight(.semibold))
                    Spacer()
                    Button("Run Diagnostics") { Task { await model.runDiagnostics() } }
                    Button("Export Support Bundle…") { previewSupportBundle() }
                }

                if let operation = model.latestOperation {
                    GroupBox("Latest operation") {
                        VStack(alignment: .leading, spacing: 7) {
                            Label("\(operation.kind): \(operation.state) — \(operation.stage)", systemImage: operation.state == "Succeeded" ? "checkmark.circle.fill" : "clock.fill")
                            if let resource = operation.resource { Text("Resource: \(resource)").foregroundStyle(.secondary) }
                            if let reason = operation.reason { Text("Reason: \(reason)").foregroundStyle(.secondary) }
                            Text(operation.nextStep).foregroundStyle(.secondary)
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(6)
                    }
                }

                GroupBox("Recent activity") {
                    if model.events.isEmpty {
                        VStack(spacing: 8) {
                            Label("No activity yet", systemImage: "clock")
                                .font(.headline)
                            Text("Virtual machine and environment operations will appear here.")
                                .foregroundStyle(.secondary)
                        }
                        .frame(maxWidth: .infinity, minHeight: 140)
                    } else {
                        Table(model.events.reversed()) {
                            TableColumn("Time") { Text($0.timestamp, style: .time) }
                            TableColumn("Activity", value: \.type)
                            TableColumn("Environment") { Text($0.environmentID ?? "System") }
                            TableColumn("Resource") { Text($0.resourceID ?? "—") }
                        }
                        .frame(minHeight: 260)
                    }
                }

                DisclosureGroup("System Details") {
                    Form {
                        LabeledContent("Operator socket", value: AppModel.defaultSocketPath)
                        LabeledContent("Remote compute socket", value: AppModel.remoteComputeSocketPath)
                        LabeledContent("VM boot ID", value: model.snapshot.bootID.isEmpty ? "Unavailable" : model.snapshot.bootID)
                        LabeledContent("Local integrations", value: String(model.integrations.count))
                        LabeledContent("Active agent sessions", value: String(model.sessions.filter { $0.state == .connected }.count))
                        if let remote = model.snapshot.remoteRegistration {
                            LabeledContent("Connector release", value: remote.connector.release.isEmpty ? "Unavailable" : remote.connector.release)
                            LabeledContent("Connector phase", value: remote.connector.phase.rawValue)
                            if let code = remote.connector.failureCode { LabeledContent("Connector failure", value: code) }
                        }
                    }
                    .formStyle(.grouped)
                }
            }
            .frame(maxWidth: 1040, alignment: .leading)
            .padding(28)
        }
        .navigationTitle("Activity")
        .sheet(isPresented: $showingPreview) {
            VStack(alignment: .leading, spacing: 16) {
                Text("Support bundle contents").font(.title2.bold())
                ForEach(supportItems) { item in
                    if item.sensitive {
                        Toggle(isOn: Binding(get: { selectedSensitive.contains(item.name) }, set: { selected in
                            if selected { selectedSensitive.insert(item.name) } else { selectedSensitive.remove(item.name) }
                        })) { VStack(alignment: .leading) { Text(item.name); Text(item.description).foregroundStyle(.secondary) } }
                    } else {
                        VStack(alignment: .leading) { Text(item.name); Text(item.description).foregroundStyle(.secondary) }
                    }
                }
                HStack {
                    Spacer()
                    Button("Cancel", role: .cancel) { showingPreview = false }
                    Button("Export") { showingPreview = false; Task { await model.exportSupportBundle(sensitiveItems: Array(selectedSensitive)) } }
                }
            }.padding().frame(width: 520)
        }
    }

    private func previewSupportBundle() {
        Task {
            do { supportItems = try await model.supportPreview(); selectedSensitive = []; showingPreview = true }
            catch { model.errorMessage = hostErrorMessage(error) }
        }
    }
}
