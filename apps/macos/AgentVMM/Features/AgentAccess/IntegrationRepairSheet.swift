import SwiftUI

struct IntegrationRepairSheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    let integration: AgentIntegrationModel
    @State private var currentConfiguration = "Loading configuration…"
    @State private var isRepairing = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Review Configuration Change").font(.title2.weight(.semibold))
            Text("The managed MCP entry changed outside Agent VMM. Review the current entry before replacing it.")
                .foregroundStyle(.secondary)
            GroupBox("Current client entry") {
                ScrollView {
                    Text(currentConfiguration)
                        .font(.system(.caption, design: .monospaced))
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(height: 150)
            }
            GroupBox("Agent VMM command") {
                Text(model.manualCommand(for: integration) ?? "The Agent VMM helper is unavailable.")
                    .font(.system(.caption, design: .monospaced))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            Label("Replacing this entry does not delete the environment or retained data.", systemImage: "info.circle")
                .font(.caption).foregroundStyle(.secondary)
            HStack {
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                Button("Replace Configuration") {
                    isRepairing = true
                    Task {
                        let succeeded = await model.repairIntegration(integration)
                        isRepairing = false
                        if succeeded { dismiss() }
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(isRepairing)
            }
        }
        .padding(24)
        .frame(width: 620)
        .task { currentConfiguration = await model.currentConfiguration(for: integration) }
        .interactiveDismissDisabled(isRepairing)
        .overlay { if isRepairing { ProgressView("Repairing configuration…") } }
    }
}
