import AppKit
import SwiftUI

struct AddAgentSheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    @State private var step = 0
    @State private var client = "codex"
    @State private var displayName = ""
    @State private var environmentID = ""
    @State private var prepared: AgentIntegrationModel?
    @State private var isWorking = false
    @State private var localError: String?

    private var localEnvironments: [AgentEnvironmentModel] { model.snapshot.environments.filter { $0.origin == .local } }

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Add Local Agent").font(.title2.weight(.semibold))
            Text("Step \(step + 1) of 3 · \(["Choose agent", "Scope and environment", "Review and add"][step])")
                .font(.caption).foregroundStyle(.secondary)
            Divider()
            Group {
                switch step {
                case 0: chooseAgent
                case 1: chooseEnvironment
                default: review
                }
            }
            if let localError {
                Label(localError, systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.red)
            }
            controls
        }
        .padding(24)
        .frame(minWidth: 560, idealWidth: 620, minHeight: 360)
        .onAppear { if !model.canConfigureCodex { client = "manual" } }
        .interactiveDismissDisabled(isWorking || prepared != nil)
        .overlay { if isWorking { ProgressView(progressDescription).padding().background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10)) } }
    }

    private var chooseAgent: some View {
        Form {
            Picker("Agent", selection: $client) {
                if model.canConfigureCodex { Text("Codex").tag("codex") }
                Text("Other MCP client — manual setup").tag("manual")
            }
            Text(client == "codex" ? "Agent VMM can update the detected Codex MCP configuration." : "Agent VMM will provide an exact stdio command for your MCP client.")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var chooseEnvironment: some View {
        Form {
            LabeledContent("Scope", value: client == "codex" ? "User" : "Manual client configuration")
            TextField("Environment name", text: $displayName, prompt: Text("Development"))
            Picker("Environment", selection: $environmentID) {
                Text("Create a new environment").tag("")
                ForEach(localEnvironments) { Text("Reuse \($0.name)").tag($0.id) }
            }
            if !environmentID.isEmpty {
                Label("This agent will share the selected environment's data and policy.", systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
            }
        }
    }

    private var review: some View {
        Form {
            LabeledContent("Agent", value: client == "codex" ? "Codex" : "Other MCP client")
            LabeledContent("Configuration", value: prepared?.configTarget ?? "Preparing…")
            LabeledContent("Environment", value: prepared?.environmentID ?? "Preparing…")
            if let prepared, let command = model.manualCommand(for: prepared) {
                LabeledContent("Command") {
                    Text(command).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                }
                if client == "manual" {
                    Button("Copy Command") {
                        NSPasteboard.general.clearContents()
                        NSPasteboard.general.setString(command, forType: .string)
                    }
                }
            }
            Text(reviewGuidance)
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var controls: some View {
        HStack {
            if step > 0 && prepared == nil { Button("Back") { step -= 1 } }
            Spacer()
            if localError != nil {
                Button("Close") { dismiss() }.disabled(isWorking)
            }
            Button(prepared == nil ? "Cancel" : "Cancel Adding", role: .cancel) { cancel() }.disabled(isWorking)
            if step < 2 {
                Button("Continue") { advance() }
                    .buttonStyle(.borderedProminent)
                    .disabled(step == 1 && displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || isWorking)
            } else {
                Button(localError == nil ? (client == "codex" ? "Add Agent VMM" : "I Added the Configuration") : "Retry") { complete() }
                    .buttonStyle(.borderedProminent)
                    .disabled(prepared == nil || isWorking)
            }
        }
    }

    private func advance() {
        localError = nil
        guard step == 1 else { step += 1; return }
        isWorking = true
        let name = displayName.trimmingCharacters(in: .whitespacesAndNewlines)
        let existing = environmentID.isEmpty ? nil : environmentID
        Task {
            prepared = client == "codex"
                ? await model.prepareCodexIntegration(displayName: name, environmentID: existing)
                : await model.prepareManualIntegration(displayName: name, environmentID: existing)
            isWorking = false
            if prepared != nil { step = 2 } else { localError = model.errorMessage }
        }
    }

    private func complete() {
        guard let prepared else { return }
        isWorking = true
        localError = nil
        Task {
            let succeeded = client == "codex"
                ? await model.installCodexIntegration(prepared)
                : await model.markManualIntegrationReady(prepared)
            isWorking = false
            if succeeded { dismiss() } else { localError = model.errorMessage }
        }
    }

    private func cancel() {
        guard let prepared else { dismiss(); return }
        isWorking = true
        localError = nil
        Task {
            let cancelled = await model.cancelPreparedIntegration(prepared)
            isWorking = false
            if cancelled { dismiss() } else { localError = model.errorMessage }
        }
    }

    private var reviewGuidance: String {
        if client != "codex" {
            return "Copying is not verification. The status remains Awaiting connection until an authenticated session connects."
        }
        if model.snapshot.vmState != "Running" {
            return "Agent access can be configured now. Agent VMM will verify it after the virtual machine starts and Codex reconnects."
        }
        return "Codex sessions must reconnect before Agent VMM can verify the configuration."
    }

    private var progressDescription: String {
        if step < 2 { return "Preparing agent access…" }
        return model.integrationStage?.rawValue ?? "Adding agent access…"
    }
}
