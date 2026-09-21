import AppKit
import SwiftUI

struct BundledMCPView: View {
    @Bindable var model: AppModel
    @State private var grant = ApplianceGrantModel.bundledMCPDefault
    @State private var showingDisable = false
    @State private var showingRemove = false
    @State private var showingSetupPolicy = false
    @State private var showingOtherClient = false
    @State private var removePlan: ApplianceRemovePlanModel?
    @State private var removeError: String?
    @State private var removalTask: Task<Void, Never>?
    @State private var removalPlanning = false
    @State private var manualBindingToRemove: MCPClientBindingModel?
    @State private var manualName = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Label("MCP", systemImage: "curlybraces.square").font(.title2.weight(.semibold))
                    Text("Connect Codex, Pi, or another MCP client to an isolated runtime.").foregroundStyle(.secondary)
                }
                Spacer()
                status(model.bundledMCP)
            }
            if let appliance = model.bundledMCP {
                clients(appliance)
                accessPolicy(appliance)
                healthAndAdvanced(appliance)
            } else {
                setup
                if !model.mcpClientBindings.isEmpty { staleClients }
            }
        }
        .confirmationDialog("Disable MCP appliance?", isPresented: $showingDisable) {
            Button("Disable and Retain Agent Data") { Task { await model.setBundledMCPEnabled(false) } }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("The MCP frontend closes and active sessions are revoked. Agent allocations and retained volumes stay on this Mac.")
        }
        .sheet(isPresented: $showingRemove, onDismiss: cancelRemovalPlanning) { removeSheet }
        .sheet(isPresented: $showingSetupPolicy) { setupPolicySheet }
        .confirmationDialog(
            "Remove \(manualBindingToRemove?.displayName ?? "client")?",
            isPresented: Binding(
                get: { manualBindingToRemove != nil },
                set: { if !$0 { manualBindingToRemove = nil } }
            ),
            presenting: manualBindingToRemove
        ) { binding in
            Button("Remove client", role: .destructive) {
                model.removeManualMCPConfiguration(binding)
                manualBindingToRemove = nil
            }
            Button("Cancel", role: .cancel) { manualBindingToRemove = nil }
        } message: { binding in
            Text("This removes its connection details from Agent VMM. A bearer token already copied to the client remains valid until the MCP appliance is removed.")
        }
    }

    private var setup: some View {
        GroupBox {
            Form {
                Text("Set up the local MCP service once, then configure each client below.")
                HStack {
                    Button("Set Up MCP") { Task { _ = await model.installBundledMCP(grant: grant) } }
                        .buttonStyle(.borderedProminent)
                        .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                    Button("Customize…") { showingSetupPolicy = true }
                        .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                }
                Text("The service starts disabled. You can enable it from Health and advanced after setup.")
                    .font(.caption).foregroundStyle(.secondary)
            }.padding(8)
        }
    }

    private var staleClients: some View {
        GroupBox("Saved client configurations") {
            VStack(alignment: .leading, spacing: 10) {
                Label("MCP is not installed, but client connection details are still saved on this Mac.", systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
                Text("Set up MCP again to use them, or remove them here.")
                    .font(.caption).foregroundStyle(.secondary)
                ForEach(model.mcpClientBindings) { binding in
                    MCPClientRow(model: model, binding: binding, applianceReady: false, onRemoveManual: { manualBindingToRemove = $0 })
                }
            }
            .padding(8)
        }
    }

    private var setupPolicySheet: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("MCP access policy").font(.title2.weight(.semibold))
            Form {
                Toggle("Allow client workloads to access the public internet", isOn: $grant.allowsPublicEgress)
                LabeledContent("Per-client isolation", value: "512 PIDs · 4 GiB disk")
            }
            HStack {
                Spacer()
                Button("Cancel") { showingSetupPolicy = false }
                Button("Set Up MCP") {
                    showingSetupPolicy = false
                    Task { _ = await model.installBundledMCP(grant: grant) }
                }.buttonStyle(.borderedProminent).disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
            }
        }.padding(24).frame(width: 520)
    }

    private func clients(_ appliance: ApplianceStatusModel) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    VStack(alignment: .leading, spacing: 3) {
                        Text("Clients").font(.headline)
                        Text("Configure the clients that are allowed to use this MCP service.").font(.caption).foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("View environments") { model.section = .environments }
                }
                if model.mcpClientBindings.isEmpty {
                    ContentUnavailableView {
                        Label("No MCP clients yet", systemImage: "curlybraces")
                    } description: {
                        Text("Set up Codex or add another client to create its connection.")
                    }
                    .frame(maxWidth: .infinity, minHeight: 120)
                } else {
                    ForEach(model.mcpClientBindings) { binding in
                        MCPClientRow(
                            model: model,
                            binding: binding,
                            applianceReady: appliance.endpointAvailable,
                            onRemoveManual: { manualBindingToRemove = $0 }
                        )
                        if binding.id != model.mcpClientBindings.last?.id { Divider() }
                    }
                }
                HStack {
                    if !model.mcpClientBindings.contains(where: { $0.kind == .codex }) {
                        Button("Set up Codex") { Task { await model.installCodexMCPConfiguration() } }
                            .buttonStyle(.borderedProminent)
                            .disabled(!appliance.endpointAvailable || !model.canConfigureCodex || model.applianceActionInProgress || model.mcpClientActionInProgress)
                    }
                    Button("Add other client…") { showingOtherClient = true }.disabled(!appliance.endpointAvailable || model.applianceActionInProgress || model.mcpClientActionInProgress)
                    if !model.canConfigureCodex { Text("Codex CLI not found").font(.caption).foregroundStyle(.secondary) }
                }
                if showingOtherClient {
                    Divider()
                    HStack {
                        TextField("Client name", text: $manualName, prompt: Text("Pi or another MCP client"))
                        Button("Create client configuration") {
                            let name = manualName.trimmingCharacters(in: .whitespacesAndNewlines)
                            if !name.isEmpty, model.createManualMCPConfiguration(displayName: name) != nil {
                                manualName = ""
                                showingOtherClient = false
                            }
                        }.disabled(manualName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || model.applianceActionInProgress || model.mcpClientActionInProgress)
                    }
                }
            }.padding(8)
        }
    }

    private func accessPolicy(_ appliance: ApplianceStatusModel) -> some View {
        GroupBox("Access policy") {
            VStack(alignment: .leading, spacing: 8) {
                LabeledContent("Isolation", value: "\(appliance.grant.perEnvironmentPIDs) PIDs · \(appliance.grant.perEnvironmentDiskBytes >> 30) GiB disk per client")
                LabeledContent("Internet", value: appliance.grant.allowsPublicEgress ? "Allowed" : "Blocked")
                Text("These limits apply to every client environment.").font(.caption).foregroundStyle(.secondary)
            }.padding(8)
        }
    }

    private func healthAndAdvanced(_ appliance: ApplianceStatusModel) -> some View {
        DisclosureGroup("Health and advanced") {
            VStack(alignment: .leading, spacing: 10) {
                LabeledContent("State", value: appliance.state)
                LabeledContent("Health", value: appliance.healthy ? "Healthy" : (appliance.healthMessage ?? "Needs attention"))
                LabeledContent("MCP endpoint", value: appliance.endpointStatus)
                HStack {
                    if let action = model.applianceAction {
                        ProgressView(action.title)
                            .controlSize(.small)
                            .accessibilityLabel(action.title)
                    } else {
                        if appliance.enabled {
                            Button("Disable…") { showingDisable = true }
                            Button("Restart") { Task { await model.restartBundledMCP() } }
                        } else {
                            Button("Enable") { Task { await model.setBundledMCPEnabled(true) } }.buttonStyle(.borderedProminent)
                        }
                    }
                    Spacer()
                    Button("Remove MCP…", role: .destructive) { loadRemovePlan() }
                        .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                }
            }.padding(.top, 8)
        }
    }

    private func status(_ appliance: ApplianceStatusModel?) -> some View {
        let label: String
        let symbol: String
        let kind: AccessStatusView.Kind
        if let appliance {
            if let action = model.applianceAction {
                label = action.title
                symbol = "hourglass"
                kind = .attention
            } else {
                label = appliance.endpointAvailable && appliance.healthy ? "Ready" : (appliance.enabled ? "Needs attention" : "Disabled")
                symbol = appliance.endpointAvailable && appliance.healthy ? "checkmark.circle.fill" : "pause.circle"
                kind = appliance.endpointAvailable && appliance.healthy ? .success : (appliance.enabled ? .attention : .neutral)
            }
        } else {
            label = "Not set up"
            symbol = "plus.circle"
            kind = .neutral
        }
        return AccessStatusView(
            label: label,
            symbol: symbol,
            kind: kind
        )
    }

    private func loadRemovePlan() {
        guard !model.applianceActionInProgress else { return }
        removalTask?.cancel()
        removePlan = nil
        removeError = nil
        removalPlanning = true
        showingRemove = true
        removalTask = Task {
            defer {
                removalPlanning = false
                removalTask = nil
            }
            do {
                removePlan = try await model.planBundledMCPRemoval()
            } catch is CancellationError {
                return
            } catch {
                guard !Task.isCancelled else { return }
                removeError = hostErrorMessage(error)
            }
        }
    }

    private func cancelRemovalPlanning() {
        removalTask?.cancel()
        removalTask = nil
        removalPlanning = false
    }

    @ViewBuilder private var removeSheet: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Remove MCP Appliance").font(.title2.weight(.semibold))
            if removalPlanning || model.applianceAction != nil {
                ProgressView(model.applianceAction?.title ?? "Preparing removal…")
                    .frame(maxWidth: .infinity, minHeight: 120)
            } else if let plan = removePlan {
                Text("Client setup files and the local MCP service will be removed. Existing environments are listed below.")
                    .foregroundStyle(.secondary)
                ForEach(plan.impacts) { impact in
                    Label("\(impact.resourceType): \(impact.effect)", systemImage: impact.blocker ? "exclamationmark.octagon.fill" : (impact.irreversible ? "trash.fill" : "info.circle"))
                        .foregroundStyle(impact.blocker ? .red : .primary)
                }
                if !plan.blockers.isEmpty {
                    Text("Removal is blocked while retained agent allocations exist. Disable the appliance, clean up those environments, then request a new removal plan. Agent VMM will not bypass this safeguard.")
                        .foregroundStyle(.red)
                } else {
                    Text("This permanently deletes the encrypted appliance volume and its controller state.").foregroundStyle(.red)
                }
                HStack {
                    Spacer(); Button("Cancel") { showingRemove = false }
                    Button("Permanently Remove", role: .destructive) {
                        Task {
                            if await model.removeBundledMCP(plan: plan) {
                                removePlan = nil
                                showingRemove = false
                            } else {
                                removeError = model.errorMessage
                            }
                        }
                    }
                        .disabled(!plan.blockers.isEmpty || model.applianceActionInProgress)
                }
            } else if let removeError {
                Label(removeError, systemImage: "exclamationmark.triangle.fill").foregroundStyle(.red)
                HStack {
                    Spacer()
                    Button("Cancel") { showingRemove = false }
                    Button("Retry") { loadRemovePlan() }.buttonStyle(.borderedProminent)
                }
            } else {
                ProgressView("Preparing removal…")
                    .frame(maxWidth: .infinity, minHeight: 120)
            }
        }
        .padding(24)
        .frame(width: 560)
        .interactiveDismissDisabled(model.applianceAction == .removing)
    }
}

private struct MCPClientRow: View {
    @Bindable var model: AppModel
    let binding: MCPClientBindingModel
    let applianceReady: Bool
    let onRemoveManual: (MCPClientBindingModel) -> Void

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: binding.kind == .codex ? "terminal" : "curlybraces")
                .font(.title3).foregroundStyle(.blue).frame(width: 28)
            VStack(alignment: .leading, spacing: 4) {
                Text(binding.displayName).font(.headline)
                Text(binding.kind == .codex ? "Managed Codex MCP client" : "MCP client · manual configuration")
                    .font(.caption).foregroundStyle(.secondary)
                AccessStatusView(
                    label: applianceReady ? (binding.kind == .codex ? "Managed setup" : "Connection details ready") : "MCP endpoint unavailable",
                    symbol: applianceReady ? "checkmark.circle" : "pause.circle",
                    kind: applianceReady ? .success : .neutral
                )
            }
            Spacer()
            if binding.kind == .codex {
                HStack(spacing: 8) {
                    if model.mcpClientActionInProgress {
                        ProgressView("Updating…")
                            .controlSize(.small)
                    } else {
                        Button("Configure Codex") { Task { await model.installCodexMCPConfiguration() } }
                            .disabled(!applianceReady || !model.canConfigureCodex || model.applianceActionInProgress)
                    }
                    Menu {
                        Button("Remove Codex setup", role: .destructive) {
                            Task { await model.removeCodexMCPConfiguration() }
                        }
                    } label: {
                        Image(systemName: "ellipsis.circle")
                    }
                    .menuStyle(.borderlessButton)
                    .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                }
            } else if let connection = model.manualMCPConfiguration(for: binding) {
                VStack(alignment: .trailing, spacing: 8) {
                    HStack(spacing: 8) {
                        Button("Copy connection details") {
                            NSPasteboard.general.clearContents()
                            NSPasteboard.general.setString(connectionDetails(connection), forType: .string)
                        }.disabled(!applianceReady || model.applianceActionInProgress || model.mcpClientActionInProgress)
                        Menu {
                            Button("Remove client…", role: .destructive) { onRemoveManual(binding) }
                        } label: {
                            Image(systemName: "ellipsis.circle")
                        }
                        .menuStyle(.borderlessButton)
                        .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                    }
                    DisclosureGroup("Show connection details") {
                        VStack(alignment: .leading, spacing: 6) {
                            Text("Transport: Streamable HTTP")
                            Text("Endpoint\n\(connection.endpointURL.absoluteString)")
                            Text("Authorization header\nBearer \(connection.bearerToken)")
                        }
                        .font(.system(.caption, design: .monospaced))
                        .textSelection(.enabled)
                            .frame(maxWidth: 360, alignment: .trailing)
                    }
                    .fixedSize(horizontal: false, vertical: true)
                }
            } else {
                HStack(spacing: 8) {
                    Label("Endpoint unavailable", systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.orange)
                    Menu {
                        Button("Remove client…", role: .destructive) { onRemoveManual(binding) }
                    } label: {
                        Image(systemName: "ellipsis.circle")
                    }
                    .menuStyle(.borderlessButton)
                    .disabled(model.applianceActionInProgress || model.mcpClientActionInProgress)
                }
            }
        }
        .padding(.vertical, 4)
    }

    private func connectionDetails(_ connection: MCPManualConfiguration) -> String {
        """
        Streamable HTTP MCP
        Endpoint: \(connection.endpointURL.absoluteString)
        Authorization: Bearer \(connection.bearerToken)
        """
    }
}
