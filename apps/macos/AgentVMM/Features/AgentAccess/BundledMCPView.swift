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
            }
            if let error = model.errorMessage { Label(error, systemImage: "exclamationmark.triangle.fill").foregroundStyle(.red) }
        }
        .confirmationDialog("Disable MCP appliance?", isPresented: $showingDisable) {
            Button("Disable and Retain Agent Data") { Task { await model.setBundledMCPEnabled(false) } }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("The MCP frontend closes and active leases are revoked. Agent allocations and retained volumes stay on this Mac.")
        }
        .sheet(isPresented: $showingRemove) { removeSheet }
        .sheet(isPresented: $showingSetupPolicy) { setupPolicySheet }
    }

    private var setup: some View {
        GroupBox {
            Form {
                Text("Set up the local MCP service once, then configure each client below.")
                HStack {
                    Button("Set Up MCP") { Task { _ = await model.installBundledMCP(grant: grant) } }
                        .buttonStyle(.borderedProminent)
                        .disabled(model.applianceActionInProgress)
                    Button("Customize…") { showingSetupPolicy = true }
                }
                Text("The service starts disabled. You can enable it from Health and advanced after setup.")
                    .font(.caption).foregroundStyle(.secondary)
            }.padding(8)
        }
    }

    private var setupPolicySheet: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("MCP access policy").font(.title2.weight(.semibold))
            Form {
                Stepper("Maximum clients: \(grant.maxEnvironments)", value: $grant.maxEnvironments, in: 1...16)
                Toggle("Allow client workloads to access the public internet", isOn: $grant.allowsPublicEgress)
                Toggle("Allow workload port forwards on this Mac", isOn: $grant.dynamicHostLoopbackEnabled)
                LabeledContent("Per-client maximum", value: "1 CPU · 512 MiB memory · 4 GiB disk")
                LabeledContent("Aggregate maximum", value: "2 CPU · 1 GiB memory · 8 GiB disk")
            }
            HStack {
                Spacer()
                Button("Cancel") { showingSetupPolicy = false }
                Button("Set Up MCP") {
                    showingSetupPolicy = false
                    Task { _ = await model.installBundledMCP(grant: grant) }
                }.buttonStyle(.borderedProminent).disabled(model.applianceActionInProgress)
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
                        MCPClientRow(model: model, binding: binding, applianceEnabled: appliance.enabled)
                        if binding.id != model.mcpClientBindings.last?.id { Divider() }
                    }
                }
                HStack {
                    if !model.mcpClientBindings.contains(where: { $0.kind == .codex }) {
                        Button("Set up Codex") { Task { await model.installCodexMCPConfiguration() } }
                            .buttonStyle(.borderedProminent).disabled(!appliance.enabled || !model.canConfigureCodex)
                    }
                    Button("Add other client…") { showingOtherClient = true }.disabled(!appliance.enabled)
                    if !model.canConfigureCodex { Text("Codex CLI not found").font(.caption).foregroundStyle(.secondary) }
                }
                if showingOtherClient {
                    Divider()
                    HStack {
                        TextField("Client name", text: $manualName, prompt: Text("Pi or another MCP client"))
                        Button("Create setup command") {
                            let name = manualName.trimmingCharacters(in: .whitespacesAndNewlines)
                            if !name.isEmpty, model.createManualMCPConfiguration(displayName: name) != nil {
                                manualName = ""
                                showingOtherClient = false
                            }
                        }.disabled(manualName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                    }
                }
            }.padding(8)
        }
    }

    private func accessPolicy(_ appliance: ApplianceStatusModel) -> some View {
        GroupBox("Access policy") {
            VStack(alignment: .leading, spacing: 8) {
                LabeledContent("Capacity", value: "Up to \(appliance.grant.maxEnvironments) clients")
                LabeledContent("Internet", value: appliance.grant.allowsPublicEgress ? "Allowed" : "Blocked")
                LabeledContent("Port forwarding", value: appliance.grant.dynamicHostLoopbackEnabled ? "Local Mac only" : "Disabled")
                Text("These limits apply to every client environment.").font(.caption).foregroundStyle(.secondary)
            }.padding(8)
        }
    }

    private func healthAndAdvanced(_ appliance: ApplianceStatusModel) -> some View {
        DisclosureGroup("Health and advanced") {
            VStack(alignment: .leading, spacing: 10) {
                LabeledContent("State", value: appliance.state)
                LabeledContent("Health", value: appliance.healthy ? "Healthy" : (appliance.healthMessage ?? "Needs attention"))
                LabeledContent("MCP endpoint", value: appliance.enabled && appliance.frontends.contains(where: \.available) ? "Available on this Mac" : "Closed")
                HStack {
                    if appliance.enabled {
                        Button("Disable…") { showingDisable = true }
                        Button("Restart") { Task { await model.restartBundledMCP() } }
                    } else {
                        Button("Enable") { Task { await model.setBundledMCPEnabled(true) } }.buttonStyle(.borderedProminent)
                    }
                    Spacer()
                    Button("Remove MCP…", role: .destructive) { loadRemovePlan() }
                }
            }.padding(.top, 8)
        }
    }

    private func status(_ appliance: ApplianceStatusModel?) -> some View {
        let label: String
        let symbol: String
        let kind: AccessStatusView.Kind
        if let appliance {
            label = appliance.enabled ? (appliance.healthy ? "Ready" : "Needs attention") : "Disabled"
            symbol = appliance.enabled && appliance.healthy ? "checkmark.circle.fill" : "pause.circle"
            kind = appliance.enabled && appliance.healthy ? .success : .attention
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
        Task {
            do { removePlan = try await model.planBundledMCPRemoval(); showingRemove = true }
            catch { model.errorMessage = hostErrorMessage(error) }
        }
    }

    @ViewBuilder private var removeSheet: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Remove MCP Appliance").font(.title2.weight(.semibold))
            if let plan = removePlan {
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
                    Button("Permanently Remove", role: .destructive) { Task { if await model.removeBundledMCP(plan: plan) { showingRemove = false } } }
                        .disabled(!plan.blockers.isEmpty || model.applianceActionInProgress)
                }
            } else { ProgressView() }
        }.padding(24).frame(width: 560)
    }
}

private struct MCPClientRow: View {
    @Bindable var model: AppModel
    let binding: MCPClientBindingModel
    let applianceEnabled: Bool

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: binding.kind == .codex ? "terminal" : "curlybraces")
                .font(.title3).foregroundStyle(.blue).frame(width: 28)
            VStack(alignment: .leading, spacing: 4) {
                Text(binding.displayName).font(.headline)
                Text(binding.kind == .codex ? "Managed Codex MCP client" : "MCP client · setup command")
                    .font(.caption).foregroundStyle(.secondary)
                AccessStatusView(
                    label: applianceEnabled ? (binding.kind == .codex ? "Managed setup" : "Setup command ready") : "MCP disabled",
                    symbol: applianceEnabled ? "checkmark.circle" : "pause.circle",
                    kind: applianceEnabled ? .success : .neutral
                )
            }
            Spacer()
            if binding.kind == .codex {
                HStack(spacing: 8) {
                    Button("Configure Codex") { Task { await model.installCodexMCPConfiguration() } }
                        .disabled(!applianceEnabled || !model.canConfigureCodex)
                    Menu {
                        Button("Remove Codex setup", role: .destructive) {
                            Task { await model.removeCodexMCPConfiguration() }
                        }
                    } label: {
                        Image(systemName: "ellipsis.circle")
                    }
                    .menuStyle(.borderlessButton)
                }
            } else if let command = model.manualMCPCommand(for: binding) {
                VStack(alignment: .trailing, spacing: 8) {
                    Button("Copy setup command") {
                        NSPasteboard.general.clearContents()
                        NSPasteboard.general.setString(command, forType: .string)
                    }.disabled(!applianceEnabled)
                    DisclosureGroup("Show setup command") {
                        Text(command).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                            .frame(maxWidth: 360, alignment: .trailing)
                    }
                    .fixedSize(horizontal: false, vertical: true)
                }
            }
        }
        .padding(.vertical, 4)
    }
}
