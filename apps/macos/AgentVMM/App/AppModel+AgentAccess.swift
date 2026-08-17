import Foundation

extension AppModel {
    var bundledMCP: ApplianceStatusModel? {
        appliances.first { $0.id == BundledMCPAppliance.installationID }
    }

    var canConfigureCodex: Bool { mcpClientConfiguration.canConfigureCodex() }

    func installBundledMCP(grant: ApplianceGrantModel) async -> Bool {
        guard let client, !applianceActionInProgress else { return false }
        applianceActionInProgress = true
        defer { applianceActionInProgress = false }
        do {
            try await client.installBundledMCP(artifact: BundledMCPAppliance.loadArtifact(), grant: grant)
            try await refresh()
            return true
        } catch { errorMessage = hostErrorMessage(error); return false }
    }

    func setBundledMCPEnabled(_ enabled: Bool) async {
        guard let client, let appliance = bundledMCP, !applianceActionInProgress else { return }
        applianceActionInProgress = true
        defer { applianceActionInProgress = false }
        do {
            if enabled { try await client.activateAppliance(appliance) }
            else { try await client.stopAppliance(appliance) }
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func restartBundledMCP() async {
        guard let client, let appliance = bundledMCP, !applianceActionInProgress else { return }
        applianceActionInProgress = true
        defer { applianceActionInProgress = false }
        do { try await client.restartAppliance(appliance); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func planBundledMCPRemoval() async throws -> ApplianceRemovePlanModel {
        guard let client, let appliance = bundledMCP else { throw HostClientError.disconnected }
        return try await client.planRemoveAppliance(appliance)
    }

    func removeBundledMCP(plan: ApplianceRemovePlanModel) async -> Bool {
        guard let client, let appliance = bundledMCP, plan.blockers.isEmpty, !applianceActionInProgress else { return false }
        applianceActionInProgress = true
        defer { applianceActionInProgress = false }
        do {
            try await client.executeRemoveAppliance(appliance, plan: plan)
            try mcpClientConfiguration.removeAllBindings()
            try await refresh()
            return true
        }
        catch { errorMessage = hostErrorMessage(error); return false }
    }

    func installCodexMCPConfiguration() async {
        do { try await mcpClientConfiguration.installCodex(); mcpClientBindings = try mcpClientConfiguration.bindings() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func removeCodexMCPConfiguration() async {
        do { try await mcpClientConfiguration.removeCodex(); mcpClientBindings = try mcpClientConfiguration.bindings() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func createManualMCPConfiguration(displayName: String) -> MCPClientBindingModel? {
        do { let value = try mcpClientConfiguration.createManual(displayName: displayName); mcpClientBindings = try mcpClientConfiguration.bindings(); return value }
        catch { errorMessage = hostErrorMessage(error); return nil }
    }

    func manualMCPCommand(for binding: MCPClientBindingModel) -> String? {
        do { return try mcpClientConfiguration.manualCommand(binding: binding.binding) }
        catch { errorMessage = hostErrorMessage(error); return nil }
    }

}
