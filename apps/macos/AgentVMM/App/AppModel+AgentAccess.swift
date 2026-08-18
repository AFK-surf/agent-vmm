import Foundation

extension AppModel {
    var bundledMCP: ApplianceStatusModel? {
        appliances.first { $0.id == BundledMCPAppliance.installationID }
    }

    var canConfigureCodex: Bool { mcpClientConfiguration.canConfigureCodex() }

    func installBundledMCP(grant: ApplianceGrantModel) async -> Bool {
        guard let client, applianceAction == nil else { return false }
        applianceAction = .installing
        defer { applianceAction = nil }
        do {
            try await withTimeout(.seconds(60)) {
                try await client.installBundledMCP(artifact: BundledMCPAppliance.loadArtifact(), grant: grant)
            }
            try await waitForAppliance { appliance in
                appliance.state != "Starting" && appliance.state != "Stopping"
            }
            return true
        } catch { errorMessage = hostErrorMessage(error); return false }
    }

    func setBundledMCPEnabled(_ enabled: Bool) async {
        guard let client, let appliance = bundledMCP, applianceAction == nil else { return }
        applianceAction = enabled ? .enabling : .disabling
        defer { applianceAction = nil }
        do {
            try await withTimeout(.seconds(60)) {
                if enabled { try await client.activateAppliance(appliance) }
                else { try await client.stopAppliance(appliance) }
            }
            try await waitForAppliance { current in
                if enabled { return current.enabled && current.endpointAvailable && current.state != "Starting" && current.state != "Stopping" }
                return !current.enabled && current.state != "Starting" && current.state != "Stopping"
            }
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func restartBundledMCP() async {
        guard let client, let appliance = bundledMCP, applianceAction == nil else { return }
        applianceAction = .restarting
        defer { applianceAction = nil }
        do {
            try await withTimeout(.seconds(60)) { try await client.restartAppliance(appliance) }
            try await waitForAppliance { current in
                current.enabled && current.endpointAvailable && current.state != "Starting" && current.state != "Stopping"
            }
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func planBundledMCPRemoval() async throws -> ApplianceRemovePlanModel {
        guard let client, let appliance = bundledMCP, applianceAction == nil else { throw HostClientError.disconnected }
        applianceAction = .planningRemoval
        defer { applianceAction = nil }
        return try await withTimeout(.seconds(30)) { try await client.planRemoveAppliance(appliance) }
    }

    func removeBundledMCP(plan: ApplianceRemovePlanModel) async -> Bool {
        guard let client, let appliance = bundledMCP, plan.blockers.isEmpty, applianceAction == nil else { return false }
        guard plan.revision == appliance.revision else {
            errorMessage = "The removal plan is out of date. Prepare a new removal plan and try again."
            return false
        }
        applianceAction = .removing
        defer { applianceAction = nil }
        do {
            try await withTimeout(.seconds(120)) { try await client.executeRemoveAppliance(appliance, plan: plan) }
            do {
                try mcpClientConfiguration.removeAllBindings()
            } catch {
                // The appliance is already gone; keep the bindings visible so the user can remove them manually.
                errorMessage = hostErrorMessage(error)
            }
            reloadMCPClientBindings()
            try? await waitForAppliance(allowMissing: true) { _ in false }
            return true
        }
        catch { errorMessage = hostErrorMessage(error); return false }
    }

    func installCodexMCPConfiguration() async {
        guard applianceAction == nil, !mcpClientActionInProgress else { return }
        mcpClientActionInProgress = true
        defer { mcpClientActionInProgress = false }
        do { try await mcpClientConfiguration.installCodex(); reloadMCPClientBindings() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func removeCodexMCPConfiguration() async {
        guard applianceAction == nil, !mcpClientActionInProgress else { return }
        mcpClientActionInProgress = true
        defer { mcpClientActionInProgress = false }
        do { try await mcpClientConfiguration.removeCodex(); reloadMCPClientBindings() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func createManualMCPConfiguration(displayName: String) -> MCPClientBindingModel? {
        guard applianceAction == nil, !mcpClientActionInProgress else { return nil }
        mcpClientActionInProgress = true
        defer { mcpClientActionInProgress = false }
        do { let value = try mcpClientConfiguration.createManual(displayName: displayName); reloadMCPClientBindings(); return value }
        catch { errorMessage = hostErrorMessage(error); return nil }
    }

    func removeManualMCPConfiguration(_ binding: MCPClientBindingModel) {
        guard !mcpClientActionInProgress else { return }
        mcpClientActionInProgress = true
        defer { mcpClientActionInProgress = false }
        do {
            try mcpClientConfiguration.removeManual(id: binding.id)
            reloadMCPClientBindings()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    private func waitForAppliance(allowMissing: Bool = false, _ predicate: @escaping (ApplianceStatusModel) -> Bool) async throws {
        for _ in 0..<120 {
            try await refresh()
            if bundledMCP == nil, allowMissing { return }
            if let appliance = bundledMCP, predicate(appliance) { return }
            try await Task.sleep(for: .milliseconds(250))
        }
        throw HostClientError.invalidResponse("MCP appliance status did not settle. Refresh and try again.")
    }

    private func withTimeout<T: Sendable>(_ duration: Duration, operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask {
                try await Task.sleep(for: duration)
                throw HostClientError.invalidResponse("The MCP operation did not finish in time. Refresh and try again.")
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

}
