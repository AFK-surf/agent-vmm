import Foundation

extension AppModel {
    var servicesHealthy: Bool {
        guard let status = lifecycleStatus else { return true }
        let connectorRequired = snapshot.remoteRegistration?.enrollmentState == .ready
        return status.coreHealthy && (!connectorRequired || status.connectorLoaded)
    }

    func refreshLifecycleStatus() async {
        lifecycleStatus = try? await Task.detached { try LifecycleManager.status() }.value
    }

    func repairServices() async {
        lifecycleOperationInProgress = true
        defer { lifecycleOperationInProgress = false }
        do {
            try await Task.detached { try LifecycleManager.repair() }.value
            await refreshLifecycleStatus()
            try? await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func uninstall(purge: Bool) async -> Bool {
        lifecycleOperationInProgress = true
        defer { lifecycleOperationInProgress = false }
        do {
            guard client != nil else { throw HostClientError.disconnected }
            for integration in integrations where integration.configState != .removed {
                guard await removeIntegration(integration) else {
                    throw LifecycleManager.LifecycleError.failed("Resolve the managed MCP configuration conflict before uninstalling.")
                }
            }
            if snapshot.remoteRegistration != nil, !(await revokeRemoteAccess()) {
                throw LifecycleManager.LifecycleError.failed("Controller access could not be revoked. Repair it before uninstalling.")
            }
            if snapshot.vmState == "Running" {
                _ = try await client?.stopVM(force: false)
            }
            try await Task.detached { try LifecycleManager.uninstall(purge: purge) }.value
            return true
        } catch {
            errorMessage = hostErrorMessage(error)
            return false
        }
    }

    func environment(id: String) -> AgentEnvironmentModel? {
        snapshot.environments.first { $0.id == id }
    }

    func sessions(for integrationID: String) -> [AgentSessionModel] {
        sessions.filter { $0.integrationID == integrationID }
    }

    func showAccess(_ tab: AccessTab) {
        accessTab = tab
        section = .access
    }

    func showEnvironment(_ id: String) {
        selectedEnvironmentID = id
        environmentSection = .overview
        section = .environments
    }

    func showActivity() { section = .diagnostics }

    func logStream(environmentID: String, containerID: String, since: Date?, after cursor: String?) throws -> AsyncThrowingStream<LogEntry, Error> {
        guard let client else { throw HostClientError.disconnected }
        return client.logs(environmentID: environmentID, containerID: containerID, since: since, after: cursor)
    }

    func verifyAgentConfigurations() async {
        guard let client else { return }
        var changed = false
        for integration in integrations where integration.adapterID == "codex" &&
            (integration.configState == .ready || integration.configState == .needsRestart) && !integration.fingerprint.isEmpty {
            do {
                let fingerprint = try await integrationConfiguration.codexFingerprint(integrationID: integration.id)
                if fingerprint == integration.fingerprint { continue }
                _ = try? await client.failAgentIntegration(
                    id: integration.id, expectedRevision: integration.revision,
                    code: "CONFIG_CONFLICT", message: "The MCP client configuration changed outside Agent VMM."
                )
                changed = true
            } catch AgentIntegrationConfiguration.ConfigurationError.codexUnavailable {
                continue
            } catch AgentIntegrationConfiguration.ConfigurationError.helperUnavailable {
                continue
            } catch {
                _ = try? await client.failAgentIntegration(
                    id: integration.id, expectedRevision: integration.revision,
                    code: "CONFIG_MISSING", message: "The managed MCP client entry is missing or unreadable."
                )
                changed = true
            }
        }
        if changed { try? await refresh() }
    }
}
