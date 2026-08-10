import Foundation

extension AppModel {
    func prepareCodexIntegration(displayName: String, environmentID: String?) async -> AgentIntegrationModel? {
        guard let client else { return nil }
        do {
            let prepared = try await client.prepareAgentIntegration(
                adapterID: "codex", displayName: displayName, scope: .user,
                configTarget: integrationConfiguration.codexConfigTarget(), environmentID: environmentID
            )
            upsertIntegration(prepared)
            return prepared
        } catch { errorMessage = hostErrorMessage(error); return nil }
    }

    func installCodexIntegration(_ prepared: AgentIntegrationModel) async -> Bool {
        guard let client else { return false }
        let current = integrations.first { $0.id == prepared.id } ?? prepared
        var configuring: AgentIntegrationModel?
        integrationStage = .starting
        defer { integrationStage = nil }
        do {
            let started = try await client.beginAgentIntegrationConfiguration(id: current.id, expectedRevision: current.revision)
            configuring = started
            upsertIntegration(started)
            integrationStage = .writingConfiguration
            let fingerprint = try await integrationConfiguration.installCodex(integrationID: started.id)
            integrationStage = .finalizing
            let committed = try await client.commitAgentIntegration(
                id: started.id, expectedRevision: started.revision,
                fingerprint: fingerprint, needsRestart: true
            )
            upsertIntegration(committed)
            try await refresh()
            return true
        } catch {
            let message = hostErrorMessage(error)
            if let configuring, let failed = try? await client.failAgentIntegration(
                id: configuring.id, expectedRevision: configuring.revision,
                code: "CONFIG_INSTALL_FAILED", message: "The MCP client configuration could not be installed."
            ) {
                upsertIntegration(failed)
            }
            try? await refresh()
            errorMessage = message
            return false
        }
    }

    func prepareManualIntegration(displayName: String, environmentID: String?) async -> AgentIntegrationModel? {
        guard let client else { return nil }
        do {
            let integration = try await client.prepareAgentIntegration(
                adapterID: "manual", displayName: displayName, scope: .manual,
                configTarget: "Manual MCP configuration", environmentID: environmentID
            )
            try await refresh()
            return integration
        } catch { errorMessage = hostErrorMessage(error); return nil }
    }

    func manualCommand(for integration: AgentIntegrationModel) -> String? {
        try? integrationConfiguration.manualCommand(integrationID: integration.id)
    }

    var canConfigureCodex: Bool { integrationConfiguration.canConfigureCodex() }

    func markManualIntegrationReady(_ integration: AgentIntegrationModel) async -> Bool {
        guard let client else { return false }
        do {
            _ = try await client.commitAgentIntegration(
                id: integration.id, expectedRevision: integration.revision,
                fingerprint: "manual", needsRestart: true
            )
            try await refresh()
            return true
        } catch { errorMessage = hostErrorMessage(error); return false }
    }

    func cancelPreparedIntegration(_ integration: AgentIntegrationModel) async -> Bool {
        guard let client else { return false }
        let current = integrations.first { $0.id == integration.id } ?? integration
        guard current.configState == .pending else {
            errorMessage = "Setup already changed the Codex configuration. Close this window and review the agent before removing access."
            return false
        }
        do {
            let removing = try await client.beginAgentIntegrationRemoval(id: current.id, expectedRevision: current.revision)
            _ = try await client.completeAgentIntegrationRemoval(id: current.id, expectedRevision: removing.revision)
            try await refresh()
            return true
        } catch {
            let message = hostErrorMessage(error)
            try? await refresh()
            errorMessage = message
            return false
        }
    }

    func currentConfiguration(for integration: AgentIntegrationModel) async -> String {
        guard integration.adapterID == "codex" else {
            return "Manual configuration cannot be read by Agent VMM."
        }
        do { return try await integrationConfiguration.currentCodexConfiguration(integrationID: integration.id) }
        catch { return hostErrorMessage(error) }
    }

    func repairIntegration(_ integration: AgentIntegrationModel) async -> Bool {
        guard let client, integration.adapterID == "codex" else { return false }
        do {
            let fingerprint = try await integrationConfiguration.repairCodex(integrationID: integration.id)
            _ = try await client.commitAgentIntegration(
                id: integration.id, expectedRevision: integration.revision,
                fingerprint: fingerprint, needsRestart: true
            )
            try await refresh()
            return true
        } catch {
            _ = try? await client.failAgentIntegration(
                id: integration.id, expectedRevision: integration.revision,
                code: "CONFIG_REPAIR_FAILED", message: "The MCP client configuration could not be repaired."
            )
            errorMessage = hostErrorMessage(error)
            try? await refresh()
            return false
        }
    }

    func disconnectIntegration(_ integration: AgentIntegrationModel) async {
        do { try await client?.revokeAgentSessions(integrationID: integration.id); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func removeIntegration(_ integration: AgentIntegrationModel) async -> Bool {
        guard let client else { return false }
        if integration.configurationRemoval == .requiresRepair {
            errorMessage = "Review and repair the MCP configuration before removing this access."
            return false
        }
        do {
            let removing = try await client.beginAgentIntegrationRemoval(
                id: integration.id, expectedRevision: integration.revision
            )
            do {
                if case .verified(let fingerprint) = integration.configurationRemoval {
                    try await integrationConfiguration.removeCodex(
                        integrationID: integration.id, expectedFingerprint: fingerprint
                    )
                }
                _ = try await client.completeAgentIntegrationRemoval(
                    id: integration.id, expectedRevision: removing.revision
                )
            } catch {
                let code = error is AgentIntegrationConfiguration.ConfigurationError ? "CONFIG_CONFLICT" : "CONFIG_REMOVE_FAILED"
                _ = try? await client.failAgentIntegration(
                    id: integration.id, expectedRevision: removing.revision, code: code,
                    message: "The MCP client configuration could not be removed safely."
                )
                throw error
            }
            try await refresh()
            return true
        } catch { errorMessage = hostErrorMessage(error); try? await refresh(); return false }
    }

    func finishIntegrationRemoval(_ integration: AgentIntegrationModel) async {
        guard let client, integration.configState == .removing else { return }
        if integration.configurationRemoval == .requiresRepair {
            errorMessage = "Review and repair the MCP configuration before retrying removal."
            return
        }
        do {
            if case .verified(let fingerprint) = integration.configurationRemoval {
                try await integrationConfiguration.removeCodex(
                    integrationID: integration.id, expectedFingerprint: fingerprint
                )
            }
            _ = try await client.completeAgentIntegrationRemoval(
                id: integration.id, expectedRevision: integration.revision
            )
            try await refresh()
        } catch {
            _ = try? await client.failAgentIntegration(
                id: integration.id, expectedRevision: integration.revision,
                code: "CONFIG_REMOVE_FAILED", message: "The MCP client configuration could not be removed safely."
            )
            errorMessage = hostErrorMessage(error)
            try? await refresh()
        }
    }

    private func upsertIntegration(_ integration: AgentIntegrationModel) {
        if let index = integrations.firstIndex(where: { $0.id == integration.id }) {
            integrations[index] = integration
        } else {
            integrations.append(integration)
            integrations.sort { ($0.displayName, $0.id) < ($1.displayName, $1.id) }
        }
    }
}
