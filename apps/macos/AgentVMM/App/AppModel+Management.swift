import Foundation

extension AppModel {
    var servicesHealthy: Bool {
        guard let status = lifecycleStatus else { return true }
		return status.coreHealthy
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

	func showAccess() { section = .access }

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

}
