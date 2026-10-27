import Foundation

extension AppModel {
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
