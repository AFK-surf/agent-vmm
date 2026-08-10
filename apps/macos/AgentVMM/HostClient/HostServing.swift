import Foundation
import GRPCCore

protocol HostServing: Sendable {
    func snapshot() async throws -> SystemSnapshotModel
    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error>
    func metrics(environmentID: String?, containerID: String?) -> AsyncThrowingStream<UsageSnapshot, Error>
    func images(environmentID: String) async throws -> [EnvironmentImageModel]
    func deleteImage(environmentID: String, reference: String, expectedGenerationID: String, expectedDigest: String) async throws
    func logs(environmentID: String, containerID: String, since: Date?, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error>
    func startVM() async throws -> OperationModel
    func stopVM(force: Bool) async throws -> OperationModel
    func stopContainer(environmentID: String, containerID: String) async throws
    func continueContainer(environmentID: String, containerID: String) async throws
    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws
    func deleteContainer(environmentID: String, containerID: String, expectedGenerationID: String) async throws
    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async throws
    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview
    func executeCleanup(_ preview: CleanupPreview) async throws -> OperationModel
    func getOperation(id: String) async throws -> OperationModel
    func runDiagnostics(environmentID: String?) async throws -> OperationModel
    func supportPreview(environmentID: String?) async throws -> [SupportItem]
    func exportSupportBundle(environmentID: String?, sensitiveItems: [String]) async throws -> OperationModel
    func beginRemoteEnrollment(endpoint: String, displayName: String, pool: RemotePoolSettings) async throws -> RemoteRegistrationModel
    func completeRemoteEnrollment(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func failRemoteEnrollment(registrationID: String, expectedRevision: UInt64, code: String, message: String) async throws -> RemoteRegistrationModel
    func retryRemoteEnrollment(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func removeIncompleteRemoteRegistration(registrationID: String, expectedRevision: UInt64) async throws
    func beginRemoteRevocation(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func completeRemoteRevocation(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func updateRemotePool(_ pool: RemotePoolSettings, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func setRemoteAdmission(_ state: RemoteAdmissionState, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func listAgentIntegrations() async throws -> [AgentIntegrationModel]
    func prepareAgentIntegration(adapterID: String, displayName: String, scope: AgentIntegrationModel.Scope, configTarget: String, environmentID: String?) async throws -> AgentIntegrationModel
    func beginAgentIntegrationConfiguration(id: String, expectedRevision: UInt64) async throws -> AgentIntegrationModel
    func commitAgentIntegration(id: String, expectedRevision: UInt64, fingerprint: String, needsRestart: Bool) async throws -> AgentIntegrationModel
    func failAgentIntegration(id: String, expectedRevision: UInt64, code: String, message: String) async throws -> AgentIntegrationModel
    func beginAgentIntegrationRemoval(id: String, expectedRevision: UInt64) async throws -> AgentIntegrationModel
    func completeAgentIntegrationRemoval(id: String, expectedRevision: UInt64) async throws -> AgentIntegrationModel
    func listAgentSessions(integrationID: String?, includeTerminal: Bool) async throws -> [AgentSessionModel]
    func revokeAgentSessions(integrationID: String) async throws
}

extension HostServing {
    func metrics(environmentID: String?) -> AsyncThrowingStream<UsageSnapshot, Error> {
        metrics(environmentID: environmentID, containerID: nil)
    }
}

enum ContainerSignal: Sendable { case interrupt, kill }

enum HostClientError: LocalizedError {
    case invalidResponse(String)
    case disconnected

    var errorDescription: String? {
        switch self {
        case .invalidResponse(let detail): "The control service returned invalid data: \(detail)"
        case .disconnected: "The control service connection ended."
        }
    }
}

func hostErrorMessage(_ error: any Error) -> String {
    if let rpcError = error as? RPCError {
        switch rpcError.code {
        case .unavailable, .unknown:
            return "Agent VMM is temporarily unavailable. Try again."
        case .deadlineExceeded:
            return "Agent VMM did not respond in time. Try again."
        default:
            break
        }
        if !rpcError.message.isEmpty { return rpcError.message }
        return "The control service request failed (\(rpcError.code))."
    }
    return error.localizedDescription
}
