import Foundation
import GRPCCore

protocol HostServing: Sendable {
    func snapshot() async throws -> SystemSnapshotModel
    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error>
    func metrics(environmentID: String?) -> AsyncThrowingStream<UsageSnapshot, Error>
    func logs(environmentID: String, containerID: String, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error>
    func startVM() async throws -> OperationModel
    func stopVM(force: Bool) async throws -> OperationModel
    func stopContainer(environmentID: String, containerID: String) async throws
    func continueContainer(environmentID: String, containerID: String) async throws
    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws
    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async throws
    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview
    func executeCleanup(_ preview: CleanupPreview) async throws -> OperationModel
    func getOperation(id: String) async throws -> OperationModel
    func runDiagnostics(environmentID: String?) async throws -> OperationModel
    func supportPreview(environmentID: String?) async throws -> [SupportItem]
    func exportSupportBundle(environmentID: String?, sensitiveItems: [String]) async throws -> OperationModel
    func createRemoteRegistration(endpoint: String, displayName: String, pool: RemotePoolSettings) async throws -> RemoteRegistrationModel
    func updateRemotePool(_ pool: RemotePoolSettings, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
    func setRemoteRegistrationState(_ state: Agentvmm_Host_V1_RemoteRegistrationState, expectedRevision: UInt64) async throws -> RemoteRegistrationModel
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
