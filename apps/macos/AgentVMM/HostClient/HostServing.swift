import Foundation
import GRPCCore

protocol HostServing: Sendable {
    func snapshot() async throws -> SystemSnapshotModel
    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error>
    func metrics(environmentID: String?, containerID: String?) -> AsyncThrowingStream<UsageSnapshot, Error>
    func images(environmentID: String) async throws -> [EnvironmentImageModel]
    func deleteImage(environmentID: String, reference: String, expectedGenerationID: String, expectedDigest: String) async throws
    func logs(environmentID: String, containerID: String, since: Date?, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error>
    func getVMImpactSummary() async throws -> Agentvmm_Host_V1_GetVMImpactSummaryResponse
    func startVM(fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel
    func stopVM(force: Bool, fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel
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
    func deviceIdentity() async throws -> DeviceIdentityModel
    func listPersonalMeshes() async throws -> [PersonalMeshModel]
    func createPersonalMesh(registryAudience: String) async throws -> PersonalMeshModel
    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async throws -> PersonalMeshModel
    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String, canManageMembers: Bool) async throws -> PersonalMeshPairingModel
    func joinPersonalMeshPairing(invitationCode: String, pin: String) async throws -> PersonalMeshPairingModel
    func getPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel
    func confirmPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel
    func rejectPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel
    func repairDeviceIdentity(confirmMembershipReset: Bool) async throws -> DeviceIdentityModel
    func listAppliances() async throws -> [ApplianceStatusModel]

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
        if !rpcError.message.isEmpty { return rpcError.message }
        return "The control service request failed (\(rpcError.code))."
    }
    return error.localizedDescription
}
