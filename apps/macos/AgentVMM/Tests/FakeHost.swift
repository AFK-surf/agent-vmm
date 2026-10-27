import Foundation
import GRPCCore
import Synchronization
@testable import AgentVMM

final class FakeHost: HostServing {
    struct State {
        var online = true
        var snapshotCalls = 0
        var pauseSnapshots = false
        var snapshotWaiters: [CheckedContinuation<Void, Never>] = []
        var events: AsyncThrowingStream<ActivityEvent, Error>.Continuation?
        var metrics: AsyncThrowingStream<UsageSnapshot, Error>.Continuation?
        var appliances: [ApplianceStatusModel] = []
        var bootID = "fake-boot"
        var submittedFence: Agentvmm_Host_V1_VMMutationFence?
    }
    let state = Mutex(State())

    func snapshot() async throws -> SystemSnapshotModel {
        let online = state.withLock { value in
            value.snapshotCalls += 1
            return value.online
        }
        guard online else { throw HostClientError.disconnected }
        await withCheckedContinuation { continuation in
            state.withLock { value in
                if value.pauseSnapshots { value.snapshotWaiters.append(continuation) }
                else { continuation.resume() }
            }
        }
        let bootID = state.withLock { $0.bootID }
        return SystemSnapshotModel(vmState: .running, bootID: bootID, health: .healthy, components: [], usage: nil, environments: [], remoteRegistrations: [], collectedAt: .now)
    }

    func resumeSnapshots() {
        let waiters = state.withLock { value in
            value.pauseSnapshots = false
            let waiters = value.snapshotWaiters
            value.snapshotWaiters.removeAll()
            return waiters
        }
        waiters.forEach { $0.resume() }
    }

    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error> {
        AsyncThrowingStream { continuation in state.withLock { $0.events = continuation } }
    }
    func metrics(environmentID: String?, containerID: String?) -> AsyncThrowingStream<UsageSnapshot, Error> {
        AsyncThrowingStream { continuation in state.withLock { $0.metrics = continuation } }
    }
    func finishStreams() {
        state.withLock { value in
            value.events?.finish(throwing: CancellationError())
            value.metrics?.finish(throwing: CancellationError())
        }
    }
    func listAppliances() async throws -> [ApplianceStatusModel] { state.withLock { $0.appliances } }
    func deviceIdentity() async throws -> DeviceIdentityModel { DeviceIdentityModel(id: "fake-device", rootKeyRevision: 1, state: .ready) }
    func listPersonalMeshes() async throws -> [PersonalMeshModel] { [] }
    func getVMImpactSummary() async throws -> Agentvmm_Host_V1_GetVMImpactSummaryResponse {
        var value = Agentvmm_Host_V1_GetVMImpactSummaryResponse()
        value.vm.state = .running
        value.vm.bootID = state.withLock { $0.bootID }
        value.overflow = true
        return value
    }
    func startVM(fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel { try apply(fence) }
    func stopVM(force: Bool, fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel { try apply(fence) }
    private func apply(_ fence: Agentvmm_Host_V1_VMMutationFence) throws -> OperationModel {
        let current = state.withLock { value in value.submittedFence = fence; return value.bootID }
        guard current == fence.bootID else { throw RPCError(code: .aborted, message: "VM fence changed") }
        return OperationModel(id: "fake-operation", kind: "vm", state: .succeeded, stage: "done", reason: nil, message: nil, resource: nil)
    }
    func images(environmentID: String) async throws -> [EnvironmentImageModel] { throw HostClientError.disconnected }
    func deleteImage(environmentID: String, reference: String, expectedGenerationID: String, expectedDigest: String) async throws { throw HostClientError.disconnected }
    func logs(environmentID: String, containerID: String, since: Date?, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error> { AsyncThrowingStream { $0.finish() } }
    func stopContainer(environmentID: String, containerID: String) async throws { throw HostClientError.disconnected }
    func continueContainer(environmentID: String, containerID: String) async throws { throw HostClientError.disconnected }
    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws { throw HostClientError.disconnected }
    func deleteContainer(environmentID: String, containerID: String, expectedGenerationID: String) async throws { throw HostClientError.disconnected }
    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async throws { throw HostClientError.disconnected }
    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview { throw HostClientError.disconnected }
    func executeCleanup(_ preview: CleanupPreview) async throws -> OperationModel { throw HostClientError.disconnected }
    func getOperation(id: String) async throws -> OperationModel { throw HostClientError.disconnected }
    func runDiagnostics(environmentID: String?) async throws -> OperationModel { throw HostClientError.disconnected }
    func supportPreview(environmentID: String?) async throws -> [SupportItem] { throw HostClientError.disconnected }
    func exportSupportBundle(environmentID: String?, sensitiveItems: [String]) async throws -> OperationModel { throw HostClientError.disconnected }
    func createPersonalMesh(registryAudience: String) async throws -> PersonalMeshModel { throw HostClientError.disconnected }
    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async throws -> PersonalMeshModel { throw HostClientError.disconnected }
    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String, canManageMembers: Bool) async throws -> PersonalMeshPairingModel { throw HostClientError.disconnected }
    func joinPersonalMeshPairing(invitationCode: String, pin: String) async throws -> PersonalMeshPairingModel { throw HostClientError.disconnected }
    func getPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel { throw HostClientError.disconnected }
    func confirmPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel { throw HostClientError.disconnected }
    func rejectPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel { throw HostClientError.disconnected }
    func repairDeviceIdentity(confirmMembershipReset: Bool) async throws -> DeviceIdentityModel { throw HostClientError.disconnected }
}
