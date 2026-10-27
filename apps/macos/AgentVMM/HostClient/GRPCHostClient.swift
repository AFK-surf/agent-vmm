import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import SwiftProtobuf

final class GRPCHostClient: HostServing, Sendable {
    private let client: GRPCClient<HTTP2ClientTransport.Posix>
    private let service: Agentvmm_Host_V1_OperatorService.Client<HTTP2ClientTransport.Posix>
    let applianceService: Agentvmm_Appliance_V1_ApplianceAdminService.Client<HTTP2ClientTransport.Posix>
    private let runTask: Task<Void, Error>

    init(socketPath: String) throws {
        let transport = try HTTP2ClientTransport.Posix(
            target: .unixDomainSocket(path: socketPath),
            transportSecurity: .plaintext
        )
        let client = GRPCClient(transport: transport)
        self.client = client
        self.service = Agentvmm_Host_V1_OperatorService.Client(wrapping: client)
        self.applianceService = Agentvmm_Appliance_V1_ApplianceAdminService.Client(wrapping: client)
        self.runTask = Task { try await client.runConnections() }
    }

    deinit {
        client.beginGracefulShutdown()
        runTask.cancel()
    }

    func snapshot() async throws -> SystemSnapshotModel {
        let response = try await service.getSystemSnapshot(.init())
        return (response).domainModel
    }

    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error> {
        var request = Agentvmm_Host_V1_WatchEventsRequest()
        request.afterSequence = sequence
        let query = request
        return serverStream({ emit in
            try await self.service.watchEvents(query) { response in
                for try await value in response.messages { emit(value) }
            }
        }, map: { (value: Agentvmm_Host_V1_WatchEventsResponse) in value.domainModel })
    }

    func logs(environmentID: String, containerID: String, since: Date?, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error> {
        var query = Agentvmm_Host_V1_LogsRequest()
        query.containerID = containerID
        query.tailRecords = cursor == nil ? 5_000 : 0
        if let since { query.since = Google_Protobuf_Timestamp(date: since) }
        query.follow = true
        query.afterCursor = cursor ?? ""
        var request = Agentvmm_Host_V1_OperatorServiceLogsRequest()
        request.environmentID = environmentID
        request.query = query
        let input = request
        return serverStream({ emit in
            try await self.service.logs(input) { response in
                for try await value in response.messages { emit(value) }
            }
        }, map: { (value: Agentvmm_Host_V1_OperatorServiceLogsResponse) in value.domainModel })
    }

    func metrics(environmentID: String?, containerID: String?) -> AsyncThrowingStream<UsageSnapshot, Error> {
        var request = Agentvmm_Host_V1_WatchMetricsRequest()
        request.interval.seconds = 1
        if let environmentID {
            request.scope.kind = containerID == nil ? "environment" : "container"
            request.scope.environmentID = environmentID
            request.scope.resourceID = containerID ?? ""
        }
        let input = request
        return serverStream({ emit in
            try await self.service.watchMetrics(input) { response in
                for try await value in response.messages { emit(value) }
            }
        }, map: { (value: Agentvmm_Host_V1_WatchMetricsResponse) in value.usage.domainModel })
    }

    private func serverStream<Message: Sendable, Value: Sendable>(
        _ receive: @escaping @Sendable (@escaping @Sendable (Message) -> Void) async throws -> Void,
        map: @escaping @Sendable (Message) -> Value?
    ) -> AsyncThrowingStream<Value, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    try await receive { message in
                        if let value = map(message) { continuation.yield(value) }
                    }
                    continuation.finish()
                } catch { continuation.finish(throwing: error) }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    func images(environmentID: String) async throws -> [EnvironmentImageModel] {
        var request = Agentvmm_Host_V1_ListEnvironmentImagesRequest()
        request.environmentID = environmentID
        return try await service.listEnvironmentImages(request).images.map {
            EnvironmentImageModel(
                reference: $0.reference,
                digest: $0.digest,
                logicalBytes: $0.logicalBytes,
                platforms: $0.platforms,
                usedByContainerIDs: $0.usedByContainerIds,
                generationID: $0.generationID
            )
        }
    }

    func deleteImage(environmentID: String, reference: String, expectedGenerationID: String, expectedDigest: String) async throws {
        var request = Agentvmm_Host_V1_DeleteEnvironmentImageRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.reference = reference
        request.expectedGenerationID = expectedGenerationID
        request.expectedDigest = expectedDigest
        _ = try await service.deleteEnvironmentImage(request)
    }

    func getVMImpactSummary() async throws -> Agentvmm_Host_V1_GetVMImpactSummaryResponse {
        try await service.getVMImpactSummary(.init())
    }

    func startVM(fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StartVMRequest()
        request.requestID = UUID().uuidString
        request.fence = fence
        return (try await service.startVM(request).operation).domainModel
    }

    func stopVM(force: Bool, fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StopVMRequest()
        request.requestID = UUID().uuidString
        request.force = force
        request.fence = fence
        return (try await service.stopVM(request).operation).domainModel
    }

    func stopContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceStopContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.stopContainer(request)
    }

    func continueContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_ContinueContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.continueContainer(request)
    }

    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceSignalContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        request.signal = signal == .kill ? .kill : .int
        _ = try await service.signalContainer(request)
    }

    func deleteContainer(environmentID: String, containerID: String, expectedGenerationID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceDeleteContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        request.expectedGenerationID = expectedGenerationID
        _ = try await service.deleteContainer(request)
    }

    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async throws {
        var request = Agentvmm_Host_V1_UpdateEnvironmentPolicyRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.limits.pids = limits.pids
        request.limits.diskBytes = limits.diskBytes
        request.egressMode = allowsPublicEgress ? .publicInternet : .denyAll
        request.expectedRevision = expectedRevision
        _ = try await service.updateEnvironmentPolicy(request)
    }

    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview {
        let plan: Agentvmm_Host_V1_CleanupPlan
        switch mode {
        case .reset:
            var request = Agentvmm_Host_V1_PlanResetEnvironmentRequest()
            request.environmentID = environmentID
            plan = try await service.planResetEnvironment(request).plan
        case .discard:
            var request = Agentvmm_Host_V1_PlanDiscardEnvironmentRequest()
            request.environmentID = environmentID
            plan = try await service.planDiscardEnvironment(request).plan
        }
        return CleanupPreview(
            mode: mode,
            environmentID: environmentID,
            revision: plan.revision,
            impacts: plan.impacts.map(\.effect),
            volumeIDs: plan.impacts.filter { $0.resource.kind == "volume" }.map { $0.resource.resourceID }
        )
    }

    func executeCleanup(_ preview: CleanupPreview) async throws -> OperationModel {
        var plan = Agentvmm_Host_V1_CleanupPlan()
        plan.environmentID = preview.environmentID
        plan.kind = preview.mode == .reset ? .reset : .discard
        plan.revision = preview.revision
        var request = Agentvmm_Host_V1_ExecuteCleanupRequest()
        request.requestID = UUID().uuidString
        request.plan = plan
        request.confirmedVolumeIds = preview.mode == .discard ? preview.volumeIDs : []
        let response = try await service.executeCleanup(request)
        return (response.operation).domainModel
    }

    func getOperation(id: String) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_GetOperationRequest()
        request.operationID = id
        return (try await service.getOperation(request).operation).domainModel
    }

    func runDiagnostics(environmentID: String?) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_RunDiagnosticsRequest()
        request.requestID = UUID().uuidString
        if let environmentID { request.environmentID = environmentID }
        return (try await service.runDiagnostics(request).operation).domainModel
    }

    func supportPreview(environmentID: String?) async throws -> [SupportItem] {
        var request = Agentvmm_Host_V1_PreviewSupportBundleRequest()
        if let environmentID { request.environmentID = environmentID }
        return try await service.previewSupportBundle(request).items.map {
            SupportItem(name: $0.name, description: $0.description_p, sensitive: $0.sensitive, includedByDefault: $0.includedByDefault)
        }
    }

    func exportSupportBundle(environmentID: String?, sensitiveItems: [String]) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_ExportSupportBundleRequest()
        request.requestID = UUID().uuidString
        if let environmentID { request.environmentID = environmentID }
        request.includeSensitiveItems = sensitiveItems
        return (try await service.exportSupportBundle(request).operation).domainModel
    }

    func deviceIdentity() async throws -> DeviceIdentityModel {
        (try await service.getDeviceIdentity(.init())).domainModel
    }

    func listPersonalMeshes() async throws -> [PersonalMeshModel] {
        try await service.listPersonalMeshes(.init()).meshes.map(\.domainModel)
    }

    func createPersonalMesh(registryAudience: String) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_CreatePersonalMeshRequest()
        request.requestID = UUID().uuidString
        request.registryAudience = registryAudience
        return (try await service.createPersonalMesh(request).mesh).domainModel
    }

    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_RevokePersonalMeshMemberRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.deviceID = deviceID
        request.expectedRevision = expectedRevision
        return (try await service.revokePersonalMeshMember(request).mesh).domainModel
    }

    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String, canManageMembers: Bool) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_BeginPersonalMeshPairingRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.expectedRevision = expectedRevision
        request.pin = pin
        request.permissions = canManageMembers ? [.useServices, .manageMembers] : [.useServices]
        return try (try await service.beginPersonalMeshPairing(request)).domainModel
    }

    func joinPersonalMeshPairing(invitationCode: String, pin: String) async throws -> PersonalMeshPairingModel {
        guard let data = Data(base64Encoded: invitationCode) else { throw HostClientError.invalidResponse("pairing invitation") }
        var request = Agentvmm_Host_V1_JoinPersonalMeshPairingRequest()
        request.invitation = try Agentvmm_Host_V1_PersonalMeshPairingInvitation(serializedBytes: data)
        request.pin = pin
        return try (try await service.joinPersonalMeshPairing(request)).domainModel
    }

    func getPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_GetPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.getPersonalMeshPairing(request)).domainModel
    }

    func confirmPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_ConfirmPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.confirmPersonalMeshPairing(request)).domainModel
    }

    func rejectPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_RejectPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.rejectPersonalMeshPairing(request)).domainModel
    }

    func repairDeviceIdentity(confirmMembershipReset: Bool) async throws -> DeviceIdentityModel {
        var request = Agentvmm_Host_V1_RepairDeviceIdentityRequest()
        request.requestID = UUID().uuidString
        request.confirmMembershipReset = confirmMembershipReset
        return (try await service.repairDeviceIdentity(request)).domainModel
    }


}

extension GRPCHostClient {
    static func vmMutationFence(
        _ summary: Agentvmm_Host_V1_GetVMImpactSummaryResponse
    ) -> Agentvmm_Host_V1_VMMutationFence {
        var fence = Agentvmm_Host_V1_VMMutationFence()
        fence.state = summary.vm.state
        fence.bootID = summary.vm.bootID
        fence.observedWorkloads = summary.knownWorkloads
        fence.wholeVmImpactAcknowledged = true
        return fence
    }

}
