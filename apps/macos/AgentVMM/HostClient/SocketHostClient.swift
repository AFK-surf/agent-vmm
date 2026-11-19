import Foundation
import SwiftProtobuf

final class SocketHostClient: HostServing, Sendable {
    let service: VmmRpcClient
    let applianceService: VmmRpcClient

    init(socketPath: String) {
        service = VmmRpcClient(path: socketPath, service: "agentvmm.host.v1.OperatorService")
        applianceService = VmmRpcClient(path: socketPath, service: "agentvmm.appliance.v1.ApplianceAdminService")
    }

    func snapshot() async throws -> SystemSnapshotModel {
        let response = try await service.unary("GetSystemSnapshot", Agentvmm_Host_V1_GetSystemSnapshotRequest(), as: Agentvmm_Host_V1_GetSystemSnapshotResponse.self)
        return response.domainModel
    }

    func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error> {
        var request = Agentvmm_Host_V1_WatchEventsRequest()
        request.afterSequence = sequence
        let query = request
        return serverStream({ emit in
            try await self.service.receive("WatchEvents", query, as: Agentvmm_Host_V1_WatchEventsResponse.self, emit: emit)
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
            try await self.service.receive("Logs", input, as: Agentvmm_Host_V1_OperatorServiceLogsResponse.self, emit: emit)
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
            try await self.service.receive("WatchMetrics", input, as: Agentvmm_Host_V1_WatchMetricsResponse.self, emit: emit)
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
        return try await service.unary("ListEnvironmentImages", request, as: Agentvmm_Host_V1_ListEnvironmentImagesResponse.self).images.map {
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
        _ = try await service.unary("DeleteEnvironmentImage", request, as: Agentvmm_Host_V1_DeleteEnvironmentImageResponse.self)
    }

    func getVMImpactSummary() async throws -> Agentvmm_Host_V1_GetVMImpactSummaryResponse {
        try await service.unary("GetVMImpactSummary", Agentvmm_Host_V1_GetVMImpactSummaryRequest(), as: Agentvmm_Host_V1_GetVMImpactSummaryResponse.self)
    }

    func startVM(fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StartVMRequest()
        request.requestID = UUID().uuidString
        request.fence = fence
        return (try await service.unary("StartVM", request, as: Agentvmm_Host_V1_StartVMResponse.self).operation).domainModel
    }

    func stopVM(force: Bool, fence: Agentvmm_Host_V1_VMMutationFence) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StopVMRequest()
        request.requestID = UUID().uuidString
        request.force = force
        request.fence = fence
        return (try await service.unary("StopVM", request, as: Agentvmm_Host_V1_StopVMResponse.self).operation).domainModel
    }

    func stopContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceStopContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.unary("StopContainer", request, as: Agentvmm_Host_V1_OperatorServiceStopContainerResponse.self)
    }

    func continueContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_ContinueContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.unary("ContinueContainer", request, as: Agentvmm_Host_V1_ContinueContainerResponse.self)
    }

    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceSignalContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        request.signal = signal == .kill ? .kill : .int
        _ = try await service.unary("SignalContainer", request, as: Agentvmm_Host_V1_OperatorServiceSignalContainerResponse.self)
    }

    func deleteContainer(environmentID: String, containerID: String, expectedGenerationID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorServiceDeleteContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        request.expectedGenerationID = expectedGenerationID
        _ = try await service.unary("DeleteContainer", request, as: Agentvmm_Host_V1_OperatorServiceDeleteContainerResponse.self)
    }

    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async throws {
        var request = Agentvmm_Host_V1_UpdateEnvironmentPolicyRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.limits.pids = limits.pids
        request.limits.diskBytes = limits.diskBytes
        request.egressMode = allowsPublicEgress ? .publicInternet : .denyAll
        request.expectedRevision = expectedRevision
        _ = try await service.unary("UpdateEnvironmentPolicy", request, as: Agentvmm_Host_V1_UpdateEnvironmentPolicyResponse.self)
    }

    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview {
        let plan: Agentvmm_Host_V1_CleanupPlan
        switch mode {
        case .reset:
            var request = Agentvmm_Host_V1_PlanResetEnvironmentRequest()
            request.environmentID = environmentID
            plan = try await service.unary("PlanResetEnvironment", request, as: Agentvmm_Host_V1_PlanResetEnvironmentResponse.self).plan
        case .discard:
            var request = Agentvmm_Host_V1_PlanDiscardEnvironmentRequest()
            request.environmentID = environmentID
            plan = try await service.unary("PlanDiscardEnvironment", request, as: Agentvmm_Host_V1_PlanDiscardEnvironmentResponse.self).plan
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
        let response = try await service.unary("ExecuteCleanup", request, as: Agentvmm_Host_V1_ExecuteCleanupResponse.self)
        return response.operation.domainModel
    }

    func getOperation(id: String) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_GetOperationRequest()
        request.operationID = id
        return (try await service.unary("GetOperation", request, as: Agentvmm_Host_V1_GetOperationResponse.self).operation).domainModel
    }

    func runDiagnostics(environmentID: String?) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_RunDiagnosticsRequest()
        request.requestID = UUID().uuidString
        if let environmentID { request.environmentID = environmentID }
        return (try await service.unary("RunDiagnostics", request, as: Agentvmm_Host_V1_RunDiagnosticsResponse.self).operation).domainModel
    }

    func supportPreview(environmentID: String?) async throws -> [SupportItem] {
        var request = Agentvmm_Host_V1_PreviewSupportBundleRequest()
        if let environmentID { request.environmentID = environmentID }
        return try await service.unary("PreviewSupportBundle", request, as: Agentvmm_Host_V1_PreviewSupportBundleResponse.self).items.map {
            SupportItem(name: $0.name, description: $0.description_p, sensitive: $0.sensitive, includedByDefault: $0.includedByDefault)
        }
    }

    func exportSupportBundle(environmentID: String?, sensitiveItems: [String]) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_ExportSupportBundleRequest()
        request.requestID = UUID().uuidString
        if let environmentID { request.environmentID = environmentID }
        request.includeSensitiveItems = sensitiveItems
        return (try await service.unary("ExportSupportBundle", request, as: Agentvmm_Host_V1_ExportSupportBundleResponse.self).operation).domainModel
    }

    func deviceIdentity() async throws -> DeviceIdentityModel {
        (try await service.unary("GetDeviceIdentity", Agentvmm_Host_V1_GetDeviceIdentityRequest(), as: Agentvmm_Host_V1_GetDeviceIdentityResponse.self)).domainModel
    }

    func listPersonalMeshes() async throws -> [PersonalMeshModel] {
        try await service.unary("ListPersonalMeshes", Agentvmm_Host_V1_ListPersonalMeshesRequest(), as: Agentvmm_Host_V1_ListPersonalMeshesResponse.self).meshes.map(\.domainModel)
    }

    func createPersonalMesh(registryAudience: String) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_CreatePersonalMeshRequest()
        request.requestID = UUID().uuidString
        request.registryAudience = registryAudience
        return (try await service.unary("CreatePersonalMesh", request, as: Agentvmm_Host_V1_CreatePersonalMeshResponse.self).mesh).domainModel
    }

    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_RevokePersonalMeshMemberRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.deviceID = deviceID
        request.expectedRevision = expectedRevision
        return (try await service.unary("RevokePersonalMeshMember", request, as: Agentvmm_Host_V1_RevokePersonalMeshMemberResponse.self).mesh).domainModel
    }

    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String, canManageMembers: Bool) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_BeginPersonalMeshPairingRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.expectedRevision = expectedRevision
        request.pin = pin
        request.permissions = canManageMembers ? [.useServices, .manageMembers] : [.useServices]
        return try (try await service.unary("BeginPersonalMeshPairing", request, as: Agentvmm_Host_V1_BeginPersonalMeshPairingResponse.self)).domainModel
    }

    func joinPersonalMeshPairing(invitationCode: String, pin: String) async throws -> PersonalMeshPairingModel {
        guard let data = Data(base64Encoded: invitationCode) else { throw HostClientError.invalidResponse("pairing invitation") }
        var request = Agentvmm_Host_V1_JoinPersonalMeshPairingRequest()
        request.invitation = try Agentvmm_Host_V1_PersonalMeshPairingInvitation(serializedBytes: data)
        request.pin = pin
        return try (try await service.unary("JoinPersonalMeshPairing", request, as: Agentvmm_Host_V1_JoinPersonalMeshPairingResponse.self)).domainModel
    }

    func getPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_GetPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.unary("GetPersonalMeshPairing", request, as: Agentvmm_Host_V1_GetPersonalMeshPairingResponse.self)).domainModel
    }

    func confirmPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_ConfirmPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.unary("ConfirmPersonalMeshPairing", request, as: Agentvmm_Host_V1_ConfirmPersonalMeshPairingResponse.self)).domainModel
    }

    func rejectPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_RejectPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try (try await service.unary("RejectPersonalMeshPairing", request, as: Agentvmm_Host_V1_RejectPersonalMeshPairingResponse.self)).domainModel
    }

    func repairDeviceIdentity(confirmMembershipReset: Bool) async throws -> DeviceIdentityModel {
        var request = Agentvmm_Host_V1_RepairDeviceIdentityRequest()
        request.requestID = UUID().uuidString
        request.confirmMembershipReset = confirmMembershipReset
        return (try await service.unary("RepairDeviceIdentity", request, as: Agentvmm_Host_V1_RepairDeviceIdentityResponse.self)).domainModel
    }


}

extension SocketHostClient {
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
