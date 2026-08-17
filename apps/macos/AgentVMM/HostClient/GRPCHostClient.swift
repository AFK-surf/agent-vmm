import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import SwiftProtobuf

actor GRPCHostClient: HostServing {
    private let client: GRPCClient<HTTP2ClientTransport.Posix>
    private let service: Agentvmm_Host_V1_OperatorService.Client<HTTP2ClientTransport.Posix>
    let applianceService: Agentvmm_Appliance_V1_ApplianceAdminService.Client<HTTP2ClientTransport.Posix>
    private var runTask: Task<Void, Error>?

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
        runTask?.cancel()
    }

    func snapshot() async throws -> SystemSnapshotModel {
        let response = try await service.getSystemSnapshot(.init())
        return map(response)
    }

    nonisolated func events(after sequence: UInt64) -> AsyncThrowingStream<ActivityEvent, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    var request = Agentvmm_Host_V1_WatchEventsRequest()
                    request.afterSequence = sequence
                    try await self.service.watchEvents(request) { response in
                        for try await event in response.messages {
                            continuation.yield(self.map(event))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    nonisolated func logs(environmentID: String, containerID: String, since: Date?, after cursor: String?) -> AsyncThrowingStream<LogEntry, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    var query = Agentvmm_Host_V1_LogsRequest()
                    query.containerID = containerID
                    query.tailRecords = cursor == nil ? 5_000 : 0
                    if let since { query.since = Google_Protobuf_Timestamp(date: since) }
                    query.follow = true
                    query.afterCursor = cursor ?? ""
                    var request = Agentvmm_Host_V1_OperatorLogsRequest()
                    request.environmentID = environmentID
                    request.query = query
                    try await self.service.logs(request) { response in
                        for try await record in response.messages {
                            continuation.yield(self.map(record))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    nonisolated func metrics(environmentID: String?, containerID: String?) -> AsyncThrowingStream<UsageSnapshot, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    var request = Agentvmm_Host_V1_WatchMetricsRequest()
                    request.interval.seconds = 1
                    if let environmentID {
                        request.scope.kind = containerID == nil ? "environment" : "container"
                        request.scope.environmentID = environmentID
                        request.scope.resourceID = containerID ?? ""
                    }
                    try await self.service.watchMetrics(request) { response in
                        for try await sample in response.messages {
                            if let value = self.usage(sample.usage) { continuation.yield(value) }
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
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

    func startVM() async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StartVMRequest()
        request.requestID = UUID().uuidString
        return operation(try await service.startVM(request).operation)
    }

    func stopVM(force: Bool) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_StopVMRequest()
        request.requestID = UUID().uuidString
        request.force = force
        return operation(try await service.stopVM(request).operation)
    }

    func stopContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorStopContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.stopContainer(request)
    }

    func continueContainer(environmentID: String, containerID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorContainerMutationRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        _ = try await service.continueContainer(request)
    }

    func signalContainer(environmentID: String, containerID: String, signal: ContainerSignal) async throws {
        var request = Agentvmm_Host_V1_OperatorSignalContainerRequest()
        request.requestID = UUID().uuidString
        request.environmentID = environmentID
        request.containerID = containerID
        request.signal = signal == .kill ? .kill : .int
        _ = try await service.signalContainer(request)
    }

    func deleteContainer(environmentID: String, containerID: String, expectedGenerationID: String) async throws {
        var request = Agentvmm_Host_V1_OperatorDeleteContainerRequest()
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
        request.limits.cpuMillis = limits.cpuMillis
        request.limits.memoryBytes = limits.memoryBytes
        request.limits.pids = limits.pids
        request.limits.diskBytes = limits.diskBytes
        request.egressMode = allowsPublicEgress ? .publicInternet : .denyAll
        request.expectedRevision = expectedRevision
        _ = try await service.updateEnvironmentPolicy(request)
    }

    func cleanupPreview(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview {
        var request = Agentvmm_Host_V1_PlanEnvironmentRequest()
        request.environmentID = environmentID
        let response = switch mode {
        case .reset: try await service.planResetEnvironment(request)
        case .discard: try await service.planDiscardEnvironment(request)
        }
        return CleanupPreview(
            mode: mode,
            environmentID: environmentID,
            revision: response.plan.revision,
            impacts: response.plan.impacts.map(\.effect),
            volumeIDs: response.plan.impacts.filter { $0.resource.kind == "volume" }.map { $0.resource.resourceID }
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
        return operation(response.operation)
    }

    func getOperation(id: String) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_GetOperationRequest()
        request.operationID = id
        return operation(try await service.getOperation(request).operation)
    }

    func runDiagnostics(environmentID: String?) async throws -> OperationModel {
        var request = Agentvmm_Host_V1_RunDiagnosticsRequest()
        request.requestID = UUID().uuidString
        if let environmentID { request.environmentID = environmentID }
        return operation(try await service.runDiagnostics(request).operation)
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
        return operation(try await service.exportSupportBundle(request).operation)
    }

    func deviceIdentity() async throws -> DeviceIdentityModel {
        deviceIdentity(try await service.getDeviceIdentity(.init()))
    }

    func listPersonalMeshes() async throws -> [PersonalMeshModel] {
        try await service.listPersonalMeshes(.init()).meshes.map(personalMesh)
    }

    func createPersonalMesh(registryAudience: String) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_CreatePersonalMeshRequest()
        request.requestID = UUID().uuidString
        request.registryAudience = registryAudience
        return personalMesh(try await service.createPersonalMesh(request))
    }

    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async throws -> PersonalMeshModel {
        var request = Agentvmm_Host_V1_RevokePersonalMeshMemberRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.deviceID = deviceID
        request.expectedRevision = expectedRevision
        return personalMesh(try await service.revokePersonalMeshMember(request))
    }

    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_BeginPersonalMeshPairingRequest()
        request.requestID = UUID().uuidString
        request.meshID = meshID
        request.expectedRevision = expectedRevision
        request.pin = pin
        return try pairing(try await service.beginPersonalMeshPairing(request))
    }

    func joinPersonalMeshPairing(invitationCode: String, pin: String, canManageMembers: Bool) async throws -> PersonalMeshPairingModel {
        guard let data = Data(base64Encoded: invitationCode) else { throw HostClientError.invalidResponse("pairing invitation") }
        var request = Agentvmm_Host_V1_JoinPersonalMeshPairingRequest()
        request.invitation = try Agentvmm_Host_V1_PersonalMeshPairingInvitation(serializedBytes: data)
        request.pin = pin
        request.permissions = canManageMembers ? [.useServices, .manageMembers] : [.useServices]
        return try pairing(try await service.joinPersonalMeshPairing(request))
    }

    func getPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_GetPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try pairing(try await service.getPersonalMeshPairing(request))
    }

    func confirmPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_ConfirmPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try pairing(try await service.confirmPersonalMeshPairing(request))
    }

    func rejectPersonalMeshPairing(sessionID: Data) async throws -> PersonalMeshPairingModel {
        var request = Agentvmm_Host_V1_RejectPersonalMeshPairingRequest()
        request.sessionID = sessionID
        return try pairing(try await service.rejectPersonalMeshPairing(request))
    }

    func repairDeviceIdentity(confirmMembershipReset: Bool) async throws -> DeviceIdentityModel {
        var request = Agentvmm_Host_V1_RepairDeviceIdentityRequest()
        request.requestID = UUID().uuidString
        request.confirmMembershipReset = confirmMembershipReset
        return deviceIdentity(try await service.repairDeviceIdentity(request))
    }

    func beginRemoteEnrollment(endpoint: String, displayName: String, pool: RemotePoolSettings) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_BeginRemoteEnrollmentRequest()
        request.requestID = UUID().uuidString
        request.endpoint = endpoint
        request.displayName = displayName
        request.protocolVersion = "1"
        request.pool = remotePool(pool)
        return remoteRegistration(try await service.beginRemoteEnrollment(request).registration)
    }

    func completeRemoteEnrollment(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_CompleteRemoteEnrollmentRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        return remoteRegistration(try await service.completeRemoteEnrollment(request).registration)
    }

    func failRemoteEnrollment(registrationID: String, expectedRevision: UInt64, code: String, message: String) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_FailRemoteEnrollmentRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        request.errorCode = code
        request.errorMessage = message
        return remoteRegistration(try await service.failRemoteEnrollment(request).registration)
    }

    func retryRemoteEnrollment(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_RetryRemoteEnrollmentRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        return remoteRegistration(try await service.retryRemoteEnrollment(request).registration)
    }

    func removeIncompleteRemoteRegistration(registrationID: String, expectedRevision: UInt64) async throws {
        var request = Agentvmm_Host_V1_RemoveIncompleteRemoteRegistrationRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        _ = try await service.removeIncompleteRemoteRegistration(request)
    }

    func beginRemoteRevocation(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_BeginRemoteRevocationRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        return remoteRegistration(try await service.beginRemoteRevocation(request).registration)
    }

    func completeRemoteRevocation(registrationID: String, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_CompleteRemoteRevocationRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        return remoteRegistration(try await service.completeRemoteRevocation(request).registration)
    }

    func updateRemotePool(registrationID: String, _ pool: RemotePoolSettings, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_UpdateRemotePoolRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        request.pool = remotePool(pool)
        return remoteRegistration(try await service.updateRemotePool(request).registration)
    }

    func setRemoteAdmission(registrationID: String, _ state: RemoteAdmissionState, expectedRevision: UInt64) async throws -> RemoteRegistrationModel {
        var request = Agentvmm_Host_V1_SetRemoteRegistrationStateRequest()
        request.requestID = UUID().uuidString
        request.registrationID = registrationID
        request.expectedRevision = expectedRevision
        switch state {
        case .accepting: request.state = .enabled
        case .draining: request.state = .draining
        case .revoked, .unknown: throw HostClientError.invalidResponse("invalid remote admission transition")
        }
        return remoteRegistration(try await service.setRemoteRegistrationState(request).registration)
    }

}

extension GRPCHostClient {
    nonisolated func remotePool(_ value: RemotePoolSettings) -> Agentvmm_Host_V1_RemotePoolPolicy {
        var pool = Agentvmm_Host_V1_RemotePoolPolicy()
        pool.maxEnvironments = value.maxEnvironments
        pool.aggregateLimits = resourceLimits(value.aggregateLimits)
        pool.perEnvironmentLimits = resourceLimits(value.perEnvironmentLimits)
        pool.maxEgressMode = value.allowsPublicEgress ? .publicInternet : .denyAll
        pool.maxLeaseDuration.seconds = value.maxLeaseSeconds
        pool.disconnectGrace.seconds = value.disconnectGraceSeconds
        pool.stopGrace.seconds = value.stopGraceSeconds
        return pool
    }

    nonisolated func resourceLimits(_ value: EnvironmentLimits) -> Agentvmm_Host_V1_ResourceLimits {
        var limits = Agentvmm_Host_V1_ResourceLimits()
        limits.cpuMillis = value.cpuMillis
        limits.memoryBytes = value.memoryBytes
        limits.pids = value.pids
        limits.diskBytes = value.diskBytes
        return limits
    }
    nonisolated func map(_ response: Agentvmm_Host_V1_GetSystemSnapshotResponse) -> SystemSnapshotModel {
        SystemSnapshotModel(
            vmState: vmState(response.vm.state),
            bootID: response.vm.bootID,
            health: health(response.health.state),
            components: response.health.components.map {
                ComponentStatus(id: $0.component, level: health($0.state), reason: $0.reason.isEmpty ? nil : $0.reason, observedAt: $0.observedAt.date)
            },
            usage: usage(response.usage),
            environments: response.environments.map(environment),
            remoteRegistrations: response.remoteRegistrations.map(remoteRegistration),
            collectedAt: response.collectedAt.date
        )
    }

    nonisolated func environment(_ value: Agentvmm_Host_V1_AgentEnvironment) -> AgentEnvironmentModel {
        let inventoryAvailability: InventoryAvailability = switch value.inventoryState {
        case .available: .available
        case .vmStopped: .vmStopped
        case .unavailable: .unavailable
        case .stale: .stale
        default: .unknown
        }
        return AgentEnvironmentModel(
            id: value.id,
            name: value.displayName,
            namespaceID: value.namespaceID,
            health: health(value.health.state),
            usage: usage(value.usage),
            workloads: value.containers.map {
                let state: Workload.State = switch $0.state {
                case .created: .created
                case .running: .running
                case .stopped: .stopped
                default: .unknown
                }
                return Workload(
                    id: $0.id, generationID: $0.generationID, image: $0.image, state: state, exitCode: $0.hasExitCode ? $0.exitCode : nil,
                    argv: $0.argv, environmentKeys: $0.environmentKeys, workdir: $0.hasWorkdir ? $0.workdir : nil,
                    user: $0.hasUser ? $0.user : nil, mounts: $0.volumes.map { "\($0.volumeID) → \($0.destination)" },
                    instanceID: $0.instanceID.nilIfEmpty,
                    createdAt: optionalDate($0.createdAt), startedAt: optionalDate($0.startedAt), finishedAt: optionalDate($0.finishedAt)
                )
            },
            volumes: value.volumes.map {
                RetainedVolume(
                    id: $0.id, bytesUsed: $0.usage.diskBytes, byteLimit: $0.limitBytes, mountedBy: $0.mountedBy,
                    usageObservedAt: optionalDate($0.usage.sampledAt), usageIsStale: $0.usage.stale,
                    usageUnavailableReason: $0.usage.unavailableReason.nilIfEmpty
                )
            },
            limits: EnvironmentLimits(cpuMillis: value.limits.cpuMillis, memoryBytes: value.limits.memoryBytes, pids: value.limits.pids, diskBytes: value.limits.diskBytes),
            egressSummary: value.egressMode == .publicInternet ? "Public internet" : "Blocked by default",
            allowsPublicEgress: value.egressMode == .publicInternet,
            policyRevision: value.policyRevision,
            origin: value.owner.kind == .remoteRegistration ? .remote : .local,
            allocationID: value.owner.allocationID.nilIfEmpty,
            inventoryAvailability: inventoryAvailability,
            inventoryMessage: value.inventoryMessage.nilIfEmpty,
            inventoryObservedAt: optionalDate(value.inventoryObservedAt)
        )
    }

    nonisolated func remoteRegistration(_ value: Agentvmm_Host_V1_RemoteRegistration) -> RemoteRegistrationModel {
        let admissionState: RemoteAdmissionState = switch value.state {
        case .enabled: .accepting
        case .draining: .draining
        case .revoked: .revoked
        default: .unknown
        }
        let connector = connectorStatus(value.connectorStatus)
        let connectionState = RemoteConnectionStatus.derive(
            connected: value.connectionState == .connected,
            connectorPhase: connector.phase,
            observedAt: connector.observedAt,
            disconnectedAt: connector.disconnectedAt,
            disconnectGraceSeconds: value.pool.disconnectGrace.seconds
        )
        let enrollmentState: RemoteEnrollmentStatus = switch value.enrollmentState {
        case .pendingCredential: .pending
        case .ready: .ready
        case .setupIncomplete: .incomplete
        case .revoking: .revoking
        default: .incomplete
        }
        let owner: RemoteRegistrationModel.Owner = switch value.ownerKind {
        case .external: .external
        case .appliance: .appliance(installationID: value.installationID)
        default: .unknown
        }
        return RemoteRegistrationModel(
            id: value.id,
            displayName: value.displayName,
            endpoint: value.endpoint,
            state: admissionState,
            connectionState: connectionState,
            enrollmentState: enrollmentState,
            revision: value.revision,
            inventoryRevision: value.inventoryRevision,
            owner: owner,
            pool: RemotePoolSettings(
                maxEnvironments: value.pool.maxEnvironments,
                aggregateLimits: environmentLimits(value.pool.aggregateLimits),
                perEnvironmentLimits: environmentLimits(value.pool.perEnvironmentLimits),
                allowsPublicEgress: value.pool.maxEgressMode == .publicInternet,
                maxLeaseSeconds: value.pool.maxLeaseDuration.seconds,
                disconnectGraceSeconds: value.pool.disconnectGrace.seconds,
                stopGraceSeconds: value.pool.stopGrace.seconds
            ),
            allocations: value.allocations.map(remoteAllocation),
            connector: connector,
            enrollmentError: value.enrollmentErrorMessage.nilIfEmpty
        )
    }

    nonisolated func environmentLimits(_ value: Agentvmm_Host_V1_ResourceLimits) -> EnvironmentLimits {
        EnvironmentLimits(cpuMillis: value.cpuMillis, memoryBytes: value.memoryBytes, pids: value.pids, diskBytes: value.diskBytes)
    }

    nonisolated func remoteAllocation(_ value: Agentvmm_Host_V1_RemoteAllocation) -> RemoteAllocationModel {
        let leaseState: RemoteLeaseModel.State = switch value.lease.state {
        case .active: .active
        case .expired: .expired
        case .released: .released
        case .revoked: .revoked
        default: .unknown
        }
        return RemoteAllocationModel(
            allocationID: value.allocationID,
            environmentID: value.environmentID,
            displayName: value.displayLabel,
            state: String(describing: value.state).replacingOccurrences(of: "_", with: " ").capitalized,
            limits: environmentLimits(value.limits),
            allowsPublicEgress: value.egressMode == .publicInternet,
            lease: value.hasLease ? RemoteLeaseModel(
                id: value.lease.leaseID,
                workloadReference: value.lease.workloadRef,
                expiresAt: optionalDate(value.lease.expiresAt),
                state: leaseState
            ) : nil
        )
    }

    nonisolated func connectorStatus(_ value: Agentvmm_Host_V1_ConnectorStatus) -> ConnectorStatusModel {
        let phase: ConnectorStatusModel.Phase = switch value.phase {
        case .stopped: .stopped
        case .enrolling: .enrolling
        case .connecting: .connecting
        case .connected: .connected
        case .retryWait: .retryWait
        case .authFailed: .authFailed
        case .configError: .configError
        default: .unknown
        }
        return ConnectorStatusModel(
            phase: phase,
            release: value.connectorRelease,
            observedAt: optionalDate(value.observedAt),
            lastConnectedAt: optionalDate(value.lastConnectedAt),
            disconnectedAt: optionalDate(value.disconnectedAt),
            nextRetryAt: optionalDate(value.nextRetryAt),
            reconnectAttempt: value.reconnectAttempt,
            failureCode: value.failureCode.nilIfEmpty,
            failureMessage: value.failureMessage.nilIfEmpty
        )
    }

    nonisolated func applianceStatus(_ value: Agentvmm_Appliance_V1_ApplianceStatus) -> ApplianceStatusModel {
        let grant = value.approvedGrant
        return ApplianceStatusModel(
            id: value.installationID,
            productID: value.productID,
            revision: value.revision,
            artifactID: value.artifactID,
            state: String(describing: value.applianceState).replacingOccurrences(of: "_", with: " ").capitalized,
            channelState: String(describing: value.channelState).replacingOccurrences(of: "_", with: " ").capitalized,
            enabled: value.enabled,
            healthy: value.health.healthy,
            healthMessage: value.health.status.nilIfEmpty,
            grant: ApplianceGrantModel(
                maxEnvironments: grant.capacity.maxEnvironments,
                aggregateLimits: environmentLimits(grant.capacity.aggregateLimits),
                perEnvironmentLimits: environmentLimits(grant.capacity.perEnvironmentLimits),
                allowsPublicEgress: grant.capacity.maxEgressMode == .publicInternet,
                maxLeaseSeconds: grant.capacity.maxLeaseSeconds,
                disconnectGraceSeconds: grant.capacity.disconnectGraceSeconds,
                enabledFrontendNames: Set(grant.frontends.map(\.name)),
                dynamicHostLoopbackEnabled: grant.hasDynamicHostLoopback
            ),
            frontends: value.frontends.map {
                ApplianceFrontendModel(
                    name: $0.name,
                    transport: String(describing: $0.transport).replacingOccurrences(of: "_", with: " ").capitalized,
                    endpoint: $0.endpoint,
                    available: $0.available
                )
            }
        )
    }

    nonisolated func optionalDate(_ value: Google_Protobuf_Timestamp) -> Date? {
        value.seconds == 0 && value.nanos == 0 ? nil : value.date
    }

    nonisolated func usage(_ value: Agentvmm_Host_V1_Usage) -> UsageSnapshot? {
        guard value.sampledAt.seconds != 0 else { return nil }
        return UsageSnapshot(
            cpuNanos: value.cpuNanos,
            memoryBytes: value.memoryBytes,
            pids: value.pids,
            readBytes: value.readBytes,
            writeBytes: value.writeBytes,
            networkRXBytes: value.networkRxBytes,
            networkTXBytes: value.networkTxBytes,
            diskBytes: value.diskBytes,
            sampledAt: value.sampledAt.date,
            isStale: value.stale,
            unavailableReason: value.unavailableReason.nilIfEmpty
        )
    }

    nonisolated func health(_ value: Agentvmm_Host_V1_HealthState) -> HealthLevel {
        switch value {
        case .healthy: .healthy
        case .degraded: .degraded
        default: .unavailable
        }
    }

    nonisolated func vmState(_ value: Agentvmm_Host_V1_VMState) -> String {
        switch value {
        case .stopped: "Stopped"
        case .starting: "Starting"
        case .running: "Running"
        case .stopping: "Stopping"
        case .failed: "Failed"
        default: "Unavailable"
        }
    }

    nonisolated func map(_ event: Agentvmm_Host_V1_RuntimeEvent) -> ActivityEvent {
        ActivityEvent(id: event.sequence, type: event.type, environmentID: event.resource.environmentID.nilIfEmpty, resourceID: event.resource.resourceID.nilIfEmpty, bootID: event.guestBootID, timestamp: event.timestamp.date)
    }

    nonisolated func map(_ record: Agentvmm_Host_V1_LogRecord) -> LogEntry {
        LogEntry(id: record.cursor, timestamp: record.timestamp.date, stream: record.stream == .stderr ? "stderr" : "stdout", text: String(decoding: record.data, as: UTF8.self), truncated: record.truncated, droppedRecords: record.droppedRecords)
    }

    nonisolated func operation(_ value: Agentvmm_Host_V1_Operation) -> OperationModel {
        let state: String = switch value.state {
        case .running: "Running"
        case .succeeded: "Succeeded"
        case .failed: "Failed"
        case .interrupted: "Interrupted"
        default: "Unavailable"
        }
        let resource = [value.resource.kind, value.resource.environmentID, value.resource.resourceID]
            .filter { !$0.isEmpty }.joined(separator: "/").nilIfEmpty
        return OperationModel(
            id: value.id, kind: value.kind, state: state, stage: value.stage,
            reason: value.error.reason.nilIfEmpty, message: value.error.message.nilIfEmpty, resource: resource
        )
    }

    nonisolated func deviceIdentity(_ value: Agentvmm_Host_V1_DeviceIdentityResponse) -> DeviceIdentityModel {
        let state: DeviceIdentityModel.State = switch value.state {
        case .ready: .ready
        case .needsRepair: .needsRepair
        default: .unavailable
        }
        return DeviceIdentityModel(id: value.identity.deviceID, rootKeyRevision: value.identity.rootKeyRevision, state: state)
    }

    nonisolated func personalMesh(_ value: Agentvmm_Host_V1_PersonalMeshResponse) -> PersonalMeshModel {
        PersonalMeshModel(
            id: value.meshDescriptor.meshID,
            registryAudience: value.meshDescriptor.registryAudience,
            revision: value.snapshot.revision,
            policyEpoch: value.snapshot.policyEpoch,
            members: value.snapshot.activeMembers.map { member in
                PersonalMeshMemberModel(
                    deviceID: member.deviceID,
                    rootKeyRevision: member.rootKeyRevision,
                    permissions: member.permissions.compactMap { permission in
                        switch permission {
                        case .useServices: "Use services"
                        case .manageMembers: "Manage members"
                        default: nil
                        }
                    }
                )
            }
        )
    }

    nonisolated func pairing(_ value: Agentvmm_Host_V1_PersonalMeshPairingResponse) throws -> PersonalMeshPairingModel {
        guard value.invitation.sessionID.count == 16 else { throw HostClientError.invalidResponse("pairing session") }
        return PersonalMeshPairingModel(
            sessionID: value.invitation.sessionID,
            invitationCode: try value.invitation.serializedData().base64EncodedString(),
            role: value.role,
            state: value.state,
            attemptsRemaining: value.attemptsRemaining,
            sas: value.sas,
            registryCommitPending: value.hasPendingOperation
        )
    }
}

private extension String {
    var nilIfEmpty: String? { isEmpty ? nil : self }
}
