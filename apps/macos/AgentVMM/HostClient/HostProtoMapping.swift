import Foundation
import SwiftProtobuf

extension Agentvmm_Host_V1_GetSystemSnapshotResponse {
    var domainModel: SystemSnapshotModel {
        SystemSnapshotModel(
            vmState: (self.vm.state).domainModel,
            bootID: self.vm.bootID,
            health: (self.health.state).domainModel,
            components: self.health.components.map {
                ComponentStatus(id: $0.component, level: ($0.state).domainModel, reason: $0.reason.isEmpty ? nil : $0.reason, observedAt: $0.observedAt.date)
            },
            usage: (self.usage).domainModel,
            environments: self.environments.map(\.domainModel),
            remoteRegistrations: self.remoteRegistrations.map(\.domainModel),
            collectedAt: self.collectedAt.date
        )
    }
}

extension Agentvmm_Host_V1_AgentEnvironment {
    var domainModel: AgentEnvironmentModel {
        let inventoryAvailability: InventoryAvailability =
            switch self.inventoryState {
            case .available: .available
            case .vmStopped: .vmStopped
            case .unavailable: .unavailable
            case .stale: .stale
            default: .unknown
            }
        return AgentEnvironmentModel(
            id: self.id,
            name: self.displayName,
            namespaceID: self.namespaceID,
            health: (self.health.state).domainModel,
            usage: (self.usage).domainModel,
            workloads: self.containers.map {
                let state: Workload.State =
                    switch $0.state {
                    case .created: .created
                    case .running: .running
                    case .stopped: .stopped
                    default: .unknown
                    }
                return Workload(
                    id: $0.id, generationID: $0.generationID, image: $0.image, state: state, exitCode: $0.hasExitCode ? $0.exitCode : nil,
                    argv: $0.argv, environmentKeys: $0.environmentKeys, workdir: $0.hasWorkdir ? $0.workdir : nil,
                    user: $0.hasUser ? $0.user : nil, mounts: $0.volumes.map { "\($0.hostShare.isEmpty ? $0.ownedVolume : $0.hostShare) → \($0.destination)" },
                    instanceID: $0.instanceID.nilIfEmpty,
                    createdAt: ($0.createdAt).optionalDate, startedAt: ($0.startedAt).optionalDate, finishedAt: ($0.finishedAt).optionalDate
                )
            },
            volumes: self.volumes.map {
                RetainedVolume(
                    id: $0.id, bytesUsed: $0.usage.diskBytes, byteLimit: $0.limitBytes, mountedBy: $0.mountedBy,
                    usageObservedAt: ($0.usage.sampledAt).optionalDate, usageIsStale: $0.usage.stale,
                    usageUnavailableReason: $0.usage.unavailableReason.nilIfEmpty
                )
            },
            limits: EnvironmentLimits(
                pids: self.limits.pids, diskBytes: self.limits.diskBytes),
            egressSummary: self.egressMode == .publicInternet ? "Public internet" : "Blocked by default",
            allowsPublicEgress: self.egressMode == .publicInternet,
            policyRevision: self.policyRevision,
            origin: self.owner.kind == .remoteRegistration ? .remote : .local,
            allocationID: self.owner.allocationID.nilIfEmpty,
            inventoryAvailability: inventoryAvailability,
            inventoryMessage: self.inventoryMessage.nilIfEmpty,
            inventoryObservedAt: (self.inventoryObservedAt).optionalDate
        )
    }
}

extension Agentvmm_Host_V1_RemoteRegistration {
    var domainModel: RemoteRegistrationModel {
        let admissionState: RemoteAdmissionState =
            switch self.state {
            case .enabled: .accepting
            case .draining: .draining
            case .revoked: .revoked
            default: .unknown
            }
        let connector = (self.connectorStatus).domainModel
        let connectionState = RemoteConnectionStatus.derive(
            connected: self.connectionState == .connected,
            connectorPhase: connector.phase,
            observedAt: connector.observedAt
        )
        let enrollmentState: RemoteEnrollmentStatus =
            switch self.enrollmentState {
            case .pendingCredential: .pending
            case .ready: .ready
            case .setupIncomplete: .incomplete
            case .revoking: .revoking
            default: .incomplete
            }
        let owner: RemoteRegistrationModel.Owner =
            switch self.ownerKind {
            case .external: .external
            case .appliance: .appliance(installationID: self.installationID)
            default: .unknown
            }
        return RemoteRegistrationModel(
            id: self.id,
            displayName: self.displayName,
            endpoint: self.endpoint,
            state: admissionState,
            connectionState: connectionState,
            enrollmentState: enrollmentState,
            revision: self.revision,
            inventoryRevision: self.inventoryRevision,
            owner: owner,
            allocations: self.allocations.map(\.domainModel),
            connector: connector,
            enrollmentError: self.enrollmentErrorMessage.nilIfEmpty
        )
    }
}

extension Agentvmm_Host_V1_ResourceLimits {
    var domainModel: EnvironmentLimits {
        EnvironmentLimits(pids: self.pids, diskBytes: self.diskBytes)
    }
}

extension Agentvmm_Host_V1_RemoteAllocation {
    var domainModel: RemoteAllocationModel {
        return RemoteAllocationModel(
            allocationID: self.allocationID,
            environmentID: self.environmentID,
            displayName: self.displayLabel,
            state: self.state.domainModel,
            limits: (self.limits).domainModel,
            allowsPublicEgress: self.egressMode == .publicInternet
        )
    }
}

extension Agentvmm_Host_V1_ConnectorStatus {
    var domainModel: ConnectorStatusModel {
        let phase: ConnectorStatusModel.Phase =
            switch self.phase {
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
            release: self.connectorRelease,
            observedAt: (self.observedAt).optionalDate,
            lastConnectedAt: (self.lastConnectedAt).optionalDate,
            disconnectedAt: (self.disconnectedAt).optionalDate,
            nextRetryAt: (self.nextRetryAt).optionalDate,
            reconnectAttempt: self.reconnectAttempt,
            failureCode: self.failureCode.nilIfEmpty,
            failureMessage: self.failureMessage.nilIfEmpty
        )
    }
}

extension Agentvmm_Appliance_V1_ApplianceStatus {
    var domainModel: ApplianceStatusModel {
        let grant = self.approvedGrant
        return ApplianceStatusModel(
            id: self.installationID,
            productID: self.productID,
            revision: self.revision,
            artifactID: self.artifactID,
            state: self.applianceState.domainModel,
            channelState: String(describing: self.channelState).replacingOccurrences(of: "_", with: " ").capitalized,
            enabled: self.enabled,
            healthy: self.health.healthy,
            healthMessage: self.health.status.nilIfEmpty,
            grant: ApplianceGrantModel(
                perEnvironmentPIDs: grant.capacity.perEnvironmentLimits.pids,
                perEnvironmentDiskBytes: grant.capacity.perEnvironmentLimits.diskBytes,
                allowsPublicEgress: grant.capacity.maxEgressMode == .publicInternet,
                enabledFrontendNames: Set(grant.frontends.map(\.name))
            ),
            frontends: self.frontends.map {
                ApplianceFrontendModel(
                    name: $0.name,
                    transport: String(describing: $0.transport).replacingOccurrences(of: "_", with: " ").capitalized,
                    endpoint: $0.endpoint,
                    exposure: $0.exposure.domainModel,
                    available: $0.available,
                    errorReason: $0.errorReason
                )
            }
        )
    }
}

extension Google_Protobuf_Timestamp {
    var optionalDate: Date? {
        self.seconds == 0 && self.nanos == 0 ? nil : self.date
    }
}

extension Agentvmm_Host_V1_Usage {
    var domainModel: UsageSnapshot? {
        guard self.sampledAt.seconds != 0 else { return nil }
        return UsageSnapshot(
            cpuNanos: self.cpuNanos,
            memoryBytes: self.memoryBytes,
            pids: self.pids,
            readBytes: self.readBytes,
            writeBytes: self.writeBytes,
            networkRXBytes: self.networkRxBytes,
            networkTXBytes: self.networkTxBytes,
            diskBytes: self.diskBytes,
            sampledAt: self.sampledAt.date,
            isStale: self.stale,
            unavailableReason: self.unavailableReason.nilIfEmpty
        )
    }
}

extension Agentvmm_Host_V1_HealthState {
    var domainModel: HealthLevel {
        switch self {
        case .healthy: .healthy
        case .degraded: .degraded
        default: .unavailable
        }
    }
}

extension Agentvmm_Host_V1_VMState {
    var domainModel: VMState {
        switch self {
        case .stopped: .stopped
        case .starting: .starting
        case .running: .running
        case .stopping: .stopping
        case .failed: .failed
        default: .unavailable
        }
    }
}

extension Agentvmm_Host_V1_WatchEventsResponse {
    var domainModel: ActivityEvent {
        ActivityEvent(
            id: self.sequence, type: self.type, environmentID: self.resource.environmentID.nilIfEmpty, resourceID: self.resource.resourceID.nilIfEmpty,
            bootID: self.guestBootID, timestamp: self.timestamp.date)
    }
}

extension Agentvmm_Host_V1_OperatorServiceLogsResponse {
    var domainModel: LogEntry {
        LogEntry(
            id: self.cursor, timestamp: self.timestamp.date, stream: self.stream == .stderr ? "stderr" : "stdout",
            text: String(decoding: self.data, as: UTF8.self), truncated: self.truncated, droppedRecords: self.droppedRecords)
    }
}

extension Agentvmm_Host_V1_Operation {
    var domainModel: OperationModel {
        let state: OperationState =
            switch self.state {
            case .running: .running
            case .succeeded: .succeeded
            case .failed: .failed
            case .interrupted: .interrupted
            default: .unavailable
            }
        let resource = [self.resource.kind, self.resource.environmentID, self.resource.resourceID]
            .filter { !$0.isEmpty }.joined(separator: "/").nilIfEmpty
        return OperationModel(
            id: self.id, kind: self.kind, state: state, stage: self.stage,
            reason: self.error.reason.nilIfEmpty, message: self.error.message.nilIfEmpty, resource: resource
        )
    }
}

protocol DeviceIdentityResponse {
    var identity: Agentvmm_Trust_V1_DeviceIdentity { get }
    var state: Agentvmm_Host_V1_DeviceIdentityState { get }
}

extension Agentvmm_Host_V1_GetDeviceIdentityResponse: DeviceIdentityResponse {}
extension Agentvmm_Host_V1_RepairDeviceIdentityResponse: DeviceIdentityResponse {}

extension DeviceIdentityResponse {
    var domainModel: DeviceIdentityModel {
        let state: DeviceIdentityModel.State =
            switch self.state {
            case .ready: .ready
            case .needsRepair: .needsRepair
            default: .unavailable
            }
        return DeviceIdentityModel(id: self.identity.deviceID, rootKeyRevision: self.identity.rootKeyRevision, state: state)
    }
}

extension Agentvmm_Host_V1_PersonalMeshResponse {
    var domainModel: PersonalMeshModel {
        PersonalMeshModel(
            id: self.meshDescriptor.meshID,
            registryAudience: self.meshDescriptor.registryAudience,
            revision: self.snapshot.revision,
            policyEpoch: self.snapshot.policyEpoch,
            members: self.snapshot.activeMembers.map { member in
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
}

protocol PersonalMeshPairingResponse {
    var invitation: Agentvmm_Host_V1_PersonalMeshPairingInvitation { get }
    var role: Agentvmm_Trust_V1_PairingRole { get }
    var state: Agentvmm_Host_V1_PersonalMeshPairingState { get }
    var attemptsRemaining: UInt32 { get }
    var lastFailure: Agentvmm_Host_V1_PairingFailure { get }
    var sas: String { get }
    var hasPendingOperation: Bool { get }
}

extension Agentvmm_Host_V1_BeginPersonalMeshPairingResponse: PersonalMeshPairingResponse {}
extension Agentvmm_Host_V1_JoinPersonalMeshPairingResponse: PersonalMeshPairingResponse {}
extension Agentvmm_Host_V1_GetPersonalMeshPairingResponse: PersonalMeshPairingResponse {}
extension Agentvmm_Host_V1_ConfirmPersonalMeshPairingResponse: PersonalMeshPairingResponse {}
extension Agentvmm_Host_V1_RejectPersonalMeshPairingResponse: PersonalMeshPairingResponse {}

extension PersonalMeshPairingResponse {
    var domainModel: PersonalMeshPairingModel {
        get throws {
            guard self.invitation.sessionID.count == 16 else { throw HostClientError.invalidResponse("pairing session") }
            return PersonalMeshPairingModel(
                sessionID: self.invitation.sessionID,
                invitationCode: try self.invitation.serializedData().base64EncodedString(),
                role: self.role == .manager ? "manager" : "joiner",
                state: self.state.domainModel,
                attemptsRemaining: self.attemptsRemaining,
                lastFailure: self.lastFailure.pairingLabel,
                sas: self.sas,
                registryCommitPending: self.hasPendingOperation
            )

        }
    }
}

extension String {
    fileprivate var nilIfEmpty: String? { isEmpty ? nil : self }
}

extension Agentvmm_Host_V1_AllocationState {
    var domainModel: AllocationState {
        switch self {
        case .ready: .ready
        case .draining: .draining
        case .retained: .retained
        case .discarded: .discarded
        default: .unavailable
        }
    }
}

extension Agentvmm_Appliance_V1_ApplianceState {
    var domainModel: ApplianceState {
        switch self {
        case .stopped: .stopped
        case .starting: .starting
        case .running: .running
        case .stopping: .stopping
        case .failed: .failed
        case .sealed: .sealed
        default: .unavailable
        }
    }
}

extension Agentvmm_Appliance_V1_FrontendExposure {
    var domainModel: ApplianceFrontendExposure {
        switch self {
        case .hostLoopback: .loopback
        case .lanHTTP: .lan
        case .outboundOnly: .outbound
        default: .unknown
        }
    }
}

extension Agentvmm_Host_V1_PersonalMeshPairingState {
    var domainModel: PairingState {
        switch self {
        case .invited: .invited
        case .handshaking: .handshaking
        case .awaitingConfirm: .awaitingConfirm
        case .readyToCommit: .readyToCommit
        case .committed: .committed
        case .rejected: .rejected
        case .expired: .expired
        default: .unavailable
        }
    }
}

extension Agentvmm_Host_V1_PairingFailure {
    var pairingLabel: String? {
        switch self {
        case .contextMismatch: "Invitation details do not match"
        case .timeout: "Pairing timed out"
        case .authenticationFailed: "Pairing authentication failed"
        case .protocolViolation: "The peer sent an invalid pairing message"
        case .connectionLost: "The pairing connection closed"
        default: nil
        }
    }
}
