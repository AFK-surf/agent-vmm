import Foundation

struct DeviceIdentityModel: Sendable, Equatable {
    enum State: Sendable, Equatable { case ready, needsRepair, unavailable }
    let id: String
    let rootKeyRevision: UInt64
    let state: State
}

struct PersonalMeshMemberModel: Identifiable, Sendable, Equatable {
    var id: String { deviceID }
    let deviceID: String
    let rootKeyRevision: UInt64
    let permissions: [String]
}

struct PersonalMeshModel: Identifiable, Sendable, Equatable {
    let id: String
    let registryAudience: String
    let revision: UInt64
    let policyEpoch: UInt64
    let members: [PersonalMeshMemberModel]
}

struct PersonalMeshPairingModel: Sendable, Equatable {
    let sessionID: Data
    let invitationCode: String
    let role: String
    let state: String
    let attemptsRemaining: UInt32
    let sas: String
    let registryCommitPending: Bool
}

enum HealthLevel: Int, Sendable, Comparable {
    case unavailable = 0
    case degraded = 1
    case healthy = 2

    static func < (lhs: HealthLevel, rhs: HealthLevel) -> Bool { lhs.rawValue < rhs.rawValue }
}

struct ComponentStatus: Identifiable, Sendable, Equatable {
    let id: String
    let level: HealthLevel
    let reason: String?
    let observedAt: Date
}

struct UsageSnapshot: Sendable, Equatable {
    let cpuNanos: UInt64
    let memoryBytes: UInt64
    let pids: UInt32
    let readBytes: UInt64
    let writeBytes: UInt64
    let networkRXBytes: UInt64
    let networkTXBytes: UInt64
    let diskBytes: UInt64
    let sampledAt: Date
    let isStale: Bool
    let unavailableReason: String?

    init(cpuNanos: UInt64, memoryBytes: UInt64, pids: UInt32, readBytes: UInt64, writeBytes: UInt64, networkRXBytes: UInt64, networkTXBytes: UInt64, diskBytes: UInt64, sampledAt: Date, isStale: Bool, unavailableReason: String? = nil) {
        self.cpuNanos = cpuNanos
        self.memoryBytes = memoryBytes
        self.pids = pids
        self.readBytes = readBytes
        self.writeBytes = writeBytes
        self.networkRXBytes = networkRXBytes
        self.networkTXBytes = networkTXBytes
        self.diskBytes = diskBytes
        self.sampledAt = sampledAt
        self.isStale = isStale
        self.unavailableReason = unavailableReason
    }

    func rate(after previous: UsageSnapshot, for counter: KeyPath<UsageSnapshot, UInt64>) -> Double? {
        guard !previous.isStale, !isStale else { return nil }
        let elapsed = sampledAt.timeIntervalSince(previous.sampledAt)
        let before = previous[keyPath: counter]
        let after = self[keyPath: counter]
        guard elapsed > 0, after >= before else { return nil }
        return Double(after - before) / elapsed
    }
}

struct Workload: Identifiable, Sendable, Equatable {
    enum State: String, Sendable { case created, running, stopped, unknown }
    let id: String
    let generationID: String
    let image: String
    let state: State
    let exitCode: UInt32?
    let argv: [String]
    let environmentKeys: [String]
    let workdir: String?
    let user: String?
    let mounts: [String]
    let instanceID: String?
    let createdAt: Date?
    let startedAt: Date?
    let finishedAt: Date?

    init(id: String, generationID: String = "", image: String, state: State, exitCode: UInt32?, argv: [String], environmentKeys: [String], workdir: String?, user: String?, mounts: [String], instanceID: String? = nil, createdAt: Date? = nil, startedAt: Date? = nil, finishedAt: Date? = nil) {
        self.id = id
        self.generationID = generationID
        self.image = image
        self.state = state
        self.exitCode = exitCode
        self.argv = argv
        self.environmentKeys = environmentKeys
        self.workdir = workdir
        self.user = user
        self.mounts = mounts
        self.instanceID = instanceID
        self.createdAt = createdAt
        self.startedAt = startedAt
        self.finishedAt = finishedAt
    }
}

struct EnvironmentImageModel: Identifiable, Sendable, Equatable {
    var id: String { reference }
    let reference: String
    let digest: String
    let logicalBytes: UInt64
    let platforms: [String]
    let usedByContainerIDs: [String]
    let generationID: String
}

enum ImageRemovalAvailability: Equatable {
    case allowed
    case inUse(Int)
    case inventoryUnavailable
    case inProgress
}

func imageRemovalAvailability(for image: EnvironmentImageModel, inventory: InventoryAvailability, isRemoving: Bool) -> ImageRemovalAvailability {
    if isRemoving { return .inProgress }
    if inventory != .available { return .inventoryUnavailable }
    if !image.usedByContainerIDs.isEmpty { return .inUse(image.usedByContainerIDs.count) }
    return .allowed
}

enum InventoryAvailability: Sendable, Equatable {
    case available
    case vmStopped
    case unavailable
    case stale
    case unknown
}

struct RetainedVolume: Identifiable, Sendable, Equatable {
    let id: String
    let bytesUsed: UInt64
    let byteLimit: UInt64
    let mountedBy: [String]
    let usageObservedAt: Date?
    let usageIsStale: Bool
    let usageUnavailableReason: String?
}

struct AgentEnvironmentModel: Identifiable, Sendable, Equatable {
    enum Origin: String, Sendable { case local = "Local", remote = "Remote" }

    let id: String
    let name: String
    let namespaceID: String
    let health: HealthLevel
    let usage: UsageSnapshot?
    let workloads: [Workload]
    let volumes: [RetainedVolume]
    let limits: EnvironmentLimits
    let egressSummary: String
    let allowsPublicEgress: Bool
    let policyRevision: UInt64
    let origin: Origin
    let allocationID: String?
    let inventoryAvailability: InventoryAvailability
    let inventoryMessage: String?
    let inventoryObservedAt: Date?

    init(id: String, name: String, namespaceID: String, health: HealthLevel, usage: UsageSnapshot?, workloads: [Workload], volumes: [RetainedVolume], limits: EnvironmentLimits, egressSummary: String, allowsPublicEgress: Bool, policyRevision: UInt64, origin: Origin, allocationID: String?, inventoryAvailability: InventoryAvailability = .unknown, inventoryMessage: String? = nil, inventoryObservedAt: Date? = nil) {
        self.id = id
        self.name = name
        self.namespaceID = namespaceID
        self.health = health
        self.usage = usage
        self.workloads = workloads
        self.volumes = volumes
        self.limits = limits
        self.egressSummary = egressSummary
        self.allowsPublicEgress = allowsPublicEgress
        self.policyRevision = policyRevision
        self.origin = origin
        self.allocationID = allocationID
        self.inventoryAvailability = inventoryAvailability
        self.inventoryMessage = inventoryMessage
        self.inventoryObservedAt = inventoryObservedAt
    }

    var hasNoRetainedData: Bool {
        workloads.isEmpty && volumes.isEmpty && usage?.diskBytes == 0
    }
}

struct EnvironmentLimits: Sendable, Equatable {
    let cpuMillis: UInt32
    let memoryBytes: UInt64
    let pids: UInt32
    let diskBytes: UInt64
}

struct SystemSnapshotModel: Sendable, Equatable {
    let vmState: String
    let bootID: String
    let health: HealthLevel
    let components: [ComponentStatus]
    let usage: UsageSnapshot?
    let environments: [AgentEnvironmentModel]
    let remoteRegistration: RemoteRegistrationModel?
    let collectedAt: Date

    static let loading = SystemSnapshotModel(vmState: "Loading", bootID: "", health: .unavailable, components: [], usage: nil, environments: [], remoteRegistration: nil, collectedAt: .distantPast)
}

struct RemoteRegistrationModel: Sendable, Equatable {
    let id: String
    let displayName: String
    let endpoint: String
    let state: RemoteAdmissionState
    let connectionState: RemoteConnectionStatus
    let enrollmentState: RemoteEnrollmentStatus
    let revision: UInt64
    let pool: RemotePoolSettings
    let allocations: [RemoteAllocationModel]
    let connector: ConnectorStatusModel
    let enrollmentError: String?

    var activeLeases: Int { allocations.filter { $0.lease?.state == .active }.count }
}

struct RemotePoolSettings: Sendable, Equatable {
    let maxEnvironments: UInt32
    let aggregateLimits: EnvironmentLimits
    let perEnvironmentLimits: EnvironmentLimits
    let allowsPublicEgress: Bool
    let maxLeaseSeconds: Int64
    let disconnectGraceSeconds: Int64
    let stopGraceSeconds: Int64

    static let defaults = RemotePoolSettings(
        maxEnvironments: 2,
        aggregateLimits: EnvironmentLimits(cpuMillis: 2_000, memoryBytes: 2 << 30, pids: 512, diskBytes: 4 << 30),
        perEnvironmentLimits: EnvironmentLimits(cpuMillis: 1_000, memoryBytes: 1 << 30, pids: 256, diskBytes: 2 << 30),
        allowsPublicEgress: true,
        maxLeaseSeconds: 3_600,
        disconnectGraceSeconds: 30,
        stopGraceSeconds: 10
    )
}

enum RemoteAdmissionState: String, Sendable { case accepting = "Accepting new work", draining = "Draining", revoked = "Revoked", unknown = "Unavailable" }
enum RemoteConnectionStatus: String, Sendable {
    case connected = "Connected", connecting = "Connecting", offline = "Offline"

    static func derive(
        connected: Bool,
        connectorPhase: ConnectorStatusModel.Phase,
        observedAt: Date?,
        disconnectedAt: Date?,
        disconnectGraceSeconds: Int64,
        now: Date = .now
    ) -> Self {
        if connected {
            if let observedAt, now.timeIntervalSince(observedAt) <= 20 { return .connected }
            return .offline
        }
        if connectorPhase == .authFailed || connectorPhase == .configError { return .offline }
        let grace = TimeInterval(max(0, disconnectGraceSeconds))
        if let disconnectedAt, now.timeIntervalSince(disconnectedAt) <= grace { return .connecting }
        if (connectorPhase == .connecting || connectorPhase == .retryWait),
           let observedAt, now.timeIntervalSince(observedAt) <= grace {
            return .connecting
        }
        return .offline
    }
}
enum RemoteEnrollmentStatus: String, Sendable { case pending = "Setting up", ready = "Ready", incomplete = "Setup incomplete", revoking = "Revoking" }

struct ConnectorStatusModel: Sendable, Equatable {
    enum Phase: String, Sendable { case stopped = "Stopped", enrolling = "Enrolling", connecting = "Connecting", connected = "Connected", retryWait = "Retrying", authFailed = "Authentication failed", configError = "Configuration error", unknown = "Unavailable" }
    let phase: Phase
    let release: String
    let observedAt: Date?
    let lastConnectedAt: Date?
    let disconnectedAt: Date?
    let nextRetryAt: Date?
    let reconnectAttempt: UInt32
    let failureCode: String?
    let failureMessage: String?
}

struct RemoteAllocationModel: Identifiable, Sendable, Equatable {
    var id: String { allocationID }
    let allocationID: String
    let environmentID: String
    let displayName: String
    let state: String
    let limits: EnvironmentLimits
    let allowsPublicEgress: Bool
    let lease: RemoteLeaseModel?
}

struct RemoteLeaseModel: Sendable, Equatable {
    enum State: String, Sendable { case active = "Active", expired = "Expired", released = "Released", revoked = "Revoked", unknown = "Unavailable" }
    let id: String
    let workloadReference: String
    let expiresAt: Date?
    let state: State
}

struct AgentIntegrationModel: Identifiable, Sendable, Equatable {
    enum ConfigurationRemoval: Equatable { case none, verified(String), requiresRepair }
    enum Scope: String, Sendable { case user = "User", workspace = "Workspace", manual = "Manual" }
    enum ConfigState: String, Sendable { case pending = "Adding", configuring = "Writing configuration", ready = "Ready", needsRestart = "Needs restart", conflict = "Config changed externally", error = "Needs repair", removing = "Removing", removed = "Removed", unknown = "Unavailable" }
    let id: String
    let adapterID: String
    let displayName: String
    let scope: Scope
    let configTarget: String
    let environmentID: String
    let configState: ConfigState
    let fingerprint: String
    let revision: UInt64
    let errorMessage: String?
    let createdAt: Date
    let lastVerifiedAt: Date?

    var configurationRemoval: ConfigurationRemoval {
        guard adapterID == "codex", configState != .pending else { return .none }
        return fingerprint.isEmpty ? .requiresRepair : .verified(fingerprint)
    }
    var canRepairConfiguration: Bool {
        adapterID == "codex" && (configurationRemoval == .requiresRepair || configState == .conflict || configState == .error || configState == .configuring)
    }
    var canRetryRemoval: Bool { configState == .removing && configurationRemoval != .requiresRepair }
}

struct AgentSessionModel: Identifiable, Sendable, Equatable {
    enum State: String, Sendable { case listening = "Ready", connected = "Connected", revoked = "Revoked", expired = "Expired", closed = "Closed", unknown = "Unavailable" }
    let id: String
    let integrationID: String?
    let environmentID: String
    let state: State
    let createdAt: Date
    let expiresAt: Date
    let lastActivityAt: Date?
    let activeConnections: UInt32
    let terminalReason: String?
}

struct ActivityEvent: Identifiable, Sendable, Equatable {
    let id: UInt64
    let type: String
    let environmentID: String?
    let resourceID: String?
    let bootID: String
    let timestamp: Date
}

struct LogEntry: Identifiable, Sendable, Equatable {
    let id: String
    let timestamp: Date
    let stream: String
    let text: String
    let truncated: Bool
    let droppedRecords: UInt64
}

enum CleanupMode: Sendable, Equatable { case reset, discard }

struct CleanupPreview: Sendable, Equatable {
    let mode: CleanupMode
    let environmentID: String
    let revision: UInt64
    let impacts: [String]
    let volumeIDs: [String]
}

struct OperationModel: Identifiable, Sendable, Equatable {
    let id: String
    let kind: String
    let state: String
    let stage: String
    let reason: String?
    let message: String?
    let resource: String?

    var nextStep: String {
        switch state {
        case "Running": "Keep this window open or return later; progress can be restored."
        case "Interrupted": "Review the current resource state, then retry with a new request."
        case "Failed": "Resolve the reported resource condition, refresh, and retry."
        default: "No action is required."
        }
    }
}

struct SupportItem: Identifiable, Sendable, Equatable {
    var id: String { name }
    let name: String
    let description: String
    let sensitive: Bool
    let includedByDefault: Bool
}
