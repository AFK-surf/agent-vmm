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
    let remoteRegistrations: [RemoteRegistrationModel]
    let collectedAt: Date

    static let loading = SystemSnapshotModel(vmState: "Loading", bootID: "", health: .unavailable, components: [], usage: nil, environments: [], remoteRegistrations: [], collectedAt: .distantPast)
}

struct RemoteRegistrationModel: Sendable, Equatable {
    enum Owner: Sendable, Equatable { case external, appliance(installationID: String), unknown }
    let id: String
    let displayName: String
    let endpoint: String
    let state: RemoteAdmissionState
    let connectionState: RemoteConnectionStatus
    let enrollmentState: RemoteEnrollmentStatus
    let revision: UInt64
    let inventoryRevision: UInt64
    let owner: Owner
    let pool: RemotePoolSettings
    let allocations: [RemoteAllocationModel]
    let connector: ConnectorStatusModel
    let enrollmentError: String?

}

struct RemotePoolSettings: Sendable, Equatable {
    let perEnvironmentPIDs: UInt32
    let perEnvironmentDiskBytes: UInt64
    let allowsPublicEgress: Bool
    let stopGraceSeconds: Int64

    static let defaults = RemotePoolSettings(
        perEnvironmentPIDs: 256,
        perEnvironmentDiskBytes: 2 << 30,
        allowsPublicEgress: false,
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
        now: Date = .now
    ) -> Self {
        if connected {
            if let observedAt, now.timeIntervalSince(observedAt) <= 20 { return .connected }
            return .offline
        }
        if connectorPhase == .authFailed || connectorPhase == .configError { return .offline }
        if connectorPhase == .connecting || connectorPhase == .retryWait,
           let observedAt, now.timeIntervalSince(observedAt) <= 20 {
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
}

struct ApplianceFrontendModel: Identifiable, Sendable, Equatable {
    var id: String { name }
    let name: String
    let transport: String
    let endpoint: String
    let available: Bool
}

struct ApplianceFrontendGrantModel: Identifiable, Sendable, Equatable {
    var id: String { name }
    let name: String
    let transport: String
    let exposure: String
}

struct ApplianceStatusModel: Identifiable, Sendable, Equatable {
    let id: String
    let productID: String
    let revision: UInt64
    let artifactID: String
    let state: String
    let channelState: String
    let enabled: Bool
    let healthy: Bool
    let healthMessage: String?
    let grant: ApplianceGrantModel
    let frontends: [ApplianceFrontendModel]

    var endpointAvailable: Bool {
        enabled && frontends.contains { frontend in
            frontend.name == BundledMCPAppliance.frontendName && frontend.available && !frontend.endpoint.isEmpty
        }
    }

    var endpointStatus: String {
        if !enabled { return "Closed while MCP is disabled" }
        if endpointAvailable { return "Available on this Mac" }
        return "Starting or unavailable"
    }
}

enum ApplianceAction: Sendable, Equatable {
    case installing
    case planningRemoval
    case removing
    case enabling
    case disabling
    case restarting

    var title: String {
        switch self {
        case .installing: "Setting up MCP…"
        case .planningRemoval: "Preparing removal…"
        case .removing: "Removing MCP appliance…"
        case .enabling: "Enabling MCP…"
        case .disabling: "Disabling MCP…"
        case .restarting: "Restarting MCP…"
        }
    }
}

struct ApplianceGrantModel: Sendable, Equatable {
    var perEnvironmentPIDs: UInt32
    var perEnvironmentDiskBytes: UInt64
    var allowsPublicEgress: Bool
    var enabledFrontendNames: Set<String>

    static let bundledMCPDefault = ApplianceGrantModel(
        perEnvironmentPIDs: 512,
        perEnvironmentDiskBytes: 4 << 30,
        allowsPublicEgress: true,
		enabledFrontendNames: [BundledMCPAppliance.frontendName]
    )
}

struct ApplianceImpactModel: Identifiable, Sendable, Equatable {
    var id: String { "\(resourceType):\(resourceID)" }
    let resourceType: String
    let resourceID: String
    let effect: String
    let irreversible: Bool
    let blocker: Bool
}

struct ApplianceRemovePlanModel: Sendable, Equatable {
    let revision: UInt64
    let impacts: [ApplianceImpactModel]
    var blockers: [ApplianceImpactModel] { impacts.filter(\.blocker) }
    var requiredConfirmations: [String] { impacts.filter(\.irreversible).map(\.resourceID) }
}

struct BundledApplianceArtifact: Sendable, Equatable {
    let manifest: Data
    let requestedFrontends: [ApplianceFrontendGrantModel]
    let requestsDynamicHostLoopback: Bool
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
