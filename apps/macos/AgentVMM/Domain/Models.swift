import Foundation

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
    let image: String
    let state: State
    let exitCode: UInt32?
    let argv: [String]
    let environmentKeys: [String]
    let workdir: String?
    let user: String?
    let mounts: [String]
}

struct RetainedVolume: Identifiable, Sendable, Equatable {
    let id: String
    let bytesUsed: UInt64
    let byteLimit: UInt64
    let mountedBy: [String]
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
    let state: String
    let connectionState: String
    let revision: UInt64
    let maxEnvironments: UInt32
    let activeLeases: Int
}

struct RemotePoolSettings: Sendable, Equatable {
    let maxEnvironments: UInt32
    let cpuMillis: UInt32
    let memoryBytes: UInt64
    let pids: UInt32
    let diskBytes: UInt64
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
