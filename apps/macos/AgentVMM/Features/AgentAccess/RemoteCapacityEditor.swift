import SwiftUI

struct RemotePoolDraft {
    var maxEnvironments: Int
    var aggregateCPU: Int
    var aggregateMemoryBytes: UInt64
    var aggregatePIDs: Int
    var aggregateDiskBytes: UInt64
    var perEnvironmentCPU: Int
    var perEnvironmentMemoryBytes: UInt64
    var perEnvironmentPIDs: Int
    var perEnvironmentDiskBytes: UInt64
    var allowsPublicEgress: Bool
    var maxLeaseMinutes: Int
    var disconnectGraceSeconds: Int
    var stopGraceSeconds: Int

    init(_ settings: RemotePoolSettings = .defaults) {
        maxEnvironments = Int(settings.maxEnvironments)
        aggregateCPU = Int(settings.aggregateLimits.cpuMillis)
        aggregateMemoryBytes = settings.aggregateLimits.memoryBytes
        aggregatePIDs = Int(settings.aggregateLimits.pids)
        aggregateDiskBytes = settings.aggregateLimits.diskBytes
        perEnvironmentCPU = Int(settings.perEnvironmentLimits.cpuMillis)
        perEnvironmentMemoryBytes = settings.perEnvironmentLimits.memoryBytes
        perEnvironmentPIDs = Int(settings.perEnvironmentLimits.pids)
        perEnvironmentDiskBytes = settings.perEnvironmentLimits.diskBytes
        allowsPublicEgress = settings.allowsPublicEgress
        maxLeaseMinutes = Int(settings.maxLeaseSeconds / 60)
        disconnectGraceSeconds = Int(settings.disconnectGraceSeconds)
        stopGraceSeconds = Int(settings.stopGraceSeconds)
    }

    var isValid: Bool {
        maxEnvironments > 0 && aggregateCPU >= perEnvironmentCPU &&
            aggregateMemoryBytes >= perEnvironmentMemoryBytes && aggregatePIDs >= perEnvironmentPIDs &&
            aggregateDiskBytes >= perEnvironmentDiskBytes && maxLeaseMinutes > 0 &&
            disconnectGraceSeconds > 0 && stopGraceSeconds > 0
    }

    var settings: RemotePoolSettings {
        RemotePoolSettings(
            maxEnvironments: UInt32(maxEnvironments),
            aggregateLimits: EnvironmentLimits(
                cpuMillis: UInt32(aggregateCPU), memoryBytes: aggregateMemoryBytes,
                pids: UInt32(aggregatePIDs), diskBytes: aggregateDiskBytes
            ),
            perEnvironmentLimits: EnvironmentLimits(
                cpuMillis: UInt32(perEnvironmentCPU), memoryBytes: perEnvironmentMemoryBytes,
                pids: UInt32(perEnvironmentPIDs), diskBytes: perEnvironmentDiskBytes
            ),
            allowsPublicEgress: allowsPublicEgress,
            maxLeaseSeconds: Int64(maxLeaseMinutes * 60),
            disconnectGraceSeconds: Int64(disconnectGraceSeconds),
            stopGraceSeconds: Int64(stopGraceSeconds)
        )
    }

    var aggregateMemoryGiB: Int {
        get { Int(aggregateMemoryBytes >> 30) }
        set { aggregateMemoryBytes = UInt64(newValue) << 30 }
    }

    var aggregateDiskGiB: Int {
        get { Int(aggregateDiskBytes >> 30) }
        set { aggregateDiskBytes = UInt64(newValue) << 30 }
    }

    var perEnvironmentMemoryMiB: Int {
        get { Int(perEnvironmentMemoryBytes >> 20) }
        set { perEnvironmentMemoryBytes = UInt64(newValue) << 20 }
    }

    var perEnvironmentDiskGiB: Int {
        get { Int(perEnvironmentDiskBytes >> 30) }
        set { perEnvironmentDiskBytes = UInt64(newValue) << 30 }
    }
}

struct RemoteCapacityEditor: View {
    @Binding var draft: RemotePoolDraft
    var showsAdvanced = true

    var body: some View {
        Stepper("Maximum environments: \(draft.maxEnvironments)", value: $draft.maxEnvironments, in: 1...128)
        Stepper("Per-environment CPU: \(draft.perEnvironmentCPU) millicores", value: $draft.perEnvironmentCPU, in: 100...64_000, step: 100)
        Stepper("Per-environment memory: \(draft.perEnvironmentMemoryMiB) MiB", value: $draft.perEnvironmentMemoryMiB, in: 128...262_144, step: 128)
        Stepper("Per-environment PIDs: \(draft.perEnvironmentPIDs)", value: $draft.perEnvironmentPIDs, in: 16...65_536, step: 16)
        Stepper("Per-environment disk: \(draft.perEnvironmentDiskGiB) GiB", value: $draft.perEnvironmentDiskGiB, in: 1...1_024)

        if showsAdvanced {
            DisclosureGroup("Advanced Capacity") {
                Stepper("Aggregate CPU: \(draft.aggregateCPU) millicores", value: $draft.aggregateCPU, in: 100...64_000, step: 100)
                Stepper("Aggregate memory: \(draft.aggregateMemoryGiB) GiB", value: $draft.aggregateMemoryGiB, in: 1...256)
                Stepper("Aggregate PIDs: \(draft.aggregatePIDs)", value: $draft.aggregatePIDs, in: 16...65_536, step: 16)
                Stepper("Aggregate disk: \(draft.aggregateDiskGiB) GiB", value: $draft.aggregateDiskGiB, in: 1...1_024)
                Toggle("Allow public internet egress", isOn: $draft.allowsPublicEgress)
                Stepper("Maximum lease: \(draft.maxLeaseMinutes) minutes", value: $draft.maxLeaseMinutes, in: 1...10_080)
                Stepper("Disconnect grace: \(draft.disconnectGraceSeconds) seconds", value: $draft.disconnectGraceSeconds, in: 1...3_600)
                Stepper("Stop grace: \(draft.stopGraceSeconds) seconds", value: $draft.stopGraceSeconds, in: 1...600)
            }
        }

        if !draft.isValid {
            Label("Aggregate capacity must be at least as large as each per-environment limit.", systemImage: "exclamationmark.triangle")
                .font(.caption)
                .foregroundStyle(.orange)
        }
    }
}
