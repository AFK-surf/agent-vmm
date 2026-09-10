import SwiftUI

struct RemotePoolDraft {
    var maxEnvironments: Int
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
        maxEnvironments > 0 && perEnvironmentCPU > 0 && perEnvironmentMemoryBytes > 0 &&
            perEnvironmentPIDs > 0 && perEnvironmentDiskBytes > 0 && maxLeaseMinutes > 0 &&
            disconnectGraceSeconds > 0 && stopGraceSeconds > 0
    }

    var settings: RemotePoolSettings {
        RemotePoolSettings(
            maxEnvironments: UInt32(maxEnvironments),
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
                Toggle("Allow public internet egress", isOn: $draft.allowsPublicEgress)
                Stepper("Maximum lease: \(draft.maxLeaseMinutes) minutes", value: $draft.maxLeaseMinutes, in: 1...10_080)
                Stepper("Disconnect grace: \(draft.disconnectGraceSeconds) seconds", value: $draft.disconnectGraceSeconds, in: 1...3_600)
                Stepper("Stop grace: \(draft.stopGraceSeconds) seconds", value: $draft.stopGraceSeconds, in: 1...600)
            }
        }

    }
}
