import SwiftUI

struct RemotePoolDraft {
    var perEnvironmentPIDs: Int
    var perEnvironmentDiskBytes: UInt64
    var allowsPublicEgress: Bool
    var stopGraceSeconds: Int

    init(_ settings: RemotePoolSettings = .defaults) {
        perEnvironmentPIDs = Int(settings.perEnvironmentPIDs)
        perEnvironmentDiskBytes = settings.perEnvironmentDiskBytes
        allowsPublicEgress = settings.allowsPublicEgress
        stopGraceSeconds = Int(settings.stopGraceSeconds)
    }

    var isValid: Bool {
        perEnvironmentPIDs > 0 && perEnvironmentDiskBytes > 0 && stopGraceSeconds > 0
    }

    var settings: RemotePoolSettings {
        RemotePoolSettings(
            perEnvironmentPIDs: UInt32(perEnvironmentPIDs),
            perEnvironmentDiskBytes: perEnvironmentDiskBytes,
            allowsPublicEgress: allowsPublicEgress,
            stopGraceSeconds: Int64(stopGraceSeconds)
        )
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
        Stepper("Per-environment PIDs: \(draft.perEnvironmentPIDs)", value: $draft.perEnvironmentPIDs, in: 16...65_536, step: 16)
        Stepper("Per-environment disk: \(draft.perEnvironmentDiskGiB) GiB", value: $draft.perEnvironmentDiskGiB, in: 1...1_024)

        if showsAdvanced {
            DisclosureGroup("Advanced Capacity") {
                Toggle("Allow public internet egress", isOn: $draft.allowsPublicEgress)
                Stepper("Stop grace: \(draft.stopGraceSeconds) seconds", value: $draft.stopGraceSeconds, in: 1...600)
            }
        }

    }
}
