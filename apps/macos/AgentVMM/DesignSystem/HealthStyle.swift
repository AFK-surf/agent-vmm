import SwiftUI

extension HealthLevel {
    var label: String {
        switch self { case .healthy: "Healthy"; case .degraded: "Needs attention"; case .unavailable: "Unavailable" }
    }
    var symbolName: String {
        switch self { case .healthy: "checkmark.circle.fill"; case .degraded: "exclamationmark.triangle.fill"; case .unavailable: "xmark.octagon.fill" }
    }
    var color: Color {
        switch self { case .healthy: .green; case .degraded: .orange; case .unavailable: .red }
    }
}

struct HealthLabel: View {
    let level: HealthLevel
    var body: some View {
        Label(level.label, systemImage: level.symbolName)
            .foregroundStyle(level.color)
            .accessibilityLabel("Status: \(level.label)")
    }
}
