import SwiftUI

struct AccessStatusView: View {
    enum Kind { case success, neutral, attention, failure }
    let label: String
    let symbol: String
    let kind: Kind

    var body: some View {
        Label(label, systemImage: symbol)
            .font(.caption.weight(.semibold))
            .foregroundStyle(color)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(color.opacity(0.12), in: Capsule())
            .accessibilityLabel("Status: \(label)")
    }

    private var color: Color {
        switch kind {
        case .success: .green
        case .neutral: .blue
        case .attention: .orange
        case .failure: .red
        }
    }
}
