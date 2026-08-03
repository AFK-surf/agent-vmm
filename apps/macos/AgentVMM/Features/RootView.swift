import SwiftUI

struct RootView: View {
    @Bindable var model: AppModel

    var body: some View {
        NavigationSplitView {
            List(AppModel.Section.allCases, selection: $model.section) { section in
                Label(section.rawValue, systemImage: section.symbol).tag(section)
            }
            .navigationTitle("Agent VMM")
            .navigationSplitViewColumnWidth(min: 200, ideal: 220, max: 260)
        } detail: {
            Group {
                switch model.section {
                case .system: SystemStatusView(model: model)
                case .environments: EnvironmentListView(model: model)
                case .diagnostics: DiagnosticsView(model: model)
                case .settings: SettingsView(model: model)
                case nil: ContentUnavailableView("Choose a section", systemImage: "sidebar.left")
                }
            }
            .overlay(alignment: .top) {
                if let error = model.errorMessage {
                    HStack(spacing: 10) {
                        Image(systemName: "exclamationmark.triangle.fill")
                            .foregroundStyle(.orange)
                        Text(error)
                        Spacer()
                        Button("Dismiss", systemImage: "xmark") { model.errorMessage = nil }
                            .labelStyle(.iconOnly)
                            .buttonStyle(.plain)
                            .help("Dismiss")
                    }
                    .padding(12)
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10))
                    .shadow(radius: 4, y: 2)
                    .padding()
                    .accessibilityElement(children: .combine)
                }
            }
        }
    }
}

private extension AppModel.Section {
    var symbol: String {
        switch self {
        case .system: "gauge.with.dots.needle.67percent"
        case .environments: "shippingbox"
        case .diagnostics: "stethoscope"
        case .settings: "gearshape"
        }
    }
}
