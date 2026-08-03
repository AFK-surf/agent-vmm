import SwiftUI

@main
struct AgentVMMApp: App {
    @State private var model = AppModel()

    var body: some Scene {
        WindowGroup {
            RootView(model: model)
                .frame(minWidth: 880, minHeight: 600)
                .task { await model.start() }
        }
        .defaultSize(width: 1100, height: 720)

        MenuBarExtra("Agent VMM", systemImage: model.snapshot.health.symbolName) {
            Text(model.snapshot.health.label)
            Text(DisplayFormatting.age(of: model.snapshot.collectedAt))
        }
    }
}
