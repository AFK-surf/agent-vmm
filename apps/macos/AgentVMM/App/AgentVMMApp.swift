import SwiftUI

@main
struct AgentVMMApp: App {
    @State private var model = AppModel()

    var body: some Scene {
        WindowGroup {
            RootView(model: model)
                .frame(minWidth: 1120, minHeight: 680)
                .task { await model.start() }

        }
        .defaultSize(width: 1280, height: 800)

    }
}
