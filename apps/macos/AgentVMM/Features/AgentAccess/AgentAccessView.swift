import SwiftUI

struct AgentAccessView: View {
    @Bindable var model: AppModel

    var body: some View {
        ScrollView {
            BundledMCPView(model: model)
                .frame(maxWidth: 980, alignment: .leading)
                .padding(28)
        }
        .navigationTitle("MCP")
    }
}
