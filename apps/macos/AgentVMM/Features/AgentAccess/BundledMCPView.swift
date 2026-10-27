import SwiftUI

struct BundledMCPView: View {
    @Bindable var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Label("MCP", systemImage: "curlybraces.square").font(.title2.weight(.semibold))
            if let appliance = model.bundledMCP {
                GroupBox("Appliance status") {
                    Form {
                        LabeledContent("Installation", value: appliance.id)
                        LabeledContent("State", value: String(describing: appliance.state))
                        LabeledContent("Endpoint", value: appliance.endpointStatus)
                        if let message = appliance.healthMessage { Text(message) }
                        ForEach(appliance.frontends) { frontend in
                            LabeledContent(frontend.name, value: frontend.endpoint)
                            if !frontend.errorReason.isEmpty { Text(frontend.errorReason).foregroundStyle(.secondary) }
                        }
                    }.padding(8)
                }
                if let frontend = appliance.frontends.first(where: { $0.name == BundledMCPAppliance.frontendName && $0.available }) {
                    GroupBox("Client connection") {
                        VStack(alignment: .leading, spacing: 12) {
                            Text("The Host owner issues tokens with this command:")
                            Text("agent-vmm mcp token create").font(.body.monospaced()).textSelection(.enabled)
                            Text("Set AGENT_VMM_MCP_TOKEN to the issued token, then configure the client:")
                            Text(MCPClientConfiguration.connectionCommand(endpoint: frontend.endpoint))
                                .font(.body.monospaced()).textSelection(.enabled)
                        }.padding(8)
                    }
                }
            } else {
                Text("The Host lifecycle installs the MCP appliance. Its status is unavailable.").foregroundStyle(.secondary)
            }
        }
    }
}
