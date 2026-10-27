import Foundation

enum MCPClientConfiguration {
    static func connectionCommand(endpoint: String) -> String {
        "codex mcp add agent-vmm --url http://\(endpoint)/mcp --bearer-token-env-var AGENT_VMM_MCP_TOKEN"
    }
}
