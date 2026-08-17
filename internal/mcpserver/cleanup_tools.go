package mcpserver

import (
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (adapter *Adapter) registerCleanupTools(server *mcp.Server) {
	addTool(server, toolDefinition{name: "cleanup_execute", description: "Execute a current reset or irreversible discard plan after explicit confirmation; the MCP process drains after success.", inputSchema: cleanupExecuteSchema, destructive: true, timeout: 2 * time.Minute}, adapter.cleanupExecute)
}

const cleanupExecuteSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["kind","plan_revision"],"properties":{"kind":{"enum":["reset","discard"]},"plan_revision":{"type":"string","pattern":"^[1-9][0-9]{0,19}$"},"confirm_reset":{"type":"boolean"},"confirm_irreversible":{"type":"boolean"},"confirmed_volume_ids":{"type":"array","maxItems":1000,"uniqueItems":true,"items":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"}}}}`
