# Agent access management UI

Agent Access installs and controls the bundled MCP controller appliance. The appliance is disabled after installation; enable, disable, restart, and removal are revision-bound appliance operations.

The UI configures Streamable HTTP MCP clients with separate opaque bindings. The app registers Codex with the current loopback endpoint URL; no `agent-vmm-mcp` stdio proxy, port-forward helper, or external connector is installed.

The host starts and drains the data worker with its own lifecycle. Pairing and cross-device service authorization remain host-owned.
