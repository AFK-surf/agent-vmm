---
name: agent-vmm-interface
description: Operate Agent VMM workloads through the local MCP stdio bridge or environment-scoped Host gRPC API.
---

# Agent VMM interface

Read [the MCP guide](../../../docs/user/mcp.md) before operating workloads.
It defines tools, current bounds, volume ownership, service routes, and uncertain results.

Use the private Unix frontend through `agent-vmm mcp --client CLIENT_LABEL` for MCP clients.
Use Host gRPC for deterministic integration tests unless the test targets the MCP contract.
The Host selects one environment for each runtime session.
Do not infer authority over another environment from a successful connection.

Read [the remote protocol](../../../docs/protocols/remote.md) for controller integration and image receipt recovery.
Read [the appliance protocol](../../../docs/protocols/appliance.md) for artifact build, bootstrap, and lifecycle operations.
Read [cleanup](../../../docs/operator/cleanup-and-local-resources.md) before destructive work.
Use [the error reference](../../../docs/reference/errors.md) for machine-readable failures.
