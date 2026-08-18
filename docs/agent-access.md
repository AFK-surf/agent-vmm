# Agent access

## MCP appliance

Agent VMM exposes MCP only through the bundled appliance's loopback Streamable HTTP endpoint. Enable the appliance in Agent Access, then configure a client with its generated endpoint URL. The URL contains a per-client opaque binding and creates one appliance-owned allocation; it is not an OperatorService or runtime credential.

For Codex, the app runs:

```text
codex mcp add agent-vmm --url http://127.0.0.1:<port>/mcp?access_token=<binding>
```

The MCP appliance owns its tools, allocations, leases, runtime sessions, cleanup policy, and HTTP authentication. It does not expose stdio MCP, raw TCP forwarding, or an external controller connector. Containers run without Linux capabilities; use images and volumes prepared for their fixed non-root UID/GID.

Image references use the canonical OCI name returned by `image_pull`; short Docker Hub names normalize to `docker.io/library/...`. `container_run` preserves the image ENTRYPOINT and CMD unless explicitly overridden.

## Cross-device services

The host supervises the data worker and owns pairing, trust, and service authorization. The data worker is not a LaunchAgent and does not provide a controller-management interface.

`service_import` has three deliberately different paths. For an export on this
VM, pass `destination_device_id: "local"` and omit `capability`; the host
resolves the local device and still checks the destination environment's
controller domain. A cross-device route needs a current controller capability
and destination port. A personal-mesh route uses `mesh_id` and all three
nonzero budget fields instead. Agent-scoped listing exposes only the current
environment's exports, so remote export IDs and capabilities must come from
the destination environment or controller.

`container_run` reports process launch, not service readiness. Use
`container_get`, bounded logs, or an application probe. `log_limit_bytes` is
the persisted per-container retention cap; `container_logs.max_bytes` only
limits one response. `container_signal` targets the retained workload process
(not the sandbox supervisor); applications may still exit naturally while a
signal is in flight.

Environment PID grants include supervisor, workload, and exec processes. The
bundled MCP manifest requests 1024 PIDs per environment, subject to the host
or remote pool's approved grant; its default UI grant is 512. The MCP
appliance reserves request capacity for lifecycle and observation calls, but
parallel exec still consumes the PID and I/O budget. A
`PID_LIMIT_EXCEEDED` response is a quota result; finish execs or containers
before retrying. A local service import that references a stopped or replaced
destination instance returns `DESTINATION_CONTAINER_INSTANCE_STALE`; refresh
the export instead of retrying the old instance.

The `virtual_service_name` from `service_import` is available to the source
container through its namespace search domain as either the short name or
`<name>.<namespace>.agent`; it is not registered in the host's DNS.

Unexpected host or guest failures use the structured `RUNTIME_OPERATION_FAILED`
code rather than a bare `INTERNAL` response.
