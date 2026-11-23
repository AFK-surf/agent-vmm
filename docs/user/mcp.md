# MCP workloads

## Connect a client

The Host serves MCP on a loopback HTTP port. The default port is 8788.
The Host reports this service as the appliance installation `agent-vmm-mcp`.
The Host owner gives the endpoint URL to each client user.
The port stays fixed. The Host does not start when another process holds the port.

The Host owner issues a token:

```sh
umask 077
agent-vmm mcp token create > token
```

Keep the token private or transfer it securely to its user.
The token authorizes one environment within the approved MCP installation grant.
A client cannot select another environment or get operator authority.

Configure an MCP client with the loopback URL and a Bearer `Authorization` header.
For example, configure Codex from a shell that contains the token variable:

```sh
export AGENT_VMM_MCP_TOKEN=$(cat token)
codex mcp add agent-vmm --url http://127.0.0.1:<PORT>/mcp --bearer-token-env-var AGENT_VMM_MCP_TOKEN
```

Set the variable in each client session. Keep tokens out of URLs and command arguments.
Only issued tokens work. `agent-vmm mcp token rotate` invalidates all existing tokens.
Rotation preserves their environments. Issue replacement tokens after rotation.
The client owns reconnects when the Host or appliance becomes unavailable.

## Tools

Use `tools/list` for the current JSON schemas and bounds.
Each tool operates only in the environment that the token selects.

Tools cover environment inspection, images, containers, exec, logs, volumes, service routes, and confirmed cleanup.
`cleanup_plan` changes no state. `volume_delete` requires an unmounted volume.

A successful tool result contains `ok: true` and `data`.
A failure contains `ok: false` and a structured `error`.
A mutation can also return partial facts. Preserve them before another action.

## Images

Pull an image before container creation unless it already exists in the environment.
Pull selects `linux/arm64` and normalizes short Docker Hub names to `docker.io/library/...`.
Use the canonical returned reference for later calls.
A new pull uses anonymous registry access and the immutable import path.
When you delete an image grant, shared content can stay on the disk.

## Containers and exec

Create referenced volumes before `container_run`.
The image ENTRYPOINT and CMD remain effective unless you explicitly override them.
`launch_succeeded` confirms process launch. It does not confirm service readiness.
Use current container state, bounded logs, or an application probe to decide readiness.

Each container runs in its own VM. Each restart changes `instance_id`.
Refresh it before an instance-fenced operation.

Signal targets the main workload process. Read terminal state because natural exit can race a signal.
Stop gracefully before deletion. Container deletion preserves named volumes.

`container_exec` accepts an argument array. Use explicit shell arguments when you need shell syntax.
Its `stdin` is one-shot input. The adapter sends EOF after that input.

## Volumes and UID/GID

Set numeric volume ownership during `volume_create` when the image runs as a non-root user.
Start the workload with matching numeric UID/GID.
A process that runs as a non-root user has no Linux capabilities.
A process that runs as root has root authority in its own VM only.

An image layer with a `security.*` extended attribute fails the import.
Do not add a runtime or Host mount fallback.
`limit_bytes` describes the environment disk quota, not a separate volume reservation.

## Service export and import

An export authorizes one container ID and port. It follows that approved container after replacement.
An absent container leaves the export unavailable. Explicit revocation ends the authorization.
An import requires the current instance ID of the source container.

A virtual service name resolves as `<name>`, `<name>.services.agent`, or `<name>.<namespace>.agent`.
It resolves only for the source container. It is not a Host DNS record.

For a local route, use `destination_device_id: "local"` and omit `capability`.
The Host still enforces the same environment or controller domain.

For a cross-device controller route, supply the destination export, port, and current capability.
For a personal route, supply `mesh_id` and all three nonzero route budget fields.
Do not combine these authorization forms.
The Host now rejects both forms. It accepts only local routes.

Remote exports and capabilities come from the destination owner. Local listing does not enumerate them.
Refresh current facts after a stale-instance or stale-route error.
Re-resolve service names after a destination restart.

## Limits and failures

Pages contain at most 100 records. Serialized tool results have a 2 MiB bound.
The adapter admits 16 requests and at most eight exec streams per client adapter.
It reserves the remaining request capacity for observation and lifecycle work.

PID grants include the workload and its exec processes.
Check actual grant and usage after quota exhaustion. Stop work only within the approved scope.

`log_limit_bytes` sets persisted retention. `container_logs.max_bytes` bounds one response.

Multiline environment values are valid. Malformed entries and NUL bytes fail validation.
Use the [error reference](../reference/errors.md) for reason and retry details.
A timeout or disconnect does not prove mutation rejection.
Read authoritative state before retrying an uncertain side effect.
An unavailable MCP frontend does not prove sibling workloads stopped or authorize their restart or deletion.
