---
name: agent-vmm-interface
description: Use Agent VMM's Streamable HTTP MCP and host gRPC runtime APIs safely, including container, volume, service-route, and appliance-build workflows.
---

# Agent VMM interface guide

Use this guide when an agent operates an Agent VMM environment or writes an
integration test. Prefer host gRPC for deterministic end-to-end tests; use MCP
only when testing the agent-facing Streamable HTTP contract.

## Transport and prerequisites

- MCP is Streamable HTTP only. The endpoint is the loopback URL and bearer
  token shown by the host app or host API. Do not start a stdio proxy or a
  separate forwarding daemon.
- `503 MCP runtime unavailable` describes this MCP frontend. Inspect its VM,
  appliance, allocation, and exact error. It does not prove sibling workloads
  stopped or authorize their restart, deletion, or re-enrollment.
- Every operation is scoped to the authenticated environment. A successful
  connection does not grant access to another environment, device, or mesh.

## Container workflow

1. Pull the image explicitly with `image_pull` when the image is not already
   present.
2. Create referenced named volumes before creating a container. `volume_create` returns
   the effective environment disk limit in `limit_bytes`; this is an
   environment quota, not an independent per-volume quota.
3. Use `container_run` for a new container, or `container_start` for a created
   or stopped container. `launch_succeeded` means that the main process was
   started; it is not a readiness or health result.
4. After a run, inspect with `container_get`, read bounded `container_logs`, or
   use an application-specific probe/`container_exec` to decide readiness.
5. For MCP `container_exec`, put one-shot input in `stdin`; the MCP adapter
   sends the explicit close event for you. For a raw host gRPC exec stream,
   send all input and then send `close_stdin: true`; closing the outer stream
   alone is not a substitute for closing the process stdin pipe.
6. Use `container_stop` for graceful shutdown, `container_signal` with `TERM`
   or `KILL` only when needed, then `container_wait` for the exit code. Delete
   only after the container is stopped.

Container `instance_id` identifies one running process generation. It changes
   after every restart and must be refreshed before using any instance-fenced
   API. A stopped container is not a running container even if its task is
   still being reaped; callers should observe one terminal `stopped` state.

## Service routes

- Call `mesh_service_list` to discover exports/imports, and filter records with
  `revoked: true` when selecting an active route; historical rows remain
  visible for audit. Always use the source container's current `instance_id`.
- `service_export` requires `container_id`, current `container_instance_id`,
  `port`, and `protocol` (`tcp` or `http`). A container restart or terminal
  stop automatically revokes exports and their imports/routes; export the new
  instance again.
- `service_import` requires the source container and instance, destination
  device, destination export ID, and a virtual service name.
- The virtual service name is resolvable from the source container through its
  namespace search domain (`<name>` or the canonical `<name>.<namespace>.agent`);
  it is not a host-wide DNS record.
- For an export on this VM, use `destination_device_id: "local"` and omit
  `capability`. The host resolves the local device and permits the route only
  within the same environment or controller registration domain. For a
  cross-device route, provide a controller-issued `capability`; for a personal
  mesh route, provide `mesh_id` plus all three nonzero budget fields. Do not
  combine authorization forms.
- `destination_port` is needed only when a direct capability route targets a
  remote device. It is not needed for a local destination or for the personal
  mesh form; the remote endpoint supplies that port.
- The agent-scoped service list contains only the current environment's
  exports. Remote export IDs and capabilities are supplied out of band by the
  destination environment or controller.
- Route capabilities, exports, imports, leases, and projections are all
  generation/instance fenced. On a stale-instance or stale-route error, fetch
  current facts and create a new route instead of retrying the old capability.
  This route fence does not set the lifetime of an independently running workload.

## Image and sandbox expectations

The runtime accepts OCI images but is not a Docker-compatible privileged
runtime. Workloads run without Linux capabilities, setuid, device access, or
host mounts. Images whose entrypoint performs `chown`, `setuid`, or privileged
initialization can fail at startup; use an unprivileged image or an explicit
non-root configuration. Do not add a capability, runc, or host-filesystem
fallback in an integration.

## Appliance Dockerfile build

The supported build path is:

```text
agent-vmm appliance build \
  --context <directory> \
  --dockerfile Dockerfile \
  --output <artifact-directory> \
  --publisher-key development.seed \
  --product <product-id> \
  --release <release>
```

The output is an Agent VMM appliance artifact (`image.oci.tar`, signed
manifest, signature, and publisher key). It is intended for staging/installing
into Agent VMM and does not promise publication as a general-purpose Docker
image.

## Current operation limits

Record these constraints in test plans and do not silently work around them:

- `container_run` reports process launch, not service readiness. Probe the
  requested service when needed, not every unrelated container or appliance.
- Same-container concurrent `container_exec` calls can contend with lifecycle
  and container PID/IO budgets. The MCP adapter reserves half of its request
  slots for exec streams so lifecycle and observation calls retain capacity;
  inspect the returned limit before retrying. Do not create extra containers
  or serialize independent work merely because this implementation has slots.
- PID limits are environment grants, not a per-command setting. They include
  container supervisors, workload processes, and exec processes. A
  `PID_LIMIT_EXCEEDED` error requires checking the actual grant and usage.
  Stop or remove work only within the user's requested scope. Manifest and UI
  defaults are not permanent product capacity requirements.
- `PERMISSION_DENIED` from an import means the requested route crossed the
  controller-domain boundary or its supplied authorization is not current;
  same-device routes can avoid a capability only with the `local` destination
  alias.
- Close exec stdin when no further input is needed. Missing EOF can make commands such as
  `sha256sum` wait until the caller timeout; the MCP adapter closes one-shot
  stdin, while raw gRPC callers must send `close_stdin: true`.
- Multiline environment values are valid. Malformed entries or NUL bytes
  return the structured `ENVIRONMENT_INVALID` error. Unexpected host/guest
  failures are reported as `RUNTIME_OPERATION_FAILED`, not a bare `INTERNAL`.
- Redis-style images that call `setpriv` and standard Nginx images that need
  `chown` are expected compatibility failures under the sandbox; unprivileged
  variants are the current common path.
- A container restart changes its network instance address. Consumers that
  cache peer addresses must resolve the service name again or restart their
  own client when authorized. This does not require a new discovery service.
- `container_signal` targets the retained workload process rather than the
  sandbox supervisor. A signal can still race a natural process exit, so read
  the terminal state and exit code instead of inferring it from the request.
- `log_limit_bytes` is the persisted retention limit; `container_logs.max_bytes`
  is only the response-page bound. They intentionally have different ranges.

When reporting a failure, include the operation, environment/container ID,
current instance ID, structured error code, and the bounded log tail. Avoid
retrying a mutation with the same request ID after an `UNKNOWN` result until
the authoritative state has been read.
