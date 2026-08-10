# Agent access

## Local MCP

Build or install the signed bundle, then configure the MCP host to execute:

```text
~/Applications/Agent VMM.app/Contents/Helpers/agent-vmm-mcp --integration <managed-integration-id>
```

The server communicates only on stdio. The managed integration binds each process to one LOCAL environment;
ending the process releases its session and loopback forwards but retains the
environment and named volumes.

Image references use the canonical OCI name returned by `image_pull`; short Docker
Hub names such as `postgres:16-alpine` are accepted and normalize to
`docker.io/library/postgres:16-alpine`. `container_run` preserves the image
ENTRYPOINT and CMD. Set `entrypoint` or `command` only to override that part of the
OCI configuration; provide both to replace the complete process command.
`launch_succeeded` means that the initial process was launched, not that the
service is ready. The response also contains the state observed after a short
bounded interval; use `container_get`, logs, or an application health check for
readiness.

Image records are scoped to one agent environment, so `image_list` only returns
images pulled into that environment. The guest stores content-addressed blobs in
a VM-wide content store and can deduplicate identical layers across environments.

`volume_create` can set `owner_uid`, `owner_gid`, and a numeric permission `mode`
once, when the volume is created. Use these fields for images such as PostgreSQL
that require a non-root data directory. The sandbox does not grant workloads a
general-purpose `CAP_CHOWN`. A port forward is created only for a running
container and is revoked when that specific container instance stops or is
replaced.

Containers run with no Linux capabilities, including `CHOWN`, `SETUID`, and
`SETGID`. Images must support a fixed non-root user without privileged startup.
Some stock entrypoints, including configurations of the official nginx image,
attempt ownership or user changes and will not run unchanged. Use an image built
for non-root execution, pre-create retained volumes with the required numeric
owner, or explicitly override the entrypoint and user when the image supports it.

## Remote controller

In Agent Access, enter a TLS `host:port`, controller name, one-time enrollment token,
and bounded pool size. The app creates the host-owned registration and passes the
token once to the signed connector through stdin. The returned opaque credential
is stored in macOS Keychain and is absent from SQLite, snapshots, logs, and support
bundles. The connector LaunchAgent discovers non-secret registration metadata
through the restricted local socket and then connects outbound.

The public contract is `api/remote/v1/remote.proto`. Controller adapters map their
worker identity to registration/allocation/opaque workload references and use the
per-lease AgentRuntimeService tunnel. They must not send local environment or
namespace identities. No Salix or cue SDK is required by Agent VMM.

Enable admits new leases. Drain prevents new work while existing leases finish.
Revoke deletes the Keychain credential and closes remote access. Release retains
containers and volumes for local inspection; discard is irreversible and requires
the current cleanup revision and exact volume confirmation.

The restricted socket prevents accidental OperatorService use but shares the
logged-in user's UID. A compromised connector is therefore not claimed to have
OS-level isolation. V0 also excludes multiple controllers, public ingress,
dynamic VM resizing, oversubscription, and a cloud scheduler.
