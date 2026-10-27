# Appliance protocol

An installation owns one selected artifact, private controller registration, and persistent data volume.
The Host owns lifecycle receipts and publication. The appliance owns its application and controller behavior.
The wire owner is [appliance.proto](../../api/appliance/v1/appliance.proto).

## Manifest and build

`ControllerApplianceManifest` contract version is `1`.
It binds product, release, `linux/arm64` image, manifest digest, and content archives.
It requests PID and disk limits, environment capacity, egress, and static frontends.
The operator approves the effective grant. A manifest request alone grants no access.
The startup deadline bounds activation.

Build an artifact with a source-built `agent-vmm`:

```sh
agent-vmm appliance build --context DIRECTORY --dockerfile Dockerfile \
  --output ARTIFACT_DIRECTORY --product PRODUCT --release RELEASE
```

The result contains `image.oci.tar` and deterministic `manifest.pb`.
This command does not sign the manifest.
Dependencies use `content/<archive_sha256>.oci.tar` and digest-bound content references.
The CLI uploads missing content before lifecycle admission.
Each archive must match its exact size, SHA-256, platform, and OCI manifest digest.

An appliance image provides fixed mount targets, including `/run/agent-vmm`, `/var/lib/agent-vmm`,
`/etc/resolv.conf`, and `/.agent-vmm-supervisor`.
Parent directories cannot be symbolic links. The Guest does not alter shared image contents to repair missing targets.
The supervisor mount is executable and read-only. Runtime and data mounts remain non-executable.
Runsc uses `host-uds=create` for the projected private sockets.
It does not grant general Host Unix socket access.

## Bootstrap

A launch claim binds installation, artifact, volume-derived launch identity, Guest boot, container instance, and configuration revision.
The Host preserves the same launch request and boot nonce while retrying one launch.
A reinstall uses a different volume identity. The Guest checks the complete request fingerprint.
The signed claim includes the approved grant and exact dependency content mounts.
The controller receives authorized dependencies read-only at `/opt/agent-vmm/content`.
Workload allocations cannot access that mount.

The installation CLI reads the optional bootstrap seed from stdin.
It sends the seed through the operator gRPC request. The Host stores an opaque envelope through its selected secret backend.
Keep seeds and management credentials out of arguments, environment variables, and logs.
The envelope binds installation and bootstrap ID.
Bootstrap returns consumption receipt, health, and explicit stream closure in protocol order.
The Host clears consumed ciphertext. Repeating the same bootstrap ID is idempotent. Mismatched launch facts fail.
Ordinary restart does not replay the original management seed.

Managed trust is optional. An appliance without a trust bundle has no managed route authority.
Renewal uses the current installation, boot, instance, and configuration fences.
Read [trust and service](trust-service.md) for the renewal contract.

## Static frontend publication

A static frontend declares its socket, transport, exposure, and stream bounds.
`HOST_LOOPBACK` accepts connections from every local user. MCP authenticates issued Bearer tokens.
After first publication, the Host binds only the saved port. An occupied port returns `FRONTEND_PORT_UNAVAILABLE`.
`LAN_HTTP` requires explicit approval and application authentication.
The Host reports the actual endpoint and its availability through appliance status.
An already-current route and runtime remain published across ordinary reconciliation ticks.

Bootstrap has a positive cumulative byte limit.
Controller streams have no cumulative lifetime byte limit and retain frame, concurrency, idle, and backpressure bounds.
Frontend streams use their declared bounds. A zero total means no cumulative byte limit for that frontend.
One sender owns each gRPC stream. Half-close waits for both copy directions before stream send closure.

## Lifecycle and data

Stop closes runtime publication and retains installation data, managed trust, and private registration.
Restart waits for the exact old writer before it launches another instance.
Revoke retires the installation's private controller authority and owned workloads.
Remove requires a fresh impact plan, matching revision, and explicit confirmation of the exact resource set.
Remove retains its pending operation until Guest cleanup succeeds and the private domain is empty.
Background retries use that original operation. Other installations remain observable.

The data volume has an independent project quota on the control filesystem.
Its projected directory uses numeric UID/GID 1000 and mode 0700.
`data_volume_bytes` is a hard limit. Stop does not unmount or delete it.
`SEALED` denotes an integrity failure.
A sealed installation cannot start or restart. Stop and revoke preserve the sealed state.
Removal still requires the current approved plan.

## Update and recovery

Update persists preparation, drain, stop, switch, and activation phases.
Drain closes runtime sessions and waits for accepted executions to settle.
It does not stop independent workload containers.
Unknown execution settlement can block the update until its deadline.
Switch preserves installation identity, registration, and the data volume.
Target bootstrap and health decide activation success.

A client disconnect leaves accepted Update running.
Host restart resumes the saved phase while its deadline remains valid.
Failure retains the phase, target, data, and receipt.
Recover resumes that exact operation with an explicitly extended deadline and current observation fence.
An older observation cannot change its deadline.
Stop, Revoke, or confirmed Remove can interrupt a failed update through their existing lifecycle owners.
A running update remains exclusive. A sealed or revoked installation cannot recover into activation.

Read [operator installation](../operator/install-and-update.md) for Host package updates.
Use `ListAppliances` and the original operation receipt to resolve an uncertain result.
Do not reinstall an already-installed appliance merely because activation failed.

## Controller preparation

BuildImage accepts an owned base image by reference and manifest digest.
It validates the complete uploaded context before extraction and supplies the base as a read-only OCI layout.
CopyImage binds source and target allocation IDs and generations within the same enabled registration.
Cached copy can publish a grant without cold physical materialization.
New materialization still obeys the Guest writer slot and physical growth admission.
See [image operation receipts](remote.md#image-operation-receipts).
