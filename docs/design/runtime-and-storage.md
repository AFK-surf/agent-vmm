# Runtime and storage

## Workload runtime

VMMD maps logical environments to owned containerd objects in one private runtime namespace.
Physical image records use manifest digests. Logical image grants remain environment-specific.
Each workload gets its own runsc sandbox, writable snapshot, cgroup, network endpoint, and volume mounts.
Persistent snapshots use containerd overlayfs. Runsc adds no persistent nested overlay.
Immutable lower layers can serve several workloads.

The fixed supervisor remains sandbox init. The user process runs as a retained exec process.
Container `instance_id` changes on each process start.
The Guest reconciles task state before it reports `running` or `stopped`.
A stale containerd label does not override a missing task.

CPU and memory remain shared within the Guest capacity.
Public `ResourceLimits` grants contain only PID and disk limits.
PID usage includes supervisors, workload processes, and exec processes.

## Filesystems and quotas

The state disk separates control data from workload storage.
The control filesystem holds containerd metadata, VMMD records, and system private volumes.
The pool filesystem holds OCI content, overlay snapshots, workload volumes, logs, and build data.
Pool exhaustion must not prevent startup, stop, or recovery metadata writes.
Existing incompatible disks require an explicitly approved new installation.
Boot does not format an existing disk.

A namespace project quota covers writable snapshots and private volumes.
Overlay upper and work directories retain the same project.
Creation-time inheritance assigns immutable content to the shared pool project.
Shared logical image size is not physical space reclaimed by deleting one grant.

Disk admission uses filesystem usage, quota commitments, metadata reserves, and admitted import or builder growth.
A quota is a hard limit. It does not allocate every permitted byte in advance.
A system installation has an independent data quota on the control filesystem.
Appliance stop retains that data and its quota. Successful removal releases its exact namespace.

## Images and builds

Image availability requires the logical grant, content graph, and materialized lower snapshot.
An exact digest-bound import can repair missing materialization.
A different import fingerprint cannot replace the original request identity.
Multi-platform indexes select `linux/arm64`.
Import checks the archive size, SHA-256, manifest digest, configuration, and layers.
Registry export normalizes Docker media types before it commits an OCI manifest digest.

Each build starts a separate runsc sandbox with a fresh rootless BuildKit worker as UID 1000.
Only sandbox-visible `SETUID`, `SETGID`, and `SETPCAP` support sub-ID mapping.
The sandbox mounts only the authorized context, temporary secrets, cache, and output.
BuildKit control sockets never enter a workload.
Build output uses the caller's bounded progress stream. It does not enter the shared Guest log.

Use the [image receipt contract](../protocols/remote.md#image-operation-receipts) before replacement or deletion.

## Recovery

At Guest startup, VMMD marks stale `running` records as stopped and rebuilds network configuration.
Host startup loads revocation and cleanup fences before it admits replacement control paths.
A failed startup commit closes admission and egress. It does not stop the Guest.
Explicit VM shutdown settles writers, stops tasks, and closes their process and log handles before disk teardown.

Use [backup](../operator/backup.md) for an offline copy.
Use [cleanup](../operator/cleanup-and-local-resources.md) for exact, confirmed deletion.
