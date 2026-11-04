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

VMMD samples pool project quotas in one coalesced worker. Requests read its cached sample without namespace RPCs or admission commitment scans.
Project 9000 reports images and shared runtime files. Project 9001 reports build cache.
Private bytes include all namespace projects on the pool filesystem, including projects outside the displayed environment page.
Other bytes are the physical remainder. These categories partition one sample of used bytes.
Import `.hold` allocations remain excluded from used bytes. Unused builder quota credit remains a reservation, not physical usage.
Missing quota facts leave categories absent. A disk sample expires after 15 seconds.
The Host stamps successful summary collection after the Guest reply.

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

VMMD owns ordinary orphan image collection. Startup reconstructs logical grants. The GC worker reconstructs accepted-source pins before destructive collection.
One worker coalesces release notifications and visits at most 32 indexed roots per batch.
Each pass has a fixed upper bound. Changes during a pass schedule another pass.
Each batch releases the materialization slot before the next batch. Failed batches retry after one second.
Shutdown cancels and joins the worker before containerd closes or filesystems detach.
Only exact digest-owned runtime image names qualify. Ordinary collection preserves retained build roots and temporary writer roots.
A grant replacement, partial release, import completion, or settled writer can wake collection.
The inventory checks grants and retention pins together. Copies publish target grants before settlement releases source pins.
Receipt acceptance and image publication pin their candidate before the metadata write.
A lost write reply preserves the previous grant and candidate pin. Positive owner indexing or terminal receipt observation releases the matching pin.
An unconfirmed write can retain its candidate until the existing startup process settles the previous containerd daemon and rebuilds inventory.
A same-daemon recovery read cannot clear a pin merely because its record is absent.
Unreadable source receipts defer orphan collection and retry in its worker. They do not block VMMD startup or existing workload service.
Source-less receipts do not require payload decoding for collection.
Fresh first-grant creation holds the materialization slot. Existing grants or accepted pins protect warm copies.
Containerd synchronous deletion preserves shared content and snapshots still used by containers or other roots.
Retained image and builder cache eviction remains pressure-driven. Deleting a logical grant does not wait for physical collection.

Use the [image receipt contract](../protocols/remote.md#image-operation-receipts) before replacement or deletion.

## Recovery

At Guest startup, VMMD marks stale `running` records as stopped and rebuilds network configuration.
Host startup loads revocation and cleanup fences before it admits replacement control paths.
A failed startup commit closes admission and egress. It does not stop the Guest.
Explicit VM shutdown settles writers, stops tasks, and closes their process and log handles before disk teardown.

Use [backup](../operator/backup.md) for an offline copy.
Use [cleanup](../operator/cleanup-and-local-resources.md) for exact, confirmed deletion.

## Host database upgrades

[Store](../../internal/host/store/schema.go) owns forward SQLite migrations from schema 39.
The compiled catalog defines the current schema. New databases use the current schema snapshot.
Each later change adds one ordered SQL migration and updates that snapshot.
Migrations preserve Host identities, registrations, custody references, allocation authority, and accepted receipts.
They perform no filesystem, credential, Guest, or remote-provider operations.
The Host owner lock excludes another Host. A single SQLite transaction commits the complete pending chain and its version records.
Commit completes migration. A later startup failure requires forward repair or the authorized reset policy, never automatic downgrade.
Guest control and pool filesystems, containerd metadata, volumes, secrets, and lifecycle journals are separate storage domains.
A future incompatible change to those domains needs a design from its owner.
