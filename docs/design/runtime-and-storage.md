# Runtime and storage

## Workload VM

The dom0 service starts one l4h VM for each workload.
Each VM has its own Linux kernel, switch address, image disk, writable disks, and volume mounts.
The public API calls a workload a container. Its `instance_id` changes on each start.

`vmm-guest-init` is PID 1 in the VM.
It reads its switch slot from the kernel command line and fetches the boot configuration from dom0 one time.
The boot configuration contains the disk keys, the mounts, the workload command, and the RPC token.
PID 1 assembles the root filesystem, starts the workload, and serves the Guest RPC to dom0.
The Guest RPC covers health, exec, logs, workspace files, and power off.

The workload and its exec processes run in one cgroup with the PID limit of the workload.
Dom0 authenticates a boot through its Guest RPC health reply before it reports the VM as running.
A graceful stop asks PID 1 to stop the workload and unmount its disks. Dom0 then destroys the VM.
A forced stop destroys the VM directly.

Public `ResourceLimits` grants contain only PID and disk limits.
The Host sets the CPU count and the memory sizes of each workload VM.

## Memory

A workload VM boots with its boot memory and plugs more memory on demand, to its grant.
The default boot memory is 384 MiB. The default grant is 4096 MiB.
PID 1 enables a 512 MiB zram swap device in the VM.

`l4h-memd` owns the free memory of the l4h computer.
`GetMemorySummary` reports its layers: clean, free, inflated, and plugged memory.
Clean memory was never in use after boot. A VM released free memory, and the hypervisor still backs it.
`l4h-memd` returned inflated memory to the hypervisor.
On macOS, the Carrier sets the VirtIO balloon target and `l4h-memd` returns free memory to macOS.

## Root filesystem and volumes

The image disk is one read-only EROFS filesystem. Workload VMs with the same image share that disk.
The root filesystem is an overlay on the image.
A writable root keeps its upper layer on a private dm-crypt ext4 disk.
A read-only root keeps the mount points that the image lacks in memory.

Each named volume is a private dm-crypt ext4 disk.
A disk with a writer has no other running VM. Several VMs can mount one volume read-only.
The Guest formats a disk only when the boot configuration says that the disk is empty.
Dom0 clears that instruction after the first health reply of that boot.

Each disk has its own random key. Dom0 seals the key with its master key and the disk identity.
Only a VM that mounts the disk receives the key, in its boot configuration.

A Host share reaches the VM as a 9p2000.L mount through a dom0 gateway port.
On macOS, the Carrier serves the directory to dom0 through a QUIC stream.
Workspace file operations use the Guest RPC. For a volume with no running workload, dom0 mounts the volume in a helper VM.

## Extent pool and quotas

The extent pool is a region of one block device. It allocates 4 MiB blocks.
The pool database holds the allocation records, the sealed disk keys, and the boot records.
The database resides on the dom0 control filesystem, outside the pool.
On macOS, the pool and the control filesystem are regions of the pool file `pool.raw`.

Image extents are immutable. Root overlay and volume extents are writable and sparse.
Dom0 releases an extent only after no VM has it attached. It then discards the blocks.
A reconciliation with the complete Host inventory removes orphan pool objects.
A missing referenced object stops that reconciliation.

The environment disk quota is a hard limit for each disk.
Admission adds the measured bytes of all named volumes and root overlays in the environment.
A quota does not allocate every permitted byte in advance.
The pool refuses a new allocation when the backing storage has less free space than its emergency reserve.
Shared logical image size is not physical space reclaimed by deleting one grant.

## Images and builds

The Host verifies each image before dom0 installs it.
`image_pull` reads a public registry without credentials and selects `linux/arm64`.
An import accepts an OCI archive stream, a public HTTPS archive URL, or a prebuilt EROFS bundle.
The Host checks the archive size, SHA-256, manifest digest, configuration, and layers.

The image builder applies the OCI layers in a virtual tree and writes one EROFS image.
Archive names never become paths in the Host or dom0 filesystem.
The builder rejects a layer that has a `security.*` extended attribute.
On macOS, the Host writes the EROFS image and sends it to dom0 through the Carrier.

The manifest digest identifies the installed image. Each environment holds its own image grant.
Dom0 discards the image disk when no grant, retained build output, or workload refers to that digest.
An image operation keeps a receipt with its request ID in the Host database.

Each build starts a disposable build VM from the trusted BuildKit image of the Guest bundle.
BuildKit runs without root as UID 1000. It keeps only `SETUID`, `SETGID`, and `SETPCAP` for sub-ID mapping.

The build VM has separate encrypted work and root disks of at most 4 GiB each.
It has no Host socket grants. The request selects no egress or the egress of the environment.
Dom0 destroys the build VM and discards its disks when the build settles.

Use the [image receipt contract](../protocols/remote.md#image-operation-receipts) before replacement or deletion.

## Recovery

At startup, the dom0 service reclaims the helper VMs, build disks, and staged images of its previous process.
It then rebuilds the network policy of each running workload VM and authenticates the VM again.
It stops a boot whose retirement started or whose VM no longer runs.

At startup, the Host marks running receipts as interrupted and clears controller connection state.
It adopts each live workload VM that matches its stored boot identity.
It reports a mismatched or unowned VM. It does not stop that VM with operator authority.
Host startup loads revocation and cleanup fences before it admits replacement control paths.

Explicit VM shutdown checks every live boot identity before its first effect.
It then stops each workload VM. On macOS, dom0 flushes the pool before the Carrier stops the VM.

Use [backup](../operator/backup.md) for an offline copy.
Use [cleanup](../operator/cleanup-and-local-resources.md) for exact, confirmed deletion.

## Host database

[The store](../../crates/vmm-core/src/store.rs) owns the Host SQLite database.
An owner lock file excludes a second Host. The database uses WAL mode.

The store accepts its own application identifier with schema version 1 or 2.
It rejects other databases. A new database gets the current schema.
One transaction applies the pending schema additions at startup.

Schema changes preserve Host identities, registrations, allocation authority, and accepted receipts.
They do no filesystem, credential, Guest, or remote-provider operations.
The dom0 pool database, the extent pool, secrets, and lifecycle journals are separate storage domains.
