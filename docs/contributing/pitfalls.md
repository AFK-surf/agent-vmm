# Contributor pitfalls

These failures explain constraints that the code must preserve.
Recheck dependency-specific behavior when its module version changes.

## Guest runtime

Workload processes have an empty capability set.
A root entrypoint cannot initialize a mode-0700 volume owned by another numeric UID.
Start the workload with the UID and GID that own the volume.

## Apple VZ boot

On `PLAT_APPLE_VZ`, `clear_boot_memory` scans only the loader boot window and the user-image neighborhood so untouched host pages stay zero-backed.
`sel4-kernel-loader` must zero and clean-invalidate (`dc civac`) the compile-time `payload.info.dtb` after `copy_data_out` whenever it is not passed to the kernel.
Otherwise, a `free_index = 0` boot Untyped retains FDT bytes and corrupts later kernel objects.

## Guest init

`std::process::Command` keeps the descriptors of its `Stdio` values until it is dropped.
Drop the command after the spawn, or the output pipe of the child never reports its end.
The standard library waits for a child whose `exec` failed.
The reaper must hold the child table lock while it calls `waitpid(-1)`. Otherwise it collects that child first and the spawn panics.

A `/proc/self/fd/N` path names the directory that the descriptor opened, also after a filesystem is mounted on it.
Resolve the path again to reach the mounted filesystem.
Move `/proc` into the workload root last. Descriptor paths stop resolving after the move.

Plain dm-crypt does not detect a wrong key.
Only dom0 knows that a disk is empty. The Guest formats a disk only when the boot configuration says so.

Device-mapper loads a writable table on a read-only block device without an error.
Load the table with `DM_READONLY_FLAG`, so the mapping is read-only too.

## Build and install

A shared data reset fences old install requests before it removes their plans.
Project that exact-request fence when reading status, even if the historical receipt still says retryable.
The fence does not prove reset completion or cloud revocation. Do not infer a missing Registration target.
Old successful installs delete their plan after they write a version 1 completion.
Use that completion as identity proof when converting an unbound historical receipt. Do not require a live plan for success.

Distribution ZIPs omit resource forks and extended attributes.
Default `ditto` archives can emit AppleDouble `._` entries for those attributes.
JavaScript extractors create ordinary files from those entries, which fail the bundle's strict code signature check.
Keep the signed file bytes and Unix modes in the archive.

Rust's macOS linker can record `libiconv` even when the service executor does not use it.
Use `-dead_strip_dylibs` when linking the executor.
Use `otool -L` to require only `libSystem`.

Guest assembly pipes cpio into xz. Without cpio, the pipeline can produce a valid empty initramfs.
Keep cpio in the Dockerfile tools stage. An empty initramfs leaves the VM in `STARTING`.

Swift generator builds resolve their own transitive dependencies, even when their source checkout came from the app build.
Copy the app’s `Package.resolved` into each generator package before building it to keep generated output consistent.

## Host store

InstallApplianceDomain must insert its private registration before the installation because registration_id has an immediate foreign key.
Keep both writes in the same transaction. Planning success cannot substitute for this ordering.

## Streams and reports

Return the response stream when the client opens a bidirectional RPC.
The first response to `BuildImage` can depend on input that the caller sends after the call returns.
Waiting for that response can also prevent the caller from cancelling the upload.
The runtime integration test covers cancellation while the upload remains open.

On macOS, setting `SO_RCVTIMEO` on a Unix socket can return `EINVAL` after the peer closes.
The receive buffer can still contain the terminal status.
Use readiness polling with the remaining RPC timeout before reading the buffer.

A macOS network interception path can accept the system TLS client while Go handshakes fail before HTTP.
Runtime archive downloads use the system curl adapter on macOS.
Keep address admission, the pinned destination, and Guest content checks across that adapter.

## Guest network

Nftables v0.3.0 sends an atomic batch before reading ACKs and rule echoes.
The default receive buffer overflowed with seven public namespaces.
Keep the explicit per-reconcile receive buffer and five-second deadline.
Run the kernel regression in its private network namespace.
Its sixteen-namespace fixture is not a product capacity limit.

## Filesystems and quota

`CreateVolumeRequest` has no size field. A sparse volume can have a logical limit equal to the full Scope disk quota.
For admission, sum the actual bytes used by all named volumes and root overlays in the Scope.
Keep measured usage even when the total exceeds the quota.

Linux openat2 rejects `O_PATH|O_NONBLOCK` with `EINVAL`.
Add O_NONBLOCK only to ordinary beneath descriptors.
An O_PATH socket descriptor cannot open device contents or block.
Keep O_CLOEXEC, no-follow resolution, and the socket type check.

Keep control metadata and system private volumes on the control filesystem.
OCI content and system, builder, and workload snapshots belong on the pool filesystem.
An incompatible disk requires an explicitly approved new installation. Never reformat it during boot.

OCI startup prepares fixed roots without traversing imported snapshot contents.
Quota access uses no-follow descriptors for regular files and directories.
Preserve project inheritance and each writable snapshot's upper/work pairing.
Older snapshots can have the wrong workdir project.
Limit offline audit and repair to that snapshot. Do not restore startup scans or discard the disk.

An unprivileged macOS `ditto --clone` copy can succeed while changing GIDs to the destination directory's group.
Compare byte hashes and numeric owners after copying a stopped state root.
Use a copy account with the required ownership permissions, or a destination that preserves the source ownership.

Linux fallocate reserves physical backing for unwritten extents that SEEK_HOLE can report as holes.
Preserve fallocate restoration without zero writes. The Darwin repair loop would rewrite the entire Linux disk.
Skip disk restoration and serial rotation while the VMM instance lock proves that the VM runs.

Ext4 can report `ENOENT` when `Q_GETNEXTQUOTA` reaches the last allocated project.
Confirm ordinary quota access before treating this result as the end of a usage sample.
A missing device or disabled quotas must leave usage unknown.

A concurrent fork can inherit the open file description of an exclusive `File::try_lock()` lock.
`O_CLOEXEC` closes the child descriptor at exec.
A `Drop` that only closes the parent descriptor can leave the lock held until then.
An immediate reopen can fail with `EWOULDBLOCK`, for example during a concurrent `mkfs.erofs` spawn.
Call `File::unlock()` explicitly in the lock owner's `Drop`.

## Appliance lifecycle

Keep the launch request and boot nonce stable for retries of one launch.
Include volume ID in launch identity to separate reinstalls. Preserve the Guest's complete fingerprint check.
Frequent appliance reconciliation must preserve an already-current private controller route and frontend runtime.
Recreating either on each tick changes loopback ports and drops controller sessions.

## Dependency behavior

The macOS data worker has crashed inside SystemConfiguration's localized interface-name API.
Netwatch discards this optional name after interface enumeration.
The macOS interface adapter reads BSD network facts without SystemConfiguration metadata.
Preserve interface addresses, IPv6 scopes, address flags, and route selection when changing this adapter.

Pakery crypto types implement traits from pakery-core and pakery-spake2.
Update these crates together. Mixed versions can expose incompatible copies of the same trait.

## macOS secret storage

Development packages and releases without a provisioning profile have no Keychain permissions.
A fresh installation selects file storage when its Keychain probe fails.
Keychain and Secure Enclave need a signed release package, its provisioning profile, and a logged-in user session.
A daemon uses file storage. The Host does not switch an existing Keychain installation to files when Keychain becomes unavailable.
