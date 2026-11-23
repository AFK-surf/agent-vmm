# Contributor pitfalls

These failures explain constraints that the code must preserve.
Recheck dependency-specific behavior when its module version changes.

## Guest runtime

A workload process that runs as a non-root user has an empty capability set.
PID 1 sets the owner and mode of a volume only when it formats the empty volume.
Start the workload with the UID and GID that own the volume.

Disposable build VMs (`BuildVm`) set `subid_caps` so `vmm-guest-init` retains only ambient `CAP_SETGID`, `CAP_SETUID`, and `CAP_SETPCAP` across the UID 1000 drop, and align `BUILDKIT_HOST` to `unix:///workspace/runtime/buildkit/buildkitd.sock`.
OCI layer import rejects `security.*` xattrs, so `newuidmap` and `newgidmap` cannot use file capabilities.

## Apple VZ boot

On `PLAT_APPLE_VZ`, `clear_boot_memory` scans only the loader boot window and the user-image neighborhood so untouched host pages stay zero-backed.
`sel4-kernel-loader` must zero and clean-invalidate (`dc civac`) the compile-time `payload.info.dtb` after `copy_data_out` whenever it is not passed to the kernel.
Otherwise, a `free_index = 0` boot Untyped retains FDT bytes and corrupts later kernel objects.

## Apple VZ memory accounting

Virtualization.framework runs the VM in the XPC service `com.apple.Virtualization.VirtualMachine`.
`launchd` starts the service. Its parent PID is 1, and `proc_listchildpids` for the Carrier does not find it.
macOS charges the guest memory to the service, not to the Carrier.
Find the service as the process with that executable that holds the pool file open.

## l4h memory hotplug

On aarch64, `l4h_pt_map` ignores its level argument and calls `seL4_ARM_PageTable_Map`.
The kernel puts the table at the highest level that the address lacks.
In a new GiB of the direct map, a table for 4 KiB pages can go one level too high.
An unplug revokes that slot.
The map then fails with `seL4_FailedLookup`.
`l4h_hp_map` must first call `map_kernel_at(large[i], va, 1)` for each block that has a 2 MiB aligned address.

## Guest init

`std::process::Command` keeps the descriptors of its `Stdio` values until its drop.
Drop the command after the spawn, or the output pipe of the child never reports its end.
The standard library waits for a child whose `exec` failed.
The reaper must hold the child table lock while it calls `waitpid(-1)`. Otherwise it collects that child first and the spawn panics.

A `/proc/self/fd/N` path names the directory that the descriptor opened, also after a mount on that directory.
Resolve the path again to reach the mounted filesystem.
Move `/proc` into the workload root last. Descriptor paths stop resolving after the move.

Plain dm-crypt does not detect a wrong key.
Only dom0 knows that a disk is empty. The Guest formats a disk only when the boot configuration says so.

Device-mapper loads a writable table on a read-only block device without an error.
Load the table with `DM_READONLY_FLAG`, so the mapping also rejects writes.

## Build and install

A shared data reset fences old install requests before it removes their plans.
Project that exact-request fence when reading status, even if the historical receipt still says retryable.
The fence does not prove reset completion or cloud revocation. Do not infer a missing Registration target.
Old successful installs delete their plan after they write a version 1 completion.

Use that completion as identity proof when converting an unbound historical receipt. Do not require a live plan for success.

Distribution ZIPs omit resource forks and extended attributes.
Default `ditto` archives can emit AppleDouble `._` entries for those attributes.
JavaScript extractors create ordinary files from those entries, which fail the strict code signature check of the bundle.
Keep the signed file bytes and Unix modes in the archive.

The macOS linker of Rust can record `libiconv` even when the service executor does not use it.
Use `-dead_strip_dylibs` when linking the executor.
Use `otool -L` to require only `libSystem`.

Guest assembly pipes cpio into xz. Without cpio, the pipeline can produce a valid empty initramfs.
Keep cpio in the Dockerfile tools stage. An empty initramfs leaves the VM in `STARTING`.

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

A macOS network interception path can accept the system TLS client while other TLS clients fail before HTTP.
Runtime archive downloads use the system curl.
Keep address admission, the pinned destination, and content checks across that adapter.

## Guest network

When the host runs a TUN Fake-IP proxy (`198.18.0.0/15`), upstream DNS returns RFC 2544 addresses.
`dom0` records only DNS-resolved Fake-IPs for `PublicInternet` sandboxes into `@resolved_fake_ips`, with a bounded DNS TTL.
It excludes `198.18.0.0..=2` and `198.19.255.255`. The table matches that set before the bogon drop rule.

## Filesystems and quota

`CreateVolumeRequest` has no size field. A sparse volume can have a logical limit equal to the full Scope disk quota.
For admission, sum the actual bytes used by all named volumes and root overlays in the Scope.
Keep measured usage even when the total exceeds the quota.

Linux openat2 rejects `O_PATH|O_NONBLOCK` with `EINVAL`.
Add O_NONBLOCK only to ordinary beneath descriptors.
An O_PATH socket descriptor cannot open device contents or block.
Keep O_CLOEXEC, no-follow resolution, and the socket type check.

Keep the pool allocation database on the dom0 control filesystem, outside the pool.
An existing pool database needs its original master key.
The dom0 service creates a pool only with `--init-pool`. Never create a key or a pool for a disk that has a database.

An unprivileged macOS `ditto --clone` copy can succeed while changing GIDs to the group of the destination directory.
Compare byte hashes and numeric owners after copying a stopped state root.
Use a copy account with the required ownership permissions, or a destination that preserves the source ownership.

A concurrent fork can inherit the open file description that holds an exclusive `File::try_lock()` lock.
`O_CLOEXEC` closes the child descriptor at exec.
A `Drop` that only closes the parent descriptor can leave the lock held until then.
An immediate reopen can fail with `EWOULDBLOCK`, for example during a concurrent `mkfs.erofs` spawn.
Call `File::unlock()` explicitly in the `Drop` of the lock owner.

## Appliance lifecycle

Keep the launch request and boot nonce stable for retries of one launch.
Include volume ID in launch identity to separate reinstalls. Preserve the complete fingerprint check of the Guest.
Frequent appliance reconciliation must preserve an already-current private controller route and frontend runtime.
Recreating either on each tick changes loopback ports and drops controller sessions.

## Dependency behavior

The macOS data worker has crashed inside the localized interface-name API of SystemConfiguration.
Netwatch discards this optional name after interface enumeration.
The macOS interface adapter reads BSD network facts without SystemConfiguration metadata.
Preserve interface addresses, IPv6 scopes, address flags, and route selection when changing this adapter.

Pakery crypto types implement traits from pakery-core and pakery-spake2.
Update these crates together. Mixed versions can expose incompatible copies of the same trait.

## Offscreen rendering tests

Tests in one process run on parallel threads.
On Linux with Mesa `lavapipe`, the process can stop with `SIGSEGV` in `libvulkan` when each test makes a Vulkan instance and device.
About one run in ten of the `wgpu` atlas tests of `apps/console` stopped this way.
Make one device for the process and share it between the tests.

## macOS secret storage

Development packages and releases without a provisioning profile have no Keychain permissions.
A fresh installation selects file storage when its Keychain probe fails.
Keychain and Secure Enclave need a signed release package, its provisioning profile, and a logged-in user session.
A daemon uses file storage. The Host does not switch an existing Keychain installation to files when Keychain becomes unavailable.
