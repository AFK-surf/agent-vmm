# Contributor pitfalls

These failures explain constraints that the code must preserve.
Recheck dependency-specific behavior when its module version changes.

## Guest runtime

VMMD starts containerd without OpenRC. Set `RLIMIT_NOFILE` before dropping `CAP_SYS_RESOURCE`.
The inherited 4096 limit can exhaust gofer mount caches even after workload descriptors close.
Preserve the capability removal that enforces project quotas.

The workload PID cgroup counts runsc threads and systrap stub processes.
A low limit can make retained exec report `out of memory` or terminate the sandbox during clone.
Check `pids.peak` and `pids.events` with the memory OOM counters before classifying this as memory exhaustion.
Use the standard E2E PID allowance for runtime smoke tests. Test PID exhaustion separately.

Workload processes have an empty capability set.
A root entrypoint cannot initialize a mode-0700 volume owned by another numeric UID.
Start tests and workloads with the matching numeric UID/GID instead of relying on root bypass.
Containerd can retain a `running` label after task reaping.
Use inspectContainer task reconciliation. A missing task means stopped, not raw NotFound or stale running state.

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

Guest assembly pipes cpio into xz. Without cpio, the pipeline can produce a valid empty initramfs.
Keep cpio in the Dockerfile tools stage. An empty initramfs leaves the VM in `STARTING`.

Swift generator builds resolve their own transitive dependencies, even when their source checkout came from the app build.
Copy the app’s `Package.resolved` into each generator package before building it to keep generated output consistent.

## Host store

InstallApplianceDomain must insert its private registration before the installation because registration_id has an immediate foreign key.
Keep both writes in the same transaction. Planning success cannot substitute for this ordering.

## Streams and reports

The MCP ForwardTCP bridge has one sender goroutine.
Wait for both copy directions before CloseSend, or it races an in-flight Send.
`go test -json` can report package failure before a child subtest reaches a terminal event.
The E2E renderer must retain that child as `INTERRUPTED`.
Tests that open the Host WAL database directly need one connection and a busy timeout.
Otherwise forced-stop recovery can surface `SQLITE_BUSY` from an unrelated RPC.

## Image import

A macOS network interception path can accept the system TLS client while Go handshakes fail before HTTP.
Runtime archive downloads use the system curl adapter on macOS. Changing HTTP versions does not repair this TLS failure.
Keep address admission, the pinned destination, and Guest content checks across that adapter.

Host-to-Guest import sends archive chunks synchronously.
Keep MaxImportArchiveChunkBytes, currently 3 MiB, below the Guest's 4 MiB gRPC receive limit.
One-MiB chunks made a 367 MiB archive exceed the ten-minute budget despite a fast origin.

Go-containerregistry preserves Docker schema 2 media types when it writes an OCI layout.
Normalize exports before computing the committed OCI manifest digest.
Preserve compressed blobs and configuration and layer digest checks.
Test the exported archive with the shared OCI validator. Tar entry presence alone is insufficient.

## Guest network

A TUN read can return a non-IPv4 packet.
Reject and count it at the TUN boundary instead of letting WriteFrame tear down the egress session.
Go's file poller can reject `/dev/net/tun` as not pollable after offline VM restart.
Keep packet I/O on blocking unix.Read and unix.Write.
A stopped pump otherwise appears as unrelated runtime or registry failure.

Nftables v0.3.0 sends an atomic batch before reading ACKs and rule echoes.
The default receive buffer overflowed with seven public namespaces.
Keep the explicit per-reconcile receive buffer and five-second deadline.
Run the kernel regression in its private network namespace.
Its sixteen-namespace fixture is not a product capacity limit.

## Filesystems and quota

Linux openat2 rejects `O_PATH|O_NONBLOCK` with `EINVAL`.
Add O_NONBLOCK only to ordinary beneath descriptors.
An O_PATH socket descriptor cannot open device contents or block.
Keep O_CLOEXEC, no-follow resolution, and the socket type check.

Keep control metadata and system private volumes on the control filesystem.
OCI content and system, builder, and workload snapshots belong on the pool filesystem.
Containerd metadata must stay outside the OCI project quota so pool exhaustion cannot block recovery.
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

## Appliance lifecycle

Keep the launch request and boot nonce stable for retries of one launch.
Include volume ID in launch identity to separate reinstalls. Preserve the Guest's complete fingerprint check.
The system namespace survives appliance stop and removal cycles.
Streamed system imports derive request identity from the immutable header.
Bundled images derive it from their digest-bound canonical manifest reference.
A fixed import ID makes later changed content conflict with old receipt identity.

Frequent appliance reconciliation must preserve an already-current private controller route and frontend runtime.
Recreating either on each tick changes loopback ports and drops controller sessions.

## Dependency behavior

The macOS data worker has crashed inside SystemConfiguration's localized interface-name API.
Netwatch discards this optional name after interface enumeration.
The macOS interface adapter reads BSD network facts without SystemConfiguration metadata.
Preserve interface addresses, IPv6 scopes, address flags, and route selection when changing this adapter.

Pakery crypto types implement traits from pakery-core and pakery-spake2.
Update these crates together. Mixed versions can expose incompatible copies of the same trait.

Containerd 2.2.6 can return empty Labels for a nonexistent namespace.
Use List to establish existence. A successful Labels call does not prove it.
Its Docker authorizer retains failed anonymous token results without expiry.
Create a fresh authorizer and resolver for each bounded registry retry.
Recheck both behaviors when go.mod changes containerd.

Code-Hex/vz v3.8.0 contains virtio-socket close wakeup and serial attachment fixes.
Keep the platform tests when changing this module.

Cloning http.DefaultTransport can add h2 to its shared TLSClientConfig.NextProtos.
A custom transport that copies this configuration must set ForceAttemptHTTP2.
Otherwise the server selects h2 while the HTTP/1 client reports a malformed response.
The Linux executable links code that clones the default transport.

Alpine's containerd and CNI plugin executables can report their Go main module version as `(devel)` without a VCS revision.
The notices generator cannot download that value as a module version.
Keep their explicit Apache-2.0 component declarations and collect the versioned dependency modules separately.
The installed APK metadata remains the source of their package versions.

## macOS secret storage

Development packages and releases without a provisioning profile have no Keychain permissions.
A fresh installation selects file storage when its Keychain probe fails.
Keychain and Secure Enclave need a signed release package, its provisioning profile, and a logged-in user session.
A daemon uses file storage. The Host does not switch an existing Keychain installation to files when Keychain becomes unavailable.
