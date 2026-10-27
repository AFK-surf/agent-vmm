# Contributor invariants

## Compatibility and state

Remove obsolete interfaces and parallel services that exist only for mixed-version availability.
Preserve product files, credentials, workloads, and registrations through supported updates.
Destructive cleanup requires owner approval for the exact scope.
A version difference alone never authorizes deletion, re-enrollment, or workload termination.

## Guest build and supply chain

Build Guest artifacts on Linux arm64 with a case-sensitive filesystem.
Resolve current Alpine stable, APK, kernel, containerd, gVisor, and BuildKit during the Guest build.
Do not pin their versions or input hashes.
Test the resolved runtime set with a real sandbox before publishing its Guest bundle.
Install the complete gVisor release directory.
Remove guest-native runc and `containerd-shim-runc-v2` from the assembled Guest and verify their absence.
Select `linux/arm64` from multi-platform image indexes.
Record the completed Guest bundle and published artifact content identities.
Downloaded dependency hashes do not identify a release.
Link the Host executable with the canonical Guest manifest digest.
Keep SBOMs as CI inventory for concrete dependency or license consumers, outside runtime bundle authority.
The Guest root uses read-only SquashFS. Do not put the build Host resolver file in it.

## Runtime and image builds

Use one independent runsc sandbox for each workload and mark its spec `io.kubernetes.cri.container-type=sandbox`.
Do not hide image incompatibility with a wrapper or a guest-native runc fallback.
Use the fixed supervisor as sandbox init and the user process as a retained exec process.
Do not substitute a delay, CRI pause container, or exit-code rewriting.

Run rootless BuildKit as UID 1000 inside its separate runsc sandbox.
Grant only sandbox-visible `CAP_SETUID`, `CAP_SETGID`, and `CAP_SETPCAP` for sub-ID mapping.
Do not rely on setuid or file capabilities for `newuidmap` and `newgidmap` there.
Use a build-only runsc `root:memory` overlay for the read-only BuildKit root.
Do not restore nested persistent overlays.
Keep `host-uds=create` for appliances' projected private sockets. Never expand it to `open` or `all`.

## Storage

Use one VMMD-owned internal containerd namespace and containerd overlayfs for persistent workloads.
Share immutable lower snapshots by digest.
Keep each workload's writable upper, volume, runsc sandbox, cgroup, and network independent.
Delete only a fenced logical image record. Shared CAS content can remain referenced elsewhere.
Never report that image's logical size as physically reclaimed space.

## Network and VM lifecycle

Send public IPv4 through the fixed Guest TUN and dedicated egress vsock session.
Resolve public names through virtual DNS. Do not restore DHCP or a VZ resolver dependency.
Neither virtual DNS nor the transparent proxy exception grants LAN access.
Reject new public relay connections after VM state enters `STOPPING`.
Permit the internal PrepareShutdown connection during `STOPPING` so Guest cleanup can finish.

A Linux VMM survives Host restart. The replacement Host adopts that running Guest.
A failed startup commit closes admission and egress. It does not stop the Guest.

## Controller fencing

A replacement control connection fences the old command path and preserves accepted allocations and workloads.
Keep accepted Guest Exec terminal observation outside the revocable runtime RPC handler.
Waiting for another controller cannot block session revocation through WaitForHandlers.
Revoke the previous session capability before issuing its replacement for the current allocation.
Keep registration admission separate from connector transport state.
Drain rejects new allocations and preserves existing allocations and sessions.
Only explicit allocation or registration lifecycle authority can retire the owned workloads.
Transport disconnect closes command paths.
Keep appliance activation failures within that installation's authority and preserve cross-installation isolation.

## Capacity and persistent identities

Compare one requested PID and disk contract with VM capacity.
`RESOURCE_CAPACITY_EXHAUSTED` with `stage=vm_capacity` means that the requested contract exceeds VM capacity.
It does not mean other allocations reserved that capacity.
Do not delete another allocation to clear this error.
Bound actual shared usage and each operation without arbitrary stored-workload counts or fixed percentage partitions.

Host-owned service route generations use nonzero signed 64-bit values because SQLite cannot store larger unsigned values.
Keep deterministic request identity within that domain.
External allocation and fencing epochs retain their full unsigned representation.

Read [pitfalls](pitfalls.md), [the termination table](../design/overview.md#what-ends-what), and [testing](testing.md).
