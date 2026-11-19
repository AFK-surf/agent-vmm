# Contributor invariants

## Compatibility and state

Remove obsolete interfaces and parallel services that exist only for mixed-version availability.
Preserve product files, credentials, workloads, and registrations through supported updates.
Destructive cleanup requires owner approval for the exact scope.
A version difference alone never authorizes deletion, re-enrollment, or workload termination.

## Guest build and supply chain

Build Guest artifacts on Linux arm64 with a case-sensitive filesystem.
Resolve Alpine packages and the Guest kernel from current Alpine stable.
Resolve BuildKit during the Guest build.
The release smoke test checks the extracted binaries and excludes the retired runtime.
Select `linux/arm64` from multi-platform image indexes.
Record the completed Guest bundle and published artifact content identities.
Link the Carrier with the canonical Guest manifest digest.
Keep SBOMs as CI inventories outside runtime bundle authority.
The Carrier root uses read-only SquashFS. Keep the build Host resolver file out of it.

## Runtime and storage

Run each workload in its own l4h VM.
Keep immutable image extents separate from writable workload and volume extents.
Release an extent only after the VM detaches it and the owner releases it.
Separate image metadata and pool capacity so pool exhaustion cannot block cleanup.
Report reclaimed space only when the backing storage releases it.

## Network and VM lifecycle

Apply network policies at the dom0 switch.
Keep the Guest resolver on the dom0 DNS service.
Internet access does not grant LAN access.
Preserve the workloads of an adopted VM when its Host control process restarts.

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
