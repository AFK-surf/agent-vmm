# Remote controller protocol

The public wire owner is `api/remote/v1/remote.proto`.
Controllers use [the Go controller SDK](../../controller/remote) for stream validation and virtual connections.
`ControlSession` and `AcceptSession` own stream decoding. Integrations must not copy a private decoder.

## Registration and enrollment

Enrollment binds one registration, one device identity, and a one-time token.
The controller consumes the token with its durable membership credential commit.
The connector stores the returned bearer credential under that registration's selected secret backend.
The Host verifies the managed trust bundle before it admits the registration.
A failed verification removes the new bearer credential through that backend.

Ready selects enabled features from the supported set.
A remote-compute connection cannot observe local environments or another registration.

## Connection epoch and command fence

Each accepted connection has a nonzero unpredictable epoch and an ordered command sequence.
Hello carries bounded inventory and its watermark. Ready acknowledges that exact watermark and epoch.
The command carries its ID, sequence, epoch, relative `timeout_millis`, target revision.
The receiver measures the timeout from local receipt. It does not compare controller and Host wall clocks.
A zero or unrepresentable timeout fails before dispatch.

The controller claims the command's durable delivery slot before it sends it.
`ControlSession` validates and sends that exact identity. It does not allocate another sequence.
An exact next sequence consumes its slot even when timeout or target-fence validation rejects the command.
A stale epoch or sequence gap does not consume the slot.
The connector sends admission, execution, and terminal evidence for that identity.
A replacement connection fences the old command path.

## Idempotency and unknown outcome

A completed duplicate in the same epoch can reuse the bounded connector result cache.
It cannot authorize a new external side effect.
A deterministic refusal before dispatch reports `REJECTED`.
Deadline, cancellation, transport loss, and uncertain post-dispatch failures report `UNKNOWN`.
A recognized reason cannot turn an uncertain committed result into rejection.
The controller reads authoritative state before it decides another action.

An allocation projection failure can contain the allocation that the Host already accepted.
The controller must settle that allocation before it considers the attempt complete.
Ensure with the same identity and unchanged inputs reuses the allocation.
It does not change that allocation to a new policy through retry.

## Admission and draining

See [capacity errors](../reference/errors.md#retry-and-unknown-outcomes) for admission failures.
Draining rejects new allocations and preserves existing allocations and sessions.
See [what ends what](../design/overview.md#what-ends-what).

## Runtime session replacement

OpenSession for the current allocation generation replaces its runtime connection.
The connector first closes the old tunnel and confirms revocation of that exact session.
Failed revocation preserves the session record and rejects replacement.
A later request retries that revocation within its deadline.
Replacement does not stop the workload or settle an unknown execution.

## Image operation receipts

ObserveImageOperation reads the original Build or Copy result.
CancelImageOperation requests cancellation and waits for execution and cleanup.
Both use the original request ID and input SHA-256.
Build also binds the original execution, container, and instance.
Copy binds source and target allocation IDs and generations under the same registration.
Observation permits stopped allocations while preserving ownership and generation checks.

The Guest retains receipts in its private containerd metadata namespace.
The key binds actor namespace, operation kind, and request ID.
Deleting an actor namespace does not delete that receipt.
A controller preserves the receipt before it removes its own allocation record.
A missing image does not prove operation failure.
`NOT_FOUND` from observation means no accepted operation record exists.

A receipt reports `RUNNING`, `SUCCEEDED`, `FAILED`, `STOPPED`, or `UNKNOWN`.
`writer_settled` independently confirms writer exit and temporary resource cleanup.
A canceled RPC or expired deadline does not prove settlement.
Accepted execution survives a client disconnect. Its accepted deadline and explicit cancellation still apply.
The Guest publishes the terminal result only after it saves the receipt.

Recovery can recover a lost result from an exact content-bound image receipt.
VMMD replaces its previous socket-owned containerd process and confirms its exit before accepting the replacement.
It preserves independent ordinary workload shims.
The recorded daemon lifetime proves whether an old in-process unpack writer can still run.
Exact builder and lease cleanup must also settle before `writer_settled` becomes true.
Unavailable business results remain `UNKNOWN`, even after writer settlement.
An unsettled unknown result does not authorize source deletion or a replacement writer.

## Cleanup and absent allocations

Cleanup planning is read-only. Its revision binds the current allocation generation and exact namespace.
Execute confirms the current volume set under the admission fence.
After durable acceptance, the Host revokes capabilities and confirms workload stop before deletion.
Reset retains named volumes and the allocation. Discard removes the confirmed resources and owner record.
A retirement failure stops deletion and retains a failed receipt.

For an absent allocation, an authorized Discard plan has revision and generation zero with no impacts.
Execute rechecks absence under admission before it returns Discarded with generation zero.
If the allocation now exists, revision zero fails as stale.
Reset and nonempty volume confirmations cannot use this path.
An arbitrary NotFound or transport error is not an absence receipt.

## Registration recovery

`SignRegistrationRecovery` signs only the fixed purpose `agent-vmm/registration-recovery/1`.
The controller supplies a single-use challenge that expires within 120 seconds.
The Host signs the SHA-256 of this UTF-8 wire, in order:

1. The purpose and a newline.
2. `audience`, `subject`, `session_id`, `controller_binding`, `environment_id`, `operation_id`, `registration_id`, and `nonce`.
3. Decimal `revision` and decimal Unix-second `expires_at`.

Each value in step 2 uses unpadded base64url and ends with a newline.
Both decimal fields also end with a newline.
The nonce decodes to 32 bytes. Revision is nonzero.
`controller_binding` is opaque to the Host.
The controller checks the original subject, scope, revision, and challenge consumption.
Recovery changes the delivery target while preserving registration, allocations, and files.
Local disposal disables proof for the disposed target.
The [wire fixture](../../internal/host/trust/testdata/registration_recovery_wire.json) records this exact encoding.
