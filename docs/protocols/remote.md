# Remote controller protocol

The public wire owner is `api/remote/v1/remote.proto`.
The Host connector is [`vmm-remote-salix`](../../crates/vmm-remote-salix).
Controllers generate their stream decoders from this file. Integrations must not copy a private decoder.

## Protocol window

The controller defines integer `ProtocolVersion=4` and `MinProtocolVersion=4`.
Hello sends the version as a decimal integer string.
The controller accepts the inclusive range between those values.
Versions 1 through 3 are unsupported. The first window contains only version 4.
A later window can retain the previous version only when its message and operation semantics remain implemented.
The window does not promise that an older controller can interpret a newer Host.

An unsupported or malformed version returns gRPC `FailedPrecondition` with `google.rpc.ErrorInfo`.
Its domain is `agent-vmm`, and its reason is `REMOTE_PROTOCOL_UNSUPPORTED`.
Metadata contains `host_protocol`, `min_protocol`, `max_protocol`, and `host_side` (`too_old` or `too_new`).
Malformed versions use `too_old`. Controllers must preserve these details through their gRPC boundary.
Registration identity, connection epoch, and command target checks retain their existing owners.

A connector that recognizes this refusal starts with its existing 30-second capped backoff and jitter.
The refusal does not revoke credentials, change admission, or stop independent children and accepted work.
Registration `ConnectorStatus.remote_protocol` records only the current refusal.
A successful connection or a later status without this refusal clears it.
The Host keeps this diagnostic in memory. After Host restart, it remains absent until another refusal arrives.
An older Host does not acquire these diagnostic fields through a server update.

## Registration claim

The product creates a pending Registration after it verifies the current product owner.
The Host saves one random candidate credential, its identity, the service origin, and the exact request before claim.
A TLS claim atomically binds that Host and candidate to the Registration.
The response contains public Gateway, TLS, and pool configuration. It contains no new secret.
`ApplyRemoteRegistration` saves the candidate in the selected secret backend and starts the control connection.
Product connection setup does not install managed route trust.
Independent appliance and route authorities keep their signature and expiry checks.

An identical candidate and Host can observe the committed claim after ticket expiry.
Explicit revocation and current credential admission still apply.
Observation does not enable a stopped Registration or change policy and connection epochs.
The native source request projection preserves the original receipt during one-time local plan conversion.

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

`SignRegistrationRecovery` signs only the fixed purpose `agent-vmm/credential-replace/1`.
The controller supplies a single-use challenge that expires within 120 seconds.
The Host signs the SHA-256 of this UTF-8 wire, in order:

1. The purpose and a newline.
2. `audience`, `subject`, `session_id`, `controller_binding`, `environment_id`, `registration_id`, `candidate_credential_hash`, and `nonce`.
3. Decimal `revision` and decimal Unix-second `expires_at`.

Each value in step 2 uses unpadded base64url and ends with a newline.
Both decimal fields also end with a newline.
The nonce decodes to 32 bytes. Revision is nonzero.
`controller_binding` is opaque to the Host.
The candidate hash uses standard base64 of SHA-256 over the candidate credential bytes.
The controller checks current product permission, the original root, revision, and challenge consumption.
Recovery replaces the credential while preserving the Registration, allocations, and files.
An absent local row can use the original root. Explicit revocation and local disposal disable proof for that target.
The [wire fixture](../../internal/host/trust/testdata/registration_recovery_wire.json) records this exact encoding.
