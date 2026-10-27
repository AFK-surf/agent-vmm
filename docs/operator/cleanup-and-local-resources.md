# Cleanup and local resources

## Inspect before deletion

1. Run `agent-vmm inspect --json` for the exact service and target.
2. Record the environment or registration ID, namespace, generation, current revision, and original operation.
3. Preserve a [backup](backup.md) of required data before irreversible deletion.
4. Select a state-preserving update, exact cleanup, or explicitly approved purge.

A normal update preserves identity, files, registrations, and workloads.
A version difference or record count does not authorize deletion.
A stale process or archive requires exact ownership evidence before removal.
Never kill every process with the same name or delete live database rows.

## Environment reset or discard

Preview one exact environment:

```sh
agent-vmm cleanup preview --environment-id ENV --kind discard --json
```

Review its revision, blockers, and complete volume set.
For an active integration, remove that exact integration before environment discard.
The operator API uses BeginAgentIntegrationRemoval and CompleteAgentIntegrationRemoval with one stable request ID.
Failure preserves the removing state and receipt for retry.
Other effective integrations can retain their shared environment.

Execute only the reviewed plan:

```sh
agent-vmm cleanup execute --environment-id ENV --kind discard \
  --plan-revision REVISION --request-id REQUEST \
  --confirm-volume VOLUME --confirm-irreversible --json
```

Repeat `--confirm-volume` for every approved volume.
Reset preserves named volumes. Discard deletes the confirmed objects and owner record.
The Host re-reads the plan under the inventory, remote, and environment fences before acceptance.
The revision binds references, exact generations, and volume confirmations.
Concurrent image imports and resource mutations can delay this read.
A changed or oversized plan cannot authorize partial deletion.

Planning reads for at most 30 seconds and admits at most 100 exact objects.
Use [the retry contract](../reference/errors.md#retry-and-unknown-outcomes) for accepted cleanup and uncertain results.
For failed or interrupted cleanup, inspect the remaining state and obtain a new plan and request ID.

Remote cleanup uses the same owner.
Planning is read-only. Accepted execution revokes capabilities, confirms workload stop, and enters Retained before deletion.
Retirement failure stops deletion and saves a failed receipt.
See [the remote cleanup contract](../protocols/remote.md#cleanup-and-absent-allocations).

## Local registration disposal

The operator can dispose one OwnerLocal environment or one exact external registration without a controller login.
PlanLocalDisposal freezes the target revision and affected count.
ExecuteLocalDisposal requires fresh trusted operator confirmation.
Agent sessions and remote connectors cannot use these methods.
The Host saves its receipt and replay fence before Guest effects.
An unavailable Guest leaves accepted cleanup pending. Host restart resumes that original operation.
The durable fence rejects old enrollment and admission commands after resource rows disappear.
A disposed target reports `LOCAL_DISPOSED`.

Disposal preserves other registrations, shared OCI content, and PersonalMesh identity.
Controller cleanup still requires the owning controller's authorization.
Registration recovery cannot reactivate a disposed target.
Read [registration recovery](../protocols/remote.md#registration-recovery) for the proof contract.

## Disk observations and retained data

GetLocalDiskSummary reads the Guest filesystem total. It does not collect CPU or memory metrics.
Local environment and registration pages contain at most 32 indexed records with explicit cursors.
Private disk details use cached owner samples matched to boot, namespace, and allocation generation.
Samples older than 15 seconds remain unknown.
Import reservations are separate from used bytes.
Namespace deletion does not promise immediate physical reclamation or secure erasure.

An appliance installation selects one private data volume with its own quota.
Stop retains its data. Removal waits for the exact writer and checks active references.
See [the termination table](../design/overview.md#what-ends-what).

After a successful operation, remove only unowned archives, temporary credentials, and obsolete app copies.
Keep pending-operation preparation paths, active state disks, and owner-approved diagnostic evidence.
Never remove an entire home directory or an unresolved target path.
