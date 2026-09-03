# Appliance recovery

An installation owns one verified artifact and one encrypted data volume. Host
state records the current boot ID, container instance, bootstrap ID, connection
epoch, and installation revision. A restart creates a fresh boot/container
instance and revokes the previous runtime capabilities before publishing the
replacement.

Bootstrap envelopes are opaque, stored in the service user's mode-`0600` state
directory, bound to the installation and random single-use bootstrap ID, and
deleted after consumption. A replay,
stale instance, wrong artifact ID, lost key, or volume-integrity failure fails
closed; secret-store and encrypted-volume trust failures enter `SEALED`.

Sealed repair is an explicit plan/execute operation. It never formats an
unknown volume and never restores a previous product state. Product updates
that replace an installation use a new installation: install and verify the
new artifact, migrate product data inside the appliance, switch the consumer,
then revoke and remove the old installation.

The first-party appliance bundled in a signed Host release is different. VM
start verifies the full Guest bundle against the manifest digest embedded in
the Host executable. Appliance reconciliation verifies and reads only
`mcp-appliance/manifest.pb` and `mcp-appliance.digest` against that same
manifest; the image digest and exact
`agent-vmm.local/mcp@<digest>` reference must match the Guest lock. At Host
startup, one unique desired installation for that product may atomically move
to the bundled artifact and a stopped revision. The installation ID,
registration, approved grant, and encrypted volume do not change. Multiple
matching installations fail closed, while sealed, disabled, and pending-stop
installations remain unchanged. A desired missing or old-digest runtime is
stopped and started once through the normal artifact, grant, bootstrap, and
instance fences; a failed attempt enters `FAILED` instead of retrying without
a bound.

### Egress-grant schema preflight

An existing schema-17 Host database created before the independent
`egress_mode` column requires one explicit operator update before installing a
Host that reads the column. Stop the Host, take a restorable SQLite backup, and
audit every `appliance_grants` row against the durable approved operation or
grant record that authorized it. The
actual egress mode must come from `ApprovedGrant.egress_mode`; never copy
`max_egress_mode`, which is only the allocation ceiling. Abort if the row count,
installation identity, authorized manifest, or actual mode is not known exactly.

For the bounded staging state where the audit proves every installed appliance
has actual `DENY_ALL` (`1`), the operator may run:

```sql
BEGIN IMMEDIATE;
ALTER TABLE appliance_grants
  ADD COLUMN egress_mode INTEGER NOT NULL DEFAULT 1
  CHECK(egress_mode IN (1, 2));
UPDATE appliance_grants SET egress_mode = 1;
SELECT installation_id, egress_mode, max_egress_mode FROM appliance_grants;
COMMIT;
```

The final `SELECT` must match the audited row set before the new Host starts.
New databases create the non-defaulted, non-null column directly. Agent VMM has
no legacy schema reader, inferred value, or runtime backfill.

Removal is revision-fenced. A plan first creates a pending-destroy tombstone;
execute checks the expected revision and domain emptiness, revokes trust and
leases, deletes the encrypted volume only after explicit confirmation, and
commits the terminal removal operation atomically.

## Pre-release and build-identity cleanup

Development and release installations do not share identity or state. Before
switching build identity, uninstall the current app with `build/host/uninstall.sh
--purge`, then install the newly signed bundle. Purging removes the current Agent
VMM application-support directory, including appliance secrets, the device-root
key, controller credentials, encrypted appliance volumes, and VM state. Product
data must be exported by the product before this step; the host does not migrate
it.

Older developer installations are unsupported. Remove them once, manually,
while Agent VMM is stopped:

1. Boot out `com.afk-surf.agent-vmm` and
   `com.afk-surf.agent-vmm-connect` from the current GUI launchd domain.
2. Delete the old Agent VMM app and any separately copied `agent-vmm-mcp`,
   `agent-vmm-connect`, or data-worker binaries.
3. Delete obsolete Agent VMM launchd plists, connector configuration, MCP client
   configuration, and the old application-support and log directories.
4. Install the current bundle and recreate controller registrations, MCP client
   bindings, and appliance installations from fresh identities.

Do not copy old SQLite databases, credentials, appliance secrets, device-root
keys, or data volumes into the new installation. Production code does not detect,
migrate, or read these formats.

## Host replacement and cleanup runbook

Use the least destructive path that matches the operation. A normal Host
update keeps the registration, Host database, appliance volumes, and
`instance/state.raw`. Do not purge or recreate them merely because the Host
application changed. Use `--purge` only for an explicitly approved fresh
identity cutover, corrupt state that cannot be repaired, or obsolete state that
blocks operation.

Choose the path before making changes:

| Condition | Path | State and control-plane records |
| --- | --- | --- |
| New Host bundle, otherwise healthy installation | State-preserving update | Keep the registration, workloads, Host database, keys, and `state.raw`. |
| One stale process, archive, previous application, or failed temporary patch | Targeted cleanup | Remove only the resolved stale object. Keep the registration and workload identities. |
| More than 20 obsolete control-plane records, or a smaller bounded set that blocks reconciliation | Control-plane cleanup | Keep the healthy Host and disk. Retire only the obsolete registrations/workloads through Cue lifecycle owners. |
| Unrecoverable disk/database/key state or an approved fresh-identity cutover | Destructive reinstall | Purge the installation, then explicitly retire the bounded old registrations/workloads and enroll fresh identities. |

Do not turn an ordinary update into a destructive reinstall to simplify the
procedure. Conversely, do not repair an unrecoverable pre-release installation
with database edits merely to preserve an identity that has no product data.
Never reboot macOS as a cleanup step; stop the exact launchd job and its exact
processes instead.

### Standard cleanup workflow

Use this workflow for every replacement or cleanup. Keep one operation record
with the resolved values; do not rely on shell history or an assumed service
user.

1. **Inventory.** Record the target host, launchd domain and label, service
   user, installed application path and release ID, Host PID, VZ PIDs, selected
   `state.raw`, registration ID, and the bounded workload/allocation IDs. Count
   obsolete registrations and workloads before deciding to remove any of them.
2. **Classify.** Select exactly one row from the decision table above. A bundle
   update is state preserving by default. Fewer than 20 obsolete records that
   do not block reconciliation are not a reason to purge or edit data.
3. **Quiesce.** Boot out the exact launchd job once. Wait through its configured
   graceful shutdown interval, then verify the Host and only the VZ processes
   that own this installation have exited. Do not reboot macOS and do not kill
   every process with the Virtualization XPC name.
4. **Clean or replace.** Remove only the objects authorized by the selected
   path. For a normal update, atomically replace the complete Host application
   and retain the existing database, keys, registration, and `state.raw`. For a
   destructive reinstall, use the purge helper and do not retain a second disk.
5. **Start once.** Bootstrap the exact launchd job. Do not repeatedly unload and
   reload it while investigating the first failure; preserve the first Host,
   Guest, and controller error and diagnose from that state.
6. **Validate from the bottom up.** Verify one Host generation, the selected
   fully-backed disk, Guest readiness, registration connectivity, allocation
   readiness, runtime image/version, authentication projection, then one real
   runtime session. A healthy registration alone is not completion evidence.
7. **Converge control-plane state.** For a control-plane cleanup or destructive
   reinstall, retire the recorded old workloads and registrations through their
   lifecycle owners. Never delete live rows directly. Create fresh identities
   only after the old identities are terminal or explicitly shown not to block
   reconciliation. A state-preserving Host update needs no control-plane
   cleanup unless the preflight independently selected that path.
8. **Remove residue.** After runtime validation, delete the previous app,
   obsolete disk, downloaded archive and parts, extracted bundle, temporary
   credentials, debug binaries, and hot-patch objects. Keep the active app and
   exactly one active state disk.

Use this compact operation record in an issue, deployment log, or scratch file
outside the repository. Do not put credentials in it.

```text
target / launchd domain / label:
service user / installed release:
Host PID / installation-owned VZ PIDs:
selected state.raw path / size / allocated blocks:
registration ID:
workload and allocation IDs / obsolete record count:
chosen path and reason:
first exact failure:
validation: disk / guest / registration / allocation / runtime / real session
removed residue:
```

Before changing files, resolve the service user and installed application from
the launchd job. Do not assume that a system LaunchDaemon runs as root. The
standard headless installation uses service user `bftvmm`, label
`com.afk-surf.agent-vmm-host`, and this application:

```text
/Users/bftvmm/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app
```

For a state-preserving update:

1. Download or copy the complete content-addressed Host bundle. Prefer the R2
   CI intermediate for large bundles. Do not assemble a release from loose
   files on the target.
2. Verify that the application contains its Guest bundle and helpers. The Host
   executable must be linked with the SHA-256 of that exact Guest
   `manifest.json`; plain `go build ./cmd/agent-vmm-host` is not a valid Host
   replacement.
3. Unload the launchd job and wait for both the Host process and its
   Virtualization XPC process to exit before replacing the application. A VZ
   process can be reparented to PID 1 and shown with user `launchctl`, so a
   cleanup check filtered only by the service UID is insufficient. Identify a
   survivor by its executable and open Guest/state files. Force termination
   only after the bounded graceful stop fails.
4. Replace the application without exposing a partial bundle to launchd, then
   restore service-user ownership. Keep at most one previous application until
   the new Host reaches ready; remove it and the downloaded archive after
   validation. Never retain a second state disk as an application rollback
   artifact.
5. Use the release signature in production. A staging hot patch may be ad-hoc
   signed only when that exception is intended. Sign the main executable with
   `com.apple.security.virtualization`, sign nested code, then verify the whole
   application with `codesign --verify --deep --strict`. Inspect the final main
   executable entitlement: signing the outer application after replacing a
   nested binary can otherwise remove or invalidate it.
6. Load the launchd job once. Confirm that exactly one Host owns the operator
   socket and state disk, the VM reaches ready, and the registration reconnects.
   Exercise an existing allocation and runtime session before deleting the
   previous application.

Formatting an ext4 filesystem may discard or deallocate blocks from a sparse
backing file. The LaunchDaemon startup check restores the required fully-backed
`state.raw` size before the VM starts. Treat a failure of that check as a Host
storage problem; do not start the VM on a partially allocated disk and do not
recreate the disk while the existing filesystem still boots.

### Targeted cleanup

Targeted cleanup is the default response to an interrupted update. Resolve an
exact path or PID before removing anything.

- Remove downloaded archives and staging application directories after the
  installed bundle is ready. Large archives should travel through the R2 CI
  intermediate namespace; small diagnostics may be copied directly.
- Keep only the current application and, until readiness is proven, one
  previous application. Delete the previous application immediately after the
  new bundle passes the readiness and runtime-session checks.
- Keep one active `state.raw`. Delete a pre-recreate or failed-new disk as soon
  as the selected disk boots and its workloads are verified. Never retain a
  32-GiB disk merely as an application rollback artifact.
- Remove temporary credentials, debug programs, extracted bundles, and hot
  patch archives after validation. Credentials must not appear in commands,
  logs, or the runbook.
- A service restart or Host reinstall does not by itself justify deleting Cue
  registrations or workloads. Retire server-side rows only when they are
  obsolete and either exceed 20 records or prevent authoritative
  reconciliation. This cleanup does not require purging a healthy Host.

Do not kill every process named `com.apple.Virtualization.VirtualMachine` and
do not require a single VZ process: the main Guest and a system appliance may
legitimately use separate VZ XPC processes. Match a survivor to the main Guest
by its open state files and generation; match an appliance through the Host's
appliance inventory. Give the Host enough time to stop both. A launchd
`ExitTimeOut` shorter than the Host's bounded Guest shutdown forces a dirty VM
stop and can turn a routine update into a slow filesystem recovery. Only after
that complete graceful interval may the exact stale survivor be terminated.

For an explicitly approved destructive reinstall, first use the trusted
release helper through `build/host/uninstall.sh --service-type daemon
--service-user SERVICE_USER --purge`. Bound and record the registrations,
workloads, installations, application directories, and disks that will be
removed before execution. After the helper finishes, confirm that the exact
launchd job, Host process, VZ process that owned the state disk, application
support directory, and old application are gone. Do not use recursive deletion
against a home directory or unresolved variable. Install the complete new
bundle, enroll a fresh registration, and recreate product workloads from their
authoritative Cue state.

Control-plane cleanup follows, rather than precedes, the Host purge:

1. Record the exact old registration IDs and workload/allocation IDs.
2. Revoke or retire them through their owning Cue lifecycle API. Do not delete
   database rows directly while a connected Host or reconciler can still write
   them.
3. Confirm the old registration is disconnected and the old workloads are
   terminal. Remove only the bounded obsolete records; do not bulk-delete a
   tenant or project.
4. Enroll the new Host and recreate Codex, Pi, Claude, and Meeting workloads
   from Cue. Verify their provider, runtime digest/version, and registration
   before considering the cleanup complete.

Direct database repair is a staging-only last resort after runtime owners are
stopped and the affected rows are fully bounded. Record the query and row count
before mutation, use one transaction, and verify the resulting lifecycle state
before restarting writers. A successful one-off repair is not a new supported
cleanup path; add the underlying defect and durable remedy to this runbook or
its linked lifecycle documentation.

If a replacement stalls, preserve the first exact error before retrying. Check,
in order: final executable entitlement; embedded Guest manifest digest versus
the installed Guest; a VZ process left by the previous Host; state-disk
ownership; VM transition and Guest health; registration connection; allocation
lease; runtime image import. Update this runbook when a new repeatable failure
and its bounded remedy are confirmed.

Use this table for confirmed, repeatable failures. Add a row when investigation
establishes both the cause and the bounded remedy; do not record guesses or
one-off shell history as an operational procedure.

| Symptom | Confirmed cause | Bounded remedy |
| --- | --- | --- |
| A routine update leaves the VM recovering slowly or unable to resume | launchd terminated the Host before its bounded Guest shutdown completed, leaving the exact VZ process or disk dirty | Set `ExitTimeOut` to at least the Host shutdown bound, unload the job once, wait for the exact Host and state-disk-owning VZ process, and terminate only that survivor after the bound expires. Do not reboot. |
| More than one Virtualization XPC process is visible | The main Guest and a system appliance can each own a legitimate VZ XPC process | Match processes by their open state files and Host inventory. Never kill by process name or require a single VZ process. |
| A newly formatted `state.raw` has fewer allocated blocks than its configured size | ext4 formatting issued discard against the sparse backing file | Current builds format with `nodiscard`; the startup fully-backed check restores allocation before boot. If restoration fails, stop before VM start and repair Host storage. |
| The Host and VZ process start, but the VM never publishes a boot ID or VMMD health; the same bundle boots with a fresh state disk | The selected state disk contains Guest state that prevents the Guest from completing boot; this is disk-specific rather than a connector, registration, or Host-process failure | Preserve the first timeout and exact disk identity, stop only the Host job, move the blocking disk aside, and allow one fresh fully-backed disk to initialize. Keep the registration initially; reconcile workloads from Cue. After Pi/Codex runtime validation succeeds, delete the blocking disk immediately. Do not reboot macOS or keep the second 32-GiB disk. |
| Runtime image import returns a bare `Unknown desc = EOF` after the archive was sent | The Host treated stream EOF during the archive-chunk stage as a transport failure instead of receiving the Guest's final result | Install a Host containing the chunk-stage `CloseAndRecv` fix, then retry the same content-addressed import. Do not recreate the disk or registration for this error. |
| An identical appliance block configuration fails while the VM is starting or running | A repeated reconciler request reached a non-idempotent state guard | Use a Host that accepts an identical configuration as a no-op; differing block configuration still fails closed and requires the owning lifecycle to stop the VM first. |

### Keeping this runbook current

Update this document in the same change that fixes a repeatable cleanup or
replacement failure. A new table row must contain all three facts: an operator
visible symptom, a reproduced cause, and the smallest validated remedy. Also
update an existing row when the owner, command, path, or remedy changes; delete
it when the failure is no longer possible. Do not preserve temporary hot-patch
instructions after the released bundle contains the fix.

If investigation finds only a one-off environmental failure, record it in the
operation log, not here. If the remedy requires a new state owner, background
scanner, compatibility reader, database repair path, or destructive default,
stop and obtain an architecture decision instead of expanding this runbook.

The operation is complete only when all applicable checks pass:

- one loaded launchd job, one Host generation, and no stale VZ owner;
- the installed bundle, embedded Guest manifest, signature, and entitlements
  form one coherent artifact;
- exactly one selected `state.raw`, fully backed to the configured size, with
  obsolete disks and archives removed;
- Host and Guest health are ready and the registration is connected;
- an existing or freshly recreated allocation opens a runtime session;
- control-plane records contain no unintended duplicate active registration or
  workload identities; and
- temporary credentials, debug binaries, extracted bundles, and local hot
  patches have been removed.
