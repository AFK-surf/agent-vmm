# Appliance recovery

An installation owns one verified artifact and one encrypted data volume. Host
state records the current boot ID, container instance, bootstrap ID, connection
epoch, and installation revision. A restart creates a fresh boot/container
instance and revokes the previous runtime capabilities before publishing the
replacement.

An unexpected Virtualization.framework stop is handled once by the running
Host process. It revokes remote sessions, active lease capabilities, and
connector admission before using the same `state.raw`, then restores
appliances, policies, and connectors
under a fresh Guest boot ID. If that attempt fails, or if the recovered VM stops
unexpectedly again, VM status remains `FAILED` with operator action required.
Automatic recovery never formats or replaces the state disk, kills unrelated
processes, restarts the Host, or reboots macOS.

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
`agent-vmm.local/mcp@<digest>` reference must match the Guest manifest. At Host
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

### Host schema 18 to 19 conversion

Schema 19 replaces the workload reservation projection with the allocation
generation on each remote environment. The conversion keeps registrations,
environment and namespace identities, allocation identities, policies, leases,
credentials, volumes, and Guest data. It copies each exact generation from the
schema-18 reservation row, then removes the obsolete reservation table. The
table is a Host admission projection. It is not the owner of workload data.

Use this procedure for a schema-18 Host database:

1. Stop the Host and connector launchd jobs. Confirm that no Host process has
   the database open.
2. Create a restorable SQLite backup, including any WAL content. Record its
   digest and the source schema version.
3. Start the schema-19 Host once. The Host converts the database in one
   transaction before it starts the VM or accepts controller work.
4. Confirm that the Host starts and that every remote environment reports its
   original allocation identity and a nonzero allocation generation.
5. Keep the backup until the controller has reconciled the bounded environment
   inventory.

The conversion copies each exact allocation generation to its environment. It
deletes the retired workload reservation table and obsolete allocation rows.
It preserves fixed system and runtime reservation rows. It also deletes the
retired aggregate-capacity columns. The conversion fails closed if a remote
environment has no exact matching reservation. The transaction rolls back and
leaves schema 18 unchanged. Stop the rollout if this check fails. Restore the
pre-cutover database only if the database becomes unreadable. After schema 19
commits, repair forward. An older Host is not a rollback target.

### Single-filesystem state-disk conversion

The current Guest uses an 8 GiB control filesystem and a second workload
filesystem. An older `state.raw` contains one ext4 filesystem. Convert it while
the Host is offline. The conversion reads the old disk through a read-only VZ
attachment and creates a separate, fully backed target disk. It does not change
the source disk.

Use this procedure:

1. Stop the Host, data worker, and connector jobs. Confirm that no process owns
   `host-owner.lock`, `host.sqlite`, or `state.raw`.
2. Resolve the exact source installation and every product owner whose data is
   on the disk. Do not convert a shared disk if the approved migration batch
   excludes any owner on that disk.
3. Back up `state.raw`, the Host database with its WAL content, keys, and
   controller credentials. Verify that the backup can be read. Keep at least
   the source disk size plus 10 GiB free for the fully backed target.
4. Create an empty instance directory on the same protected volume. Run the
   signed mainline Host executable as the service user:

   ```sh
   /path/to/Agent\ VMM\ Host.app/Contents/MacOS/agent-vmm-host \
     --bundle /path/to/Agent\ VMM\ Host.app/Contents/Resources/Guest \
     --instance /path/to/state-conversion \
     --serial-log /path/to/state-conversion/serial.log \
     --migrate-state-from /path/to/current/instance/state.raw
   ```

5. Require a zero exit status and the final `state migration complete` record.
   The command has a 30-minute bound. It preserves all durable workload data.
   It preserves regular files, sparse extents, hard links, and symbolic links.
   It also preserves special inodes, ownership, modes, timestamps, xattrs, and project IDs. It
   discards `lost+found` and the temporary import, bootstrap-reservation, and
   `/tmp` paths that the old Guest already deletes during startup. It also
   discards migrated system image records and root filesystem snapshots.
   The current Host rebuilds those disposable projections with the native
   snapshotter under the control filesystem's fixed 2 GiB shared-image quota.
   The conversion preserves system private volumes. System appliance writable
   quotas remain within the separate 4 GiB control-filesystem budget.
6. Keep the old disk as the pre-cutover backup. Move the new `state.raw` into
   the original instance directory, then start the current Host. Confirm every
   bounded namespace, private volume, provider home, session, container, and
   allocation identity before accepting new work. The Guest converts legacy
   workload receipt labels to current namespace quota labels. It removes the
   retired receipt labels after it validates the converted limits. The Guest
   restores each namespace quota from those durable labels during recovery.
7. If conversion or validation fails before the new disk accepts work, stop the
   Host and restore the unchanged old disk with the old approved mainline
   release. After new work is accepted, repair forward. Remove the failed target
   only after its serial log and exact failure are recorded. Remove the old disk
   backup after the migration batch passes its data and execution checks.

Removal is revision-fenced. A plan first creates a pending-destroy tombstone;
execute checks the expected revision and domain emptiness, revokes trust and
leases, deletes the encrypted volume only after explicit confirmation, and
commits the terminal removal operation atomically.

## Pre-release and build-identity cleanup

Development and release installations do not share identity or state. A purge
removes the current Agent VMM application-support directory, including
appliance secrets, the device-root key, controller credentials, encrypted
appliance volumes, and VM state. Use it only for an explicitly approved
fresh-identity installation. Use the state-disk conversion above when product
data or identity must survive.

Developer installations older than schema 18 are unsupported. Remove them once, manually,
while Agent VMM is stopped:

1. Boot out `com.afk-surf.agent-vmm` and
   `com.afk-surf.agent-vmm-connect` from the current GUI launchd domain.
2. Delete the old Agent VMM app and any separately copied `agent-vmm-mcp`,
   `agent-vmm-connect`, or data-worker binaries.
3. Delete obsolete Agent VMM launchd plists, connector configuration, MCP client
   configuration, and the old application-support and log directories.
4. Install the current bundle and recreate controller registrations, MCP client
   bindings, and appliance installations from fresh identities.

Do not copy unsupported SQLite databases, credentials, appliance secrets,
device-root keys, or data volumes into the new installation. Production code
converts only the documented schema-18 format.

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

The formal Host bundle contains the managed CLI and fixed service executor.
See the authoritative [command availability table](../cmd/agent-vmm-lifecycle/README.md#command-availability).
Use this workflow for every replacement or cleanup. Keep one operation record
with the resolved values. Do not rely on shell history or an assumed service
user.

1. **Inventory.** Run installed `agent-vmm inspect --json` with the explicit
   service type, service user, and registration ID. Record the exact launchd
   job, installed application, Host PID, VZ PIDs, selected `state.raw`,
   registration ID, and bounded workload or allocation IDs from their current
   owners. Count obsolete records before removal.
2. **Classify.** Select exactly one row from the decision table above. A bundle
   update is state preserving by default. Fewer than 20 obsolete records that
   do not block reconciliation are not a reason to purge or edit data.
3. **Preview.** For reset or discard, run `agent-vmm cleanup preview` for one
   environment. The Host owner reads for at most 30 seconds and admits at most
   100 exact grants, containers, volumes, images, and environment records.
   VMMD uses its startup-rebuilt owner index for bounded container and image
   lookup, and it uses a bounded directory read for volumes. Record the plan
   revision and every volume confirmation. An oversized inventory is an
   actionable failure, not partial authorization.
4. **Update or clean.** For a normal login-agent update, run `agent-vmm update`
   with the exact release ID, HTTPS URL, SHA-256, byte size, and request ID. For
   cleanup, run `agent-vmm cleanup execute` with the preview revision, the same
   environment and kind, the exact volume set, and irreversible confirmation
   when required. The owner re-reads references and generations before it
   mutates. Image imports and network-link changes join the same inventory
   fence. Cleanup can reach its 30-second timeout while an earlier image import
   finishes; wait for that original result, then preview again. A changed plan
   fails closed. A terminal cleanup request only
   replays its stored result. If partial cleanup needs another attempt, preview
   the remaining exact objects and use a new request ID. For a destructive
   reinstall, use the purge helper and do not retain a second disk.
5. **Start once.** Bootstrap the exact launchd job. Do not repeatedly unload and
   reload it while investigating the first failure; preserve the first Host,
   Guest, and controller error and diagnose from that state.
6. **Diagnose once.** Use installed `agent-vmm diagnose`, scoped `logs`, and
   `operation get|follow` for the exact owner operation. Preserve the first
   Host, Guest, and controller error.
7. **Validate from the bottom up.** Verify one Host generation, the selected
   fully-backed disk, Guest readiness, registration connectivity, allocation
   readiness, runtime image/version, authentication projection, then one real
   runtime session. A healthy registration alone is not completion evidence.
8. **Converge control-plane state.** For a control-plane cleanup or destructive
   reinstall, retire the recorded old workloads and registrations through their
   lifecycle owners. Never delete live rows directly. Create fresh identities
   only after the old identities are terminal or explicitly shown not to block
   reconciliation. A state-preserving Host update needs no control-plane
   cleanup unless the preflight independently selected that path.
9. **Remove residue.** After runtime validation, remove only residue that is not
   retained by an incomplete lifecycle operation. A successful update removes
   its retired app. Keep the active app and exactly one active state disk.

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
the launchd job. Do not assume that a system LaunchDaemon runs as root. A
standard headless installation uses label `com.afk-surf.agent-vmm-host`. Its
application is normally under the resolved service user's home directory:

```text
/Users/<service-user>/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app
```

Treat this path as a pattern, not as target discovery. Read the loaded launchd
job and confirm the resolved user, home directory, and executable before use.

A system job does not pass a plist or program path to a privileged script. The
administrator installer copies the formally released
`agent-vmm-service-executor` to `/Library/PrivilegedHelperTools` and records the
non-root owner:

```sh
sudo /Library/PrivilegedHelperTools/agent-vmm-service-executor \
  configure --service-user <service-user>
```

The executor accepts only `--job host` or `--job runner` with the fixed
`install`, `start`, `stop`, `remove`, and `status` actions. For example, use
`sudo .../agent-vmm-service-executor start --job host` after inspect reports
the `service_start` administrator action. It rejects custom labels, paths,
templates, environments, shells, and identities. Run lifecycle repair again
as the service user with the same request ID after the job starts. Root must
not run lifecycle mutations, the Host, downloads, providers, or credential and
data operations.

For a state-preserving update:

1. Select the complete content-addressed Host bundle by release ID, exact HTTPS
   URL, SHA-256, and byte size. Do not assemble a release from loose files on
   the target.
2. Run the managed updater as the non-root service user. It downloads for at
   most 10 minutes, enforces a 4 GiB artifact limit, verifies the exact size,
   digest, release inventory, complete Host layout, and code signature, then
   prepares one hidden sibling. Preparation or cancellation failure does not
   stop or replace the current Host.
3. The lifecycle owner records `stop`, drains the VM, confirms Host and VZ exit,
   then records `replace` and atomically exchanges the complete applications.
   The database, keys, registration, workload volumes, and `state.raw` stay in
   place.
4. For a daemon, use the administrator installer. Lifecycle stops the VM before
   the fixed executor stops the Host job. Retry the same lifecycle request to
   replace the application. This retry first confirms that the job is absent
   and the exact state-disk owner has exited; it does not call the stopped Host.
   The fixed executor then installs the Host job from its protected path. Retry
   the same request once more as the service user to complete recovery. Root
   does not run lifecycle, Host, VM, download,
   credential, or user-data work.
5. After replacement, repair only forward. The retained sibling is the previous
   application, but it is not a runnable rollback target after incompatible
   state can change. Keep it until the same request observes the new service and
   VM as healthy. The owner removes it only after completion. Never retain a
   second state disk as application recovery material.
6. Confirm that exactly one Host owns the operator socket and state disk, the VM
   reaches ready, and the registration reconnects. Exercise an existing
   allocation and runtime session before declaring convergence.

Formatting an ext4 filesystem may discard or deallocate blocks from a sparse
backing file. The LaunchDaemon startup check restores the required fully-backed
existing `state.raw` to its actual file length before the VM starts. The
`--state-size` launch argument (64 GiB by default) is used only when creating a
new disk; it never resizes or rejects an existing disk. Treat a failure of the
backing check as a Host storage problem; do not start the VM on a partially
allocated disk and do not recreate the disk while the existing filesystem still
boots.

If VZ reaches `Running` but VMMD repeatedly resets vsock 4050 before publishing
health, preserve the state disk and inspect Guest startup first. An existing
containerd tree needs bounded metadata headroom while containerd starts; only
after startup recovery finishes may the Guest restore the exact idle project
quota. This symptom alone is not evidence that ext4 is damaged and does not
justify deleting `state.raw`.

VMMD establishes project inheritance on fixed OCI roots before containerd
starts. It does not traverse image content at startup. Imported absolute or
dangling links and special inodes must not be opened for quota inspection;
workload upper directories retain their separate project. The import creation
owner restores inheritance after startup removes incomplete import archives.
Do not repair a startup failure by recursively assigning quota or raising the
quota ceiling.

New writable snapshots assign both overlay upper and work directories to the
workload project. A work directory left in the OCI project causes copy-up to
fail with `invalid cross-device link`. This creation fix does not rewrite old
snapshots during startup. Preserve their workload data and identify the exact
snapshot and both directory projects before planning a bounded repair; do not
reassign lower layers or traverse all snapshots as a startup fallback.

The Host serial file receives Guest output through VZ's output-only file
attachment. VMMD logs the start and successful completion of bootstrap, OCI
storage, containerd, recovery, and listener stages. A failed stage includes a
bounded error and the existing Host operation ID. The lifecycle helper reads a
bounded tail from the current serial log and one rotated segment. A listener
reset by itself does not identify filesystem damage or a provider-auth failure.
Use only a confirmed Guest error to select a recovery operation. A last
successful stage is progress evidence, not a root cause. If the Host log does
not map the lifecycle request to a Host VM-start operation, an older Guest
record is not evidence for the current request.

The initramfs loads `virtio_rng` for the Host's configured entropy device.
Without this module, containerd's synchronous random lease-ID generation can
wait for entropy before making its GC-barrier RPC, exhausting the existing
request deadline. Keep the secure random source and startup deadline; do not
replace the ID generator or enlarge the timeout to hide missing entropy.

System appliance launch errors retain the first runtime stage within the
existing 1024-byte diagnostic bound. `stopped_container_cleanup` identifies
failure to remove the previous stopped container before replacement. This
metadata deletion, like ordinary container deletion, uses the existing bounded
metadata allowance because containerd can allocate copy-on-write pages during
delete. Settlement restores the exact idle quota. The existing slack and
sealed-budget rules still apply; deletion does not bypass those rules.

BuildKit uses the same metadata allowance for its short container/task startup
and final deletion. Running the build and waiting for exit do not hold the disk
budget lock. Its output archive stays in the existing namespace build workspace
until the runtime importer registers content and unpacks it in the runtime
namespace. Copying a logical image descriptor alone cannot establish containerd
content visibility there. The logical tag changes only after materialization;
failure before publication leaves the prior tag usable, and an explicit rebuild
can replace it. A later sync or response failure does not undo publication.
Ordinary archive import retains its conflicting-digest refusal.

The two build slots remain held through output consumption. An admitted build
waits for the single materialization slot within its request deadline and can
cancel while waiting. Its local capacity audit, physical reservation, conversion,
materialization and quota settlement share one disk-budget transaction; metadata
cannot observe an intermediate hold that has consumed all free slack. Native
BuildKit execution stays outside that transaction. A streamed import retains its
existing long-lived hold and admission refusal; this build path does not change
streaming import policy. Real capacity exhaustion still fails closed.

Foreground VM recovery and background appliance reconciliation use the same
observed-instance selection. Host installation candidates are read before Guest
inventory, so a stale missing-instance observation cannot be paired with a
newly committed Host revision. The existing installation lock covers recovery
revision checks, stopping an old exact instance, preparation, and start.
Synchronous VM recovery requires the current installation and published health
to agree on the current boot and container instance before reporting completion.

### Targeted cleanup

Targeted cleanup is the default response to an interrupted update. Resolve an
exact path or PID before removing anything.

- Remove downloaded archives and unowned staging directories after the
  installed bundle is ready. Do not remove the prepared path named by a pending
  or failed update operation; retry that same request first.
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
| The Host and VZ process start, but VMMD never publishes health | The failure occurred before a working VMMD listener; a fresh-disk comparison alone does not identify the cause | Preserve the exact disk and first Guest stage/error. Diagnose storage, containerd, and recovery separately. Do not format or replace the disk based only on a timeout. An explicitly approved reinstall must preserve required workload data and identity recovery material before isolating the old installation. |
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
- exactly one selected `state.raw`, fully backed to its actual file length, with
  obsolete disks and archives removed;
- Host and Guest health are ready and the registration is connected;
- an existing or freshly recreated allocation opens a runtime session;
- control-plane records contain no unintended duplicate active registration or
  workload identities; and
- temporary credentials, debug binaries, extracted bundles, and local hot
  patches have been removed.
