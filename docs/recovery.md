# Appliance recovery

An installation owns one canonical artifact manifest and one persistent data
directory. Host state records the selected image digest, current boot ID, container
instance, bootstrap ID, connection epoch, and installation revision. A restart
creates a fresh boot and container instance. It revokes the previous runtime
capabilities before it publishes the replacement.

The running Host process recovers an unexpected Virtualization.framework stop.
It revokes remote sessions, command capabilities, and connector admission
before it uses the same `state.raw`. Then it restores appliances, policies, and
connectors under a fresh Guest boot ID. The Host retries a failed attempt after 5, 10,
20, and 40 seconds, for at most five attempts in ten minutes. The attempts count
across stops until a recovered VM runs for ten minutes. After the last failed
attempt, VM status is `FAILED` with operator action required. Automatic recovery
never formats or replaces the state disk, kills unrelated processes, restarts
the Host, or reboots macOS.

A Host restart during an admitted Activate or Restart continues that same
operation and request after the Host starts. The Guest launch request and boot
nonce come from the installation, its volume, and the new boot. The operation
settles with the result of that activation. It does not fail only because the
Host restarted.

A Stop request settles a `FAILED` VM as `STOPPED` when the VZ manager has no
machine attachment. The stopped status retains the failure message for
diagnosis. A retained attachment must complete the existing forced-stop
confirmation; a stop failure blocks release replacement.

Bootstrap envelopes are opaque, stored in the service user's mode-`0600` state
directory, bound to the installation and random single-use bootstrap ID, and
deleted after consumption. A replay,
stale instance, or wrong artifact ID fails closed.
`SEALED` records integrity failures, including an appliance image digest mismatch.
Start and restart reject a sealed installation, including pending-request retries.
Stop and revoke preserve the sealed state. Use a current removal plan to remove the installation.
Preserve the exact installation and data directory before diagnosing an integrity failure.

Appliance data lives at `/state/appliance-data/<installation>/<volume>` on the Guest pool filesystem.
An independent project quota enforces `data_volume_bytes`.
Stop and restart retain this directory and do not unmount its backing filesystem.
Restart confirms the exact old writer stopped before launching a replacement.
Typed deletion rejects active references and requires exact installation and volume identity.

Use `agent-vmm appliance remove --installation ID` to view the removal plan.
The output identifies the data to delete, allocation blockers, request ID, and revision.
Preserve a usable backup before approving data deletion.
Resolve the listed allocation blockers through their lifecycle operations.
Execute with `--execute --request-id REQUEST --expected-revision REVISION --confirm-resource RESOURCE_ID`.
Repeat `--confirm-resource` for each approved resource and retain the request ID for retries.

The ordinary `state.raw` file and appliance directories do not encrypt themselves.
Offline device confidentiality requires FileVault or equivalent full-disk encryption on the actual backing disk.
A logical raw-file copy contains plaintext. Encrypt backups independently.
Keep diagnostic disks, volumes, and keys until their owner approves exact deletion.
Startup does not convert or delete appliance data.

The first-party appliance bundled in a signed Host release is verified against
the Guest manifest embedded in the Host executable. Reconciliation stages the
verified artifact as an available update. A staging failure is logged and leaves the Host operator service available. Changing an installed artifact uses
the explicit Update operation and its approved grant.

An installed controller first uses its frozen image digest from the Guest cache.
Cache reuse still creates the namespace image grant and records its logical usage.
Only a cache miss opens the current bundled archive and validates its actual content.
`APPLIANCE_IMAGE_SOURCE_UNAVAILABLE` means that neither source can supply that digest. Update the installation to a supported artifact.
A bootstrap integrity failure stops the exact started instance before the Host seals its installation.

Update persists preparation, draining, stopping, switching, and activation.
Draining disables new registration admission, revokes existing runtime sessions,
waits for their handlers, and then waits for the recovered Host execution ledger
to confirm that every existing execution has settled. It does not stop workload
containers. Unknown executions block the operation until its finite deadline.
Only after draining does Update stop the exact controller and atomically switch
the artifact and grant. Installation identity, registration, and data volume are
preserved. Target bootstrap and health determine success.

A client disconnect leaves Update running. A Host restart resumes the persisted phase while the original deadline remains
valid. A deadline or health failure retains the phase and data. Recover resumes the same operation and original
target and grant with an explicit extended deadline and an exact observation
fence. The operation ID and observed update timestamp identify recovery;
replaying its latest payload returns the receipt, while an older observation or
a different deadline at the same observation is rejected. It does not claim rollback success or replace the original request.

A failed Update continues to own its target until an explicit operator action.
Stop, Revoke, or confirmed Remove atomically interrupts that failed operation and
preserves its phase, receipt, and failure evidence. A running Update remains
exclusive. A sealed or revoked installation cannot Recover into activation;
use these explicit lifecycle actions to leave the failed update.

The package contains `manifest.pb`, the controller archive `image.oci.tar`, and dependency archives under `content/<archive_sha256>.oci.tar`.
The CLI queries content status and uploads missing archives before it submits the lifecycle operation.
Each client stage has a finite deadline, a 30-second receive idle bound, and one-MiB frames.
The two Host-to-Guest archive hops use bounded three-MiB chunks with stream backpressure.
The Host admits one content upload at a time and preserves filesystem headroom.
The Guest uses the existing OCI import slot, capacity reservation, and pool project quota.

The artifact manifest authorizes exact role and digest pairs. One role can authorize several digests.
The controller receives only those dependency archives through a read-only mount at `/opt/agent-vmm/content`.
The signed bootstrap claim supplies each exact archive path and image identity.
A workload cannot access this mount. Controller stop removes its content projection.
Archive caches retain verified metadata. Under storage pressure, staging collects archives that no installation, active content stage, or update references.
Frozen workflows do not create separate Host retention references. Keep their required digests authorized in an installed manifest.
Capacity exhaustion after collection rejects the new stage. Removing a digest from the target manifest makes it unavailable to those workflows.

The managed CLI accepts `appliance install --package DIR --bootstrap-stdin`,
`appliance update --package DIR --product PRODUCT`, `appliance status`, and
`appliance recover --operation ID --timeout DURATION`.
Use `appliance stop --product PRODUCT` to stop a failed Update through the existing operator lifecycle.
Use `appliance start` or `appliance activate` to plan and activate an installation.
Use `appliance restart` to restart it with a current revision fence.
Use `appliance revoke --installation ID` to revoke the controller domain and stop its workloads.
Revoke retains installation data. Each lifecycle command reads the current revision and reports its operation receipt.
The packaged CLI includes these commands. It reads installation
revision, plans, submits the exact request, and watches the durable receipt.

If Install succeeds but activation fails, use `appliance activate --installation ID` to continue.
Do not repeat installation to repair that activation.
An expired saved Remove command first submits its original identity for receipt observation.
An accepted operation keeps its original receipt. A typed `admission=not_accepted` response requires a new plan and approval.
Transport uncertainty does not prove non-admission. Retries never extend the saved payload deadline.
Install and Update accept `--system-pids`, `--system-disk-gib`,
`--environment-pids`, and `--environment-disk-gib`. Each value must be a positive
integer within the manifest limit. Disk values use GiB. Install starts with the
manifest grant; Update preserves the current grant unless ordinary grant changes
are explicitly approved. Resource flags replace only the named fields. Host
planning checks that the proposed grant fits VM capacity. A rejected plan exits
with an error and guidance to adjust disk limits.
Update accepts a target artifact from the installed product. The same artifact
can be used to change its approved grant, including exact Host TCP endpoints.
An unchanged artifact with an unchanged effective grant is rejected. Host-owned
grant revision and list order do not create a change. Grant changes use the same
durable preparation, session revocation, drain, stop, switch, and activation
phases as an artifact update.
The Host publishes controller egress after the exact boot and container instance
are persisted and bootstrap succeeds, before it admits the private controller route.
During activation, the Host holds the VM lifecycle inventory fence through content
preparation, instance creation, and bootstrap. Inventory commands can reach their
deadline while this operation is busy. Read `appliance status` and the lifecycle
receipt to observe progress. The `diagnose` JSON includes `hostHealth`, with egress
policy and dial counters from the existing health summary. The source count includes
denied entries. Revocation denies that source and cancels its old flows in place;
it does not require a new inventory publication or advance the inventory revision.
Host staging logs distinguish transfer to the Guest
from the Host cache; Guest logs identify cache reuse, completed staging, and the
system image GC boundary.
Content staging reports the role, digest, archive size, cache status, bytes sent
at 256 MiB intervals, and the final receipt.
Pass bootstrap seed and its management credentials through stdin or an anonymous
pipe. They must not appear in arguments, environment variables, files, or logs.

### Workspace read diagnostics

Workspace read errors retain their RPC code. Existing owner details retain their original meaning.
For Guest relay errors without details, the Host adds `WORKSPACE_GUEST_RELAY_FAILURE` in the `agentvmm.afk.surf` domain.
Its fixed metadata identifies `hop=guest` and `phase=start` or `phase=receive`.
`start` covers Guest client setup and errors before the first forwarded chunk.
`receive` identifies errors after a forwarded chunk. Host-to-client send errors keep their original status.
`LOCAL_TARGET_UNREADABLE` identifies a Host local-state observation failure. Code 14 alone does not identify a transport failure.
These diagnostics contain no file path, payload, or original error message.

### Managed Gateway connection recovery

The Host first resolves the configured Gateway through the system resolver.
A failed TCP or TLS connection permits another address, then another configured
macOS interface. SystemConfiguration supplies each interface and its DNS servers.
Supplemental DNS domains limit which names use that resolver. The connector uses
only active non-loopback interfaces. It prefers physical interfaces after the
default route fails. It does not install routes or change system DNS.

Each direct connection pass has a 20-second budget. Configuration discovery has
one second. At most one configuration read can remain blocked in configd.
Later passes can skip discovery instead of accumulating blocked readers.
Each route has two seconds for DNS and at most two configured resolvers.
Resolvers share the remaining budget. Each resolver tries UDP, then TCP after
failure. A truncated UDP response also uses standard DNS-over-TCP recovery.

The pass tries TCP and TLS candidates sequentially. Each candidate has three
seconds. Limits are eight candidates, three additional interfaces, and three
addresses per route. Duplicate address/interface pairs are skipped. Delays between
candidates increase from 100 to 200 to 400 milliseconds. The total budget takes
priority, so a pass need not exhaust every candidate. Later gRPC connection
passes retain the existing jittered backoff, capped at 30 seconds.

Only explicit revocation stops the connector: a `RevokeCredential` message or a
revoked local registration reports `CREDENTIAL_REVOKED` and ends the loop.
Controller `Unauthenticated` and `PermissionDenied` responses report
`AUTH_FAILED/CONTROLLER_AUTH_FAILED`. A missing credential or invalid local
connector state reports `CONFIG_ERROR`. These failures use the same capped
backoff. A rejected attempt gets no command authority.

Every candidate uses the original Gateway hostname, configured CA roots, TLS 1.3,
and HTTP/2. DNS and interface selection provide reachability, not authority.
Only stock TLS verification can admit a connection. Bearer credentials and
registration requests follow verification. The route adapter does not retry an
application RPC or replace the installation plan. Explicit HTTP CONNECT proxies
retain the existing gRPC proxy path, including NO_PROXY. Route recovery applies
to direct sockets, including system TUN routes.

If all configured resolvers or routes fail, enrollment remains failed and
recoverable under its existing owner policy. No fixed Gateway IP, public DNS
server, certificate bypass, or persistent route cache supplies a fallback.
A connected TLS test does not prove successful enrollment. The opt-in
`TestGatewayConfiguredEndpoint` checks DNS and TLS without sending credentials.

### Host metadata startup

Startup upgrades supported Host stores to the current schema in one transaction.
It preserves installations, grants, volumes, keys, and operation receipts.
After conversion, the Host enables foreign keys and checks stored references.
`egress_mode` records the approved actual mode. `max_egress_mode` records the allocation ceiling.

Startup rejects unsupported schema versions before conversion.
It also rejects older encrypted appliance storage while installation or volume records remain.
After a rejection, the Host writes `AgentVMM/host-start-failure.json` and exits
with status 0, so launchd does not restart it. The code is
`data_newer_than_host` or `data_requires_conversion`. `agent-vmm-lifecycle
status` reports the record as `hostStartFailure`. Maintenance does not install
a release whose `supportedSchema` does not include the local schema.
Preserve the database, disks, keys, and operation evidence.
Restore a compatible Host and a verified
backup to recover an unsupported installation. A fresh identity installation
requires explicit operator approval and a separate state directory.

### Service route recovery

VM restore observes ordinary source and destination containers before it restores service routes.
Imports and exports retain the approved environment, container ID, and port.
Generation or instance replacement closes old projections and publishes the current target.
A missing container leaves the intent unavailable. Recovery does not create a replacement container.
Transport observation failures remain errors.

The appliance lifecycle restores its service projections after it commits the
current running boot and instance. Controller Stop and Activate preserve service import intent.
Revoke and Remove terminate that intent.
An unavailable LAN interface or occupied port affects only that local listener.

A failed appliance service import affects only that import. The Host logs the failure and leaves its route unavailable.
Transport status codes from one import do not determine appliance health.
Parent cancellation and failures to load the installation, grant, or import list still return to the caller.
The next refresh retries it. Store failures, Guest transport failures, and parent cancellation remain lifecycle errors.
Integrations can share one service environment. Removing one integration preserves the environment while another effective integration remains.
The final integration removal revokes shared routes, listeners, shares, and running intent in the same Store transaction.
Static share changes still require a stopped VM.

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
   The current Host rebuilds those disposable projections with overlayfs in
   the shared image pool.
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
leases, deletes the exact data directory only after explicit confirmation, and
commits the terminal removal operation atomically.

## Pre-release and build-identity cleanup

Development and release installations do not share identity or state. A purge
removes the current Agent VMM application-support directory, including
appliance secrets, the device-root key, controller credentials, appliance data directories, and VM state. Use it only for an explicitly approved
fresh-identity installation. Use the state-disk conversion above when product
data or identity must survive.

For an explicitly approved fresh installation from unsupported developer
state, stop Agent VMM and retain a restorable copy of its application-support
directory before removing obsolete application and launchd files. Install the
current bundle with a separate state directory and recreate registrations and
appliances from new identities. Keep retained data and identity with their
compatible Host until their recovery is complete.

## Host replacement and cleanup runbook

Use the least destructive path that matches the operation. A normal Host
update keeps the registration, Host database, appliance volumes, and
`instance/state.raw`. Do not purge or recreate them merely because the Host
application changed. Use `--purge` only for an explicitly approved fresh
identity cutover, corrupt state that cannot be repaired, or obsolete state that
blocks operation.

Host start converts each live registration with an older numeric protocol
version to the running version, once. A newer version is not changed.
A repeated managed enrollment compares only the registration ID, endpoint,
trust bundle, and scope digest. It updates the display name, source policy
revision, and pool policy of the stored registration. An older source policy
revision, or the same revision with a different pool, returns
`POLICY_REVISION_CONFLICT`.

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
   environment. Rejections show a fixed RPC code and recognized owner reason.
   `AGENT_ACCESS_ACTIVE` requires completed removal of the exact agent
   integration before environment discard. Removal revokes its sessions;
   Guest namespace absence then uses the existing empty-resource discard plan.
   Use `agentd service access list` and `agentd service access remove --integration-id ID --request-id KEY` with the operator socket to select and remove exact service access. Cleanup failures preserve the removing state for retry. Begin and complete requests retain durable, parameter-bound operation receipts.
   The Host owner reads for at most 30 seconds and admits at most
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
   prepares one hidden sibling. The lifecycle record binds the request to the
   release, digest, and size. A retryable preparation failure reports a
   30-second retry time. Retry the same request no more than three times at one
   failed lifecycle stage.
   Preparation or cancellation failure does not stop or replace the current
   Host.
3. The lifecycle owner records `stop`, drains the VM, confirms Host and VZ exit,
   then records `replace` and atomically exchanges the complete applications.
   The database, keys, registration, workload volumes, and `state.raw` stay in
   place.
4. For a daemon, use the administrator installer. Lifecycle stops the VM before
   the fixed executor stops the Host job. Retry the same lifecycle request to
   replace the application. This retry first confirms that the job is absent
   and the exact state-disk owner has exited; it does not call the stopped Host.
   If the disk file is absent, the executor also checks unlinked open files.
   An open disk or a failed inspection blocks the stop.
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

Running the administrator installer again for the same release repairs the
fixed daemon job when necessary. It then runs lifecycle repair as the service
user and requires installed, loaded, readable, and healthy Host status. A
failed first install therefore keeps a deterministic same-release recovery
path.

Formatting an ext4 filesystem may discard or deallocate blocks from a sparse
backing file. The LaunchDaemon startup check restores the required fully-backed
existing `state.raw` to its actual file length before the VM starts. The
`--state-size` launch argument (64 GiB by default) is used only when creating a
new disk; it never resizes or rejects an existing disk. Treat a failure of the
backing check as a Host storage problem; do not start the VM on a partially
allocated disk and do not recreate the disk while the existing filesystem still
boots.

The state disk stays fully preallocated. A sparse disk was tested on a full
host volume: the Guest kernel reported lost writeback, but the workload write
and `sync` returned success. Before an install, `agent-vmm-lifecycle preflight
--json` reports `insufficient_disk` with `requiredBytes` (the new state disk
size plus the 10 GiB host emergency space) and `availableBytes` of the volume
that holds `~/Library/Application Support/AgentVMM`. A Mac with less free space
is not supported; free space before the install.

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

Writable snapshot creation assigns both overlay upper and work directories to
the workload project. A work directory in the OCI project causes copy-up to
fail with `invalid cross-device link`. Startup preserves existing snapshots.
For this error, identify the affected snapshot and both directory projects.
Preserve its workload data and limit repair to that snapshot.
Keep lower-layer ownership unchanged.

The Host serial file receives Guest output through VZ's output-only file
attachment. VMMD logs the start and successful completion of bootstrap, OCI
storage, containerd, recovery, and listener stages. A failed stage includes a
bounded error and the existing Host operation ID. The lifecycle helper reads a
bounded tail from the current serial log and one rotated segment.
`agent-vmm logs --component guest --json` gives the current file priority within
the byte limit, including JSON encoding, and keeps the newest bytes when
output needs truncation. It returns an opaque file-identity and byte-offset
cursor bound to the retained rotation.
Repeat the read with `--after-cursor` to read appended bytes and continue
through the retained rotation. `gap=true` identifies an expired cursor or
bytes omitted by the bounded tail. A listener
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
cannot observe an intermediate hold that has consumed all free slack.
BuildKit execution stays outside that transaction. Its project 9001 cache grant
is recorded in the existing import reservation before quota opens and the physical
hold is released. Other admission subtracts unused cache credit and unused
ordinary namespace hard-quota commitments. Project 9000 retains the metadata
allowance independently. Cancellation retains cache credit until the exact
builder stops and cleanup completes. A streamed import retains its
existing long-lived hold and admission refusal; this build path does not change
streaming import policy. Real capacity exhaustion still fails closed.

Foreground VM recovery and background appliance reconciliation use the same
observed-instance selection. Host installation candidates are read before Guest
inventory, so a stale missing-instance observation cannot be paired with a
newly committed Host revision. The existing installation lock covers recovery
revision checks, stopping an old exact instance, preparation, and start.
Synchronous VM recovery requires the current installation and published health
to agree on the current boot and container instance before reporting completion.

### Linux Host update and recovery

The Linux Host has one fixed system service. Run each lifecycle change as root.
The Host itself runs as the `agent-vmm` user.

| Item | Path |
| --- | --- |
| Releases | `/opt/agent-vmm/<content-id>`, with the `current` link |
| State | `/var/lib/agent-vmm` |
| Logs | `/var/lib/agent-vmm/logs` |
| Unit | `/etc/systemd/system/agent-vmm-host.service` |

For a state-preserving update:

1. Select the release tar by release ID, exact HTTPS URL, SHA-256, and size.
2. Run `sudo /opt/agent-vmm/current/agent-vmm update` with these values and a stable request ID.
3. If the update fails, read the failed stage in the output.
4. Retry the same request ID. Do not use a new request ID for the same release.
5. Make sure that the VM is healthy and that the registration is connected.

The updater writes the tar contents to `/opt/agent-vmm/.<request-id>`. The
lifecycle stops the VM and the unit. It copies the files to the content-ID
directory and checks each file against `release.json`. It switches `current`
in one rename. Then it starts the unit and the VM, and it removes the other
releases. It does not change `/var/lib/agent-vmm`.

If the VMM process holds `instance/vmm.lock` after the unit stops, the update
fails before the switch. Stop that exact process, then retry the same request.
Do not delete `/var/lib/agent-vmm` to recover a failed update.

### Targeted cleanup

Targeted cleanup is the default response to an interrupted update. Resolve an
exact path or PID before removing anything.

- Remove downloaded archives and unowned staging directories after the
  installed bundle is ready. Do not remove the prepared path named by a pending
  or failed update operation; retry that same request first.
- Keep only the current application and, until readiness is proven, one
  previous application. Delete the previous application immediately after the
  new bundle passes the readiness and runtime-session checks.
- Keep the selected `state.raw` and retained diagnostic disks.
  Delete a disk only after its owner approves the exact scope and required evidence is preserved.
- Remove temporary credentials, debug programs, extracted bundles, and hot
  patch archives after validation. Credentials must not appear in commands,
  logs, or the runbook.
- A service restart or Host reinstall does not by itself justify deleting Cue
  registrations or workloads. Retire server-side rows only when they are
  obsolete and their owner authorizes the exact destructive scope.
  Record count alone never authorizes retirement or deletion.
  This cleanup does not require purging a healthy Host.

Do not kill every process named `com.apple.Virtualization.VirtualMachine` and
do not infer ownership from the number of VZ processes. Appliances run as
containers in the shared Guest, not as independent appliance VMs.
Match a survivor to the exact Guest by its open state files and generation.
Give the Host enough time to stop that Guest. A launchd
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
| More than one Virtualization XPC process is visible | Each process must match its owned state file and the current Host inventory. System appliances run as containers in the shared Guest | Match processes by their open state files and Host inventory. Never kill by process name or require a single VZ process. |
| A newly formatted `state.raw` has fewer allocated blocks than its configured size | ext4 formatting issued discard against the sparse backing file | Current builds format with `nodiscard`; the startup fully-backed check restores allocation before boot. If restoration fails, stop before VM start and repair Host storage. |
| The Host and VZ process start, but VMMD never publishes health | The failure occurred before a working VMMD listener; a fresh-disk comparison alone does not identify the cause | Preserve the exact disk and first Guest stage/error. Diagnose storage, containerd, and recovery separately. Do not format or replace the disk based only on a timeout. An explicitly approved reinstall must preserve required workload data and identity recovery material before isolating the old installation. |
| Guest network health and allocation creation report `netlink receive: recvmsg: no buffer space available` | The nftables v0.3.0 client sends a complete batch before reading its ACK and rule-echo responses. The default receive buffer overflows. | Install the approved mainline Host bundle that contains the fixed Guest artifact. Preserve the disk, registration, and volumes. A Guest restart alone does not fix this failure. The kernel regression covers 16 public namespaces, not arbitrary capacity or packet isolation. |
| Runtime image import returns a bare `Unknown desc = EOF` after the archive was sent | The stream ended without a confirmed final import receipt | Preserve the installation and inspect the Host and Guest import result. Retry the same content-addressed import within its operation bounds; archive transmission alone does not confirm success. |

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

## Runtime session replacement

An OpenSession request for the current allocation generation replaces its runtime connection.
The connector closes the old tunnel and confirms exact session revocation before it opens a replacement.
If revocation fails, it retains that session record and rejects replacement.
A later request retries exact revocation. Waiting and revocation obey the request deadline.
Session replacement does not stop the workload or settle an unknown execution.

## Image operation receipts

`ObserveImageOperation` reads the original Build or Copy result.
`CancelImageOperation` cancels that operation and waits for its execution and cleanup.
Both calls require the original request ID and input SHA-256.
Build also requires the original execution ID, container ID, and instance ID.
Copy requires the source and target allocation IDs and generations.
The Host checks the current caller namespace and Copy ownership before the relay.
Observation accepts stopped allocations without relaxing ownership or generation checks.

The Guest stores operation receipts in its existing internal containerd namespace metadata.
The metadata key binds the original actor namespace, operation kind, and request ID.
Deleting the actor namespace does not delete its receipt.
The controller must preserve the receipt before it removes its own allocation records.
A missing image does not establish that an operation failed.
`NOT_FOUND` from operation observation means that no accepted operation record exists.

A receipt reports `RUNNING`, `SUCCEEDED`, `FAILED`, `STOPPED`, or `UNKNOWN`.
`writer_settled` separately confirms execution stop and temporary resource cleanup.
A canceled RPC or an expired deadline does not establish that fact.
Build holds its active owner until the internal task exits and cleanup succeeds.
The Guest sends the Build result only after it saves that terminal receipt.
Accepted execution continues after a client transport disconnect.
Explicit operation cancellation and the accepted operation deadline still apply.

Recovery first reads the original operation and its content-bound image receipt.
A matching successful image receipt can restore a result lost before operation receipt publication.
VMMD startup stops the previous socket-owned containerd process and waits for its pidfd to report exit.
It signals only that daemon, so independent workload shims remain alive.
It accepts the replacement only after the socket belongs to the new process.
Each accepted operation records that daemon lifetime, including the kernel boot, PID, and process start time.
A changed daemon lifetime proves that its old in-process unpack writer stopped.
Startup task cleanup, exact builder cleanup, and synchronous import lease cleanup then settle the operation writer.
Recovery preserves an unavailable business result as `UNKNOWN` with `writer_settled=true`.
It does not convert a missing result into success or safe replay.

Containerd uses the built-in walking diff and overlay snapshotter.
Its archive Apply writes snapshot files inside the daemon.
Decompressor helpers exchange archive bytes through pipes and do not own the unpack target.
The daemon restart therefore settles a lost import handler within the same kernel boot.
Startup preserves live ordinary tasks and FIFO paths, attaches their original main-process IO,
and restores the exit monitor and retained network endpoint.
An abandoned supervisor with no original main process is stopped and reaped by its exact task identity.
If the same daemon still owns an operation without its handler or a successful image receipt,
observation preserves `UNKNOWN` and `writer_settled=false` until VMMD recovery replaces that daemon.
The controller must not delete the source or start a replacement writer from this observation.

Cleanup plans include the current allocation generation.
The Host reads it under the environment lock and preserves the current controller scope.
The connector returns that generation with the plan revision.
A controller can refresh a stale cleanup revision without guessing the allocation generation.
It must reject a different generation before it sends the cleanup mutation.


System image retention follows Host installation and unfinished-update receipts.
The private `ReconcileSystemImages` RPC projects their digest set into existing
Guest namespace labels. Import carries the same set before pruning. Older
installation revisions cannot replace a newer set. Failed unfinished updates
retain their target for recovery; explicit Stop, Revoke, or Remove settles that
update through the existing lifecycle mutation. The normal Host reconcile loop
then releases the obsolete target even if no Guest appliance record remains.
A Guest task reaching RUNNING does not commit an installation update.

Remove retains its pending-destroy operation until Guest data cleanup succeeds.
Guest data cleanup returns `Unavailable` while another operation owns the materializer slot.
Data deletion uses the installation cleanup owner without holding the supervisor registry lock during container or volume I/O.
Other installations remain observable and can accept streams while deletion runs.
It checks that slot before it locks the appliance supervisor.
Other installations can still serve status and frontend connections.
Ordinary container service listeners use a separate path.
Background retries use the original deletion request. All Guest mutations retry failed calls with the same input fingerprint.
The Guest replays successful results from its in-memory cache.
The approved Remove scope includes every allocation and its current volumes in the installation private domain.
Each cleanup attempt retains a child receipt with the parent operation and environment IDs.
The final Store transaction requires an empty domain before it deletes the installation.
Observe the original Remove operation to confirm completion.

System namespace project quotas share the existing 4 GiB control-system budget,
leaving the other half of the 8 GiB control filesystem for trusted containerd
metadata, operation receipts, CNI records, and Guest-tool temporary data.
Bootstrap removes `/control/tmp` before mounting it as Guest `/tmp`; namespace
cleanup removes its CNI and resolver records. Build and repository temporary
files belong to their private workload projects. System task `/tmp` is tmpfs.
This reserve does not bound the lifetime total of trusted audit metadata;
obsolete records and images still require normal lifecycle collection.

System retention projection does not wait on a long materializer. A busy
namespace defers the projection. After projection succeeds, busy or failed GC
returns a typed applied/deferred detail; Host continues lifecycle recovery while
the existing reconciliation retries collection. A stale installation revision
still fails before projection and cannot authorize pruning.

A same-namespace Import whose existing image grant and runtime snapshot are
already materialized acknowledges before writer-slot admission. The namespace
lock protects its complete request receipt and Host retention projection. This
path defers optional collection to existing reconciliation. Missing snapshots
and cross-namespace grants continue through the ordinary materializer and
physical reservation admission.

Host archive collection runs when StageContent needs space. The current
installation and unfinished update manifests retain all approved archive roles.
Stop retains current content. Remove releases those references. A new staged
reference remains protected until its original request deadline. Collection
uses existing stage and lifecycle admission locks; Install and Update check
content availability under the same lifecycle lock before committing a claim.
Collection removes only unreferenced archive and descriptor pairs in bounded
batches, then StageContent checks free space again. A busy lifecycle admission
defers collection. A missing archive can be staged again under the same digest.

### Discard for an absent remote allocation

For an authorized registration and its current connection, a Discard plan for an absent allocation returns an empty plan with generation and revision zero. It contains no volumes or impacts. Execute rechecks absence under the admission lock before it returns Discarded with generation zero. If an allocation now exists, Execute rejects revision zero as PLAN_STALE. Reset and a nonempty volume confirmation do not use this contract.

The Host persists the allocation before it creates Guest resources. It removes the allocation only after Guest disposal is complete. A missing scoped row therefore represents an allocation that was never admitted or has already completed disposal. Consumers must use the successful Plan and Execute receipts; a transport error or an arbitrary NotFound response is not an absence receipt.
