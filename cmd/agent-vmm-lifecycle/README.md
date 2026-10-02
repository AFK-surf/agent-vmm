# Local lifecycle coordinator

## Shared Host maintenance

`maintenance` is the local operator entry for the whole shared Host. It supports
update with data preserved, reinstall with data preserved or reset, and uninstall
with data preserved or reset. These operations interrupt all BFT and Comma work
in the VM. They do not grant cloud permissions or revoke cloud registrations.
Exact environment or registration disposal remains a separate operation.

Run the helper from a verified bundle outside the installed app and Host data.
This also works when the installed helper is absent or too old. The source bundle
must keep its normal `Agent VMM Host.app` name. Verify the downloaded archive's
SHA-256 and size before extracting it. Retain that bundle for failed-operation
recovery; the native owner verifies its signature and selected release again.
The archive fields bind the selected artifact to the request; they do not replace
the caller's archive verification.

```sh
"$verified_app/Contents/Helpers/agent-vmm-lifecycle" maintenance --service-type auto < request.json
"$verified_app/Contents/Helpers/agent-vmm-lifecycle" maintenance-status < status-request.json
```

The maintenance JSON request has these fields:

| Field | Contract |
| --- | --- |
| `version` | `1` |
| `requestId` | Stable lifecycle request ID; reuse it for every retry |
| `action` | `update`, `reinstall`, or `uninstall` |
| `dataPolicy` | `preserve` or `reset`; update permits only preserve |
| `targetApp` | Service user's `~/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app` |
| `sourceApp` | Separate, canonical verified bundle; omit for uninstall |
| `targetReleaseId`, `artifactSha256`, `artifactSize` | Selected release, lowercase archive SHA-256 and bytes; omit for uninstall |
| `confirmSharedHost` | `true` after a trusted local whole-Host confirmation |
| `confirmDataDeletion` | `true` for reset only |

The owner rejects reuse of a request ID with another action, data policy, target,
source, artifact, or service identity. It does not switch targets from a developer
source path. Preserve retains Host data, volume contents, metadata, credentials,
root identity, registration records, and pairing policy. It does not preserve
running processes or restore cloud access. Reset removes the service user's fixed
`AgentVMM` data directory and `~/Library/Logs/AgentVMM`, including their keys and
bindings, only after the shared process stops and releases the disk. Reset does
not erase unrelated applications, the source bundle, or the maintenance journal.

The JSON receipt includes the request fields, `serviceMode`, `serviceUser`,
`stage`, `outcome`, `retryable`, timestamps, and completed effect checkpoints.
Stages are `accepted`, `stopping`, `resetting`, `removing`, `replacing`, `restoring`,
`verifying`, and `complete`. Outcome is `pending`, `failed`, or `succeeded`.
`failureCode=maintenance_incomplete` and `failureMessage` explain an accepted
operation's failed stage. A process crash can leave `pending`; it does not prove
that the effects failed or completed. Retry the same complete request.

The journal and flock live in `~/Library/Application Support/Agent VMM Maintenance`:
`owner.lock`, `state.json`, and `request-<sha256(requestId)>.json`. They survive app
removal and data reset. Status accepts `{ "requestId": "..." }`, or `{}`/empty
input for the latest receipt. It returns `null` when none exists. Status does not
create directories, read Host data, start a service, or need an installed app.
Other applications must use separate journal filenames or subdirectories.

The owner first attempts a graceful VM stop. An old or damaged Host can instead
be stopped through its exact launchd service. Process exit and disk release must
both be confirmed before reset or replacement. An unknown process or disk owner
leaves the operation incomplete. After replacement, retries restore and verify
the installed release. If that bundle is missing or damaged, the same request
stops the Host and replaces it from the frozen source without repeating reset. A same-version
reinstall still replaces the bundle. An uninstall never calls restore.

Daemon operations keep the existing administrator boundary. Run maintenance as
the service user. If it reports that a fixed system job must be stopped, removed,
or installed, the administrator uses `agent-vmm-service-executor` and then resumes
the same request. Do not run the maintenance business operation as root.

`state.json` has `version: 1`, optional `activeRequest` and `latestRequest`,
`uninstalled`, `completedRequests` (settled maintenance request IDs), and
`blockedInstalls` (managed install IDs). Both maps use ID keys with value `true`.
The completion state atomically clears the active request and records settlement.
A verified-effects checkpoint lets a failed completion write retry only settlement.
Status reports success only from committed owner state, including a crash before
the receipt projection was written. Historical requests cannot repeat effects.
Normal
lifecycle mutations reject active maintenance or an uninstalled Host. Explicit
maintenance reinstall permits new setup. Preserve uninstall uses this temporary
fence; after explicit preserve reinstall, retained bindings may resume under their
existing authorization. Reset records locally known managed install IDs and rejects
their replay after reinstall, including a retry
that changes its outer request ID. The shipped installer checks maintenance
before copying or starting software. Consumers must treat unreadable or unknown
policy as unavailable, not as an absent fence. The latest BFT runner must honor
this same policy before its own copy, install, and control operations.

This fence does not constrain arbitrary old binaries or a local operator who
rewrites files directly. A descriptor that was never delivered locally has no
issue timestamp, so this owner cannot infer that it predates maintenance. Product
owners must retire their old pending intent. Cloud records can remain after a
local reset; clearing those records still requires cloud authorization.

## Command availability

This table is the authoritative availability source for managed local tools.

| Tool | Formal Host bundle | Source/development |
| --- | --- | --- |
| `agent-vmm-lifecycle` | Available | Available |
| `agent-vmm` managed CLI | Available; only the eight managed command families | Available; source builds also keep the separate standalone appliance commands |
| `agent-vmm-service-executor` | Included; the administrator installer copies it to its protected fixed path | Available for isolated tests |

Operational documents must use the formal-bundle column.

The helper owns Cue/BFT Host service operations and managed remote enrollment.
It does not install the standalone MCP appliance. Standalone appliance operations
use `ApplianceAdminService` directly.

Lifecycle mutations run as the configured non-root service user. Observation
commands can run as root. Agent mode manages the user's existing `gui/<uid>`
job. For daemon mode, the administrator invokes the fixed
`agent-vmm-service-executor`; lifecycle does not write or load a system job.
The executor generates one of two fixed definitions, and launchd runs both the
Host and BFT runner as the configured non-root user.

A plain `install` or `repair` reconciles a user-owned Host service and starts
the VM. For a current daemon definition, repair uses the existing Host owner.
If the system job is stopped, the public CLI returns a `service_start` action
that requires an administrator. `repair --registration-id <id>` reconnects
only that existing registration after the VM is healthy. It does not change
the installed release or registration credentials.
`install --operation-stdin --request-id <id>` performs managed enrollment:

1. Reconcile the Host service.
2. Start the VM and observe Host health.
3. Exchange the operation descriptor for remote enrollment material.
4. Persist `plan.json` with mode 0600, then acknowledge the handoff.
5. Apply the exact remote registration and wait for `READY` and `CONNECTED`.
6. Observe the same registration and persist the completion receipt.

Registration exchange and acknowledgement require HTTPS for remote endpoints.
Local development can use HTTP only with `localhost`, `127.0.0.1`, or `[::1]`.
Local HTTP bypasses proxies. The client dials `localhost` as `127.0.0.1` without DNS.
An IPv6-only local service must use `[::1]` explicitly.
Exchange and acknowledgement never follow redirects.
The same rule applies to a retained acknowledgement after restart.
This exception does not change Gateway TLS or its enrollment trust bundle.

The coordinator delegates `install`, `init`, and `register` with bounded
contexts. `runManagedLifecycleOperation` owns the durable operation record.
It preserves the plan for acknowledgement recovery and retries. The plan has
no appliance grant, bootstrap seed, manifest, or installation ID. Operation
material never appears in argv, logs, or the operation report.

A completed receipt survives secret-plan cleanup. A recorded terminal failure
returns `destroy_terminal`. Retryable and unknown results return `retain`.
After the provisioner persists its terminal report and receives the Server
acknowledgement, it invokes `install-operation-ack --operation-id <id>`.
That command removes only the named operation plan.

`status` reports the installed service user, UID, launchd job, load state,
installed application, Operator socket, process state, PID, last exit status,
and the current lifecycle operation. `operation --operation-id <id>` reads only
that current operation record and rejects a different ID. The public CLI uses
this narrow read when it follows a lifecycle operation. These commands read
facts without taking the VM mutation lock. The load state does not
prove that the process is running. `hostObserved` is true only when launchd
returned service facts or confirmed that the job is absent. If the launchd
query fails, `hostObservationIssue` reports the failure and the false load and
process values are not known facts.

If the Host process is running, `status` calls `GetSystemHealthSummary`. This
bounded health read makes one Host observation and at most two Guest health
calls. It does not enumerate environments, registrations, or workloads. Only
a successful RPC makes `hostReadable` true. The response keeps Host
readability, VM health, and Guest health separate. `status
--registration-id <id>` also
reads that exact remote connector. It does not scan appliance installations or
infer a product connection from another registration. Salix admission and
product Node readiness remain server-owned observations.

The status RPC budget is shorter than the public inspect budget. This allows
the helper to encode launchd facts and an unreadable Host result before inspect
reaches its five-second deadline.

Status observation does not validate the complete application bundle. This
allows status to report a stopped service or a damaged installation. Install,
repair, and VM start still validate the executable Guest materials before they
use them.

Host startup records use the existing Host error log. Guest bootstrap records
use the existing serial log. Each record refers to the current lifecycle or
Host operation. The helper reads at most 1 MiB from each fixed log and does not
follow symbolic links. Serial-log rotation keeps one bounded previous segment.
The status output distinguishes the last successful stage from a confirmed
failure. It does not treat the last visible stage as a root cause.
The helper uses Guest records only after a Host record maps the lifecycle
request to the Host VM-start operation. An orphan Guest record remains unknown.

BFT `compute-node install` invokes the same helper. A plain local install does
not prove a BFT runner heartbeat or Server Node admission.

The managed `agent-vmm` CLI is the public observation contract. It composes
this status with existing Operator reads for inspect, diagnose, logs, and
operation tracking. BFT consumers decode the versioned CLI envelope instead
of interpreting lifecycle fields as a separate readiness contract.

`agent-vmm vm status|start|stop` uses the Host Operator RPCs. Start and stop
require a stable request ID. These commands authorize a whole-VM action. The
action affects all workloads in the VM, including workloads that appear after
the initial observation and workloads that do not have an individual summary
entry. The CLI shows at most 100 known Host-owned environment and registration
references. It marks the summary partial if an owner read fails or the summary
overflows. A partial summary does not narrow the whole-VM authorization.

The mutation fence contains the observed owner revisions, VM state, boot ID,
and an explicit whole-VM acknowledgement. The Host rejects a missing
acknowledgement, a changed state or boot ID, and a changed revision that is
still present in the bounded summary. It holds the lifecycle and
inventory-transition locks until the VM mutation commits. A timed-out owner
response is an unknown outcome; retry only with the same request ID. Canceling
the CLI does not prove that an accepted Host operation stopped.

The Host acquires the lifecycle owner before it writes a start or stop receipt.
After acceptance, it uses a separate two-minute execution context and a
separate five-second terminal-write context. Client cancellation does not
cancel accepted VM work. A terminal request ID only replays its stored result.
After the lifecycle lock is available, a remaining running receipt has no
active executor. The Host saves it as interrupted and requires inspection plus
a new request ID. A typed admission timeout proves that no receipt was
accepted. Untyped transport errors and terminal-write failures remain unknown;
retry only the same request ID.

The administrator configures the fixed executor once with
`configure --service-user USER`. Its other accepted forms are
`install|start|stop|remove|status --job host|runner`. It accepts no path,
label, plist, environment, shell, or numeric identity argument. The executable,
owner config, and their parents must be root-owned and not group- or
world-writable. The fixed Host job disables personal mesh pairing. The managed
installer sends the same policy to lifecycle install and repair. Root writes
launchd definitions but does not run the service binaries. Root does not download
releases, read credentials, or modify service-user data.
The fixed runner definition retains KeepAlive, file-descriptor limits, HOME,
the approved PATH, working directory, and log paths. Start is a no-op for a
running job; it never uses a killing kickstart. Host stop first requires the
service-user VM owner to finish, then checks that `state.raw` is detached before
bootout and confirms both process exit and disk release.

`update` requires a hidden prepared sibling, an exact target release ID, and a
stable request ID. The lifecycle operation persists the selected release and
rejects reuse of the request ID for a different release. It records `stop`, `replace`,
`recovery`, and `complete`. Before replacement, a failure leaves the installed
bundle unchanged. After replacement, the old bundle remains at the prepared
path until the same request repairs the new service and VM. The old binary is
not a rollback target.

For a daemon, lifecycle first observes that the Host job is loaded. It stops the
VM and returns the fixed administrator stop action. After the executor stops the
Host job, the same lifecycle request does not call the unavailable Host. It
waits for the exact state-disk owner to exit, replaces the bundle, and enters
recovery. The executor installs the fixed Host job from the protected path. One
final service-user retry verifies the exact running definition, starts the VM,
and removes the retired bundle.

`cleanup preview|execute` remains a Host Operator operation. The lifecycle
helper does not delete containers, volumes, images, grants, or environments.

The helper accepts `Agent VMM Host.app` with the fixed Host executable, the two
approved helper binaries, and the Guest payload. Release inventory is
diagnostic. Runtime bundles omit the standalone MCP OCI archive and sidecars.

The acknowledgement and resume tests cover the product handoff. The retained
`HostServiceOwnership` and `HostServiceExistingOwner` models cover local service
ownership. `HostVMRestartRecovery` covers VM admission and recovery.
Appliance activation is a separate protocol in `ApplianceActivationLifecycle`.
See [the model roster](../../spec/tla/README.md).
