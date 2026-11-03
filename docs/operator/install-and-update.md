# Install and update

## Installed tools

| Tool | Formal Host package |
| --- | --- |
| `agent-vmm` | Managed CLI, appliance commands, and MCP token commands. |
| `agent-vmm-lifecycle` | macOS lifecycle helper. Linux uses `agent-vmm lifecycle`. |
| `agent-vmm-service-executor` | macOS fixed administrator service actions. |
| `agent-vmm-data` | Host-supervised peer transport worker. |

The managed command list is [generated from its command table](../reference/cli.md).

## macOS installation

Use a complete package from [the build guide](../build.md).

```sh
OUTPUT_DIRECTORY=HOST_DIRECTORY build/host/install.sh --service-type agent
```

Agent mode uses the service user's GUI launchd domain.
Daemon mode uses `--service-type daemon --service-user USER` and requires administrator service setup.
The Host always runs as the configured non-root user.
Signed agent-mode releases with a provisioning profile can use Keychain and Secure Enclave in that user's logged-in session.
Without a profile, fresh installations use files. Existing Keychain installations report an error and preserve their storage selection.
Daemon mode uses file secrets. Changing deployment mode requires a new installation.
The fixed label is `com.afk-surf.agent-vmm-host`.
Read status to resolve the actual user, application, and state paths before an operation.

The daemon executor accepts `configure --service-user USER`, then `install`, `start`, `stop`, `remove`, or `status`.
It accepts no custom path, label, environment, shell, or numeric identity.
Its executable, owner configuration, and ancestors must be root-owned and protected from other writers.
The executor writes only the fixed service definition. Root does not run Host, download, credential, or user-data work.
The fixed daemon disables personal mesh pairing.
When the CLI reports `service_start`, an administrator starts the fixed executor job.
The service user then retries the same lifecycle request.

## State-preserving update

1. Select the exact HTTPS artifact, release ID, SHA-256, and byte size.
2. Submit the update as the service user:

   ```sh
   agent-vmm update --request-id REQUEST --target-release-id RELEASE \
     --artifact-url HTTPS_URL --artifact-sha256 SHA256 --artifact-size BYTES --json
   ```

3. Read the returned stage, disposition, and original operation ID.
4. If observation ends, retry the same request or follow that operation.
5. Perform any reported fixed administrator action, then resume the same request as the service user.
6. Confirm Guest health, registration connectivity, and an existing runtime session.

Preparation verifies the selected artifact before it stops or replaces the installed Host.
The lifecycle owner records `stop`, `replace`, `restore`, and `settled`.
It stops the VM, confirms process exit and disk release, then exchanges the complete applications.
The database, keys, registrations, workloads, volumes, and state disk stay in place.
After replacement, repair forward with the same request and artifact.
The previous application remains recovery evidence until the owner verifies the replacement.
It is not a runnable rollback target after state changes.

A retryable stage has a 30-second cooldown. Continue the same request and selected artifact after a failure.
The native owner does not apply a lifetime retry count.
A terminal refusal requires a new explicit decision.
CLI observation lasts at most 30 seconds. It does not cancel accepted owner work.
See [the disposition table](cli.md#update-dispositions).

## Linux installation and update

Linux mutations run as root. The Host runs as the `agent-vmm` system user.

| Item | Path |
| --- | --- |
| Release selection | `/opt/agent-vmm/current` |
| State | `/var/lib/agent-vmm` |
| Service logs | `/var/log/agent-vmm` |
| Guest logs | `/var/lib/agent-vmm/logs` |
| Service unit | `/etc/systemd/system/agent-vmm-host.service` |

Read [the Linux Host service guide](../../test/kvmdev/README.md#linux-host-service) for first installation.
Run the installed `agent-vmm update` with `sudo`, the exact artifact fields, and a stable request ID.
The owner stages the release, stops the VM and unit, verifies the copied files, and switches `current` atomically.
It then starts the service and VM. It preserves `/var/lib/agent-vmm`.
A surviving VMM lock blocks replacement. Identify the exact owner before another action.
Never delete the state directory to repair an update.

Host service stop or restart preserves the independent VMM and its workloads.
Update and uninstall stop the VM through the Host before stopping the service and confirming disk release.
A failed VM stop preserves the service definition, installed release, and data for retry.

## Registration and observation

`registration-claim --descriptor-stdin --request-id REQUEST` accepts a Registration descriptor through stdin.
The descriptor contains `version`, `registration_id`, `claim_url`, `ticket`, and `expires_at`.
It can include the product `request_id` as correlation metadata.
The owner starts the Host and VM. It saves the candidate credential before the TLS claim.
The response contains public connection configuration. Claim completes after the owner saves and applies that configuration and credential.
Host and VM preparation must also succeed. The control connection remains an independent observation.
An unknown reply continues the same saved candidate and Registration.
Secrets never appear in arguments or reports.

`registration-recover --descriptor-stdin --request-id REQUEST` replaces a credential with proof from the original Host root.
Its descriptor contains `version`, `registration_id`, `recovery_url`, and a current bare Session bearer in `authorization`.
The authorization value stays in stdin and memory. The owner does not save it in the plan.
Recovery completes when the same Registration configuration and replacement credential are durable.
It does not require a control connection or enable a stopped Registration.
Explicitly revoked or locally disposed Registrations cannot recover.

TLS requests reject redirects. Local development permits HTTP only on loopback, without proxies.
This exception does not weaken Gateway TLS verification.

`registration-drain`, `registration-enable`, and `registration-remove` require an exact Registration and request ID.
Drain preserves data and fences the old enable request. A new explicit enable can restore the same Registration.
Remove records its target tombstone before it completes local revocation. Old claim and enable requests cannot revive it.
The native owner acquires the OS lock before it commits a supersession fence.
A trusted caller can stop only its exact helper executor and must confirm exit before another mutation.

`operation --request-id REQUEST` and `maintenance-status` read every accepted lifecycle receipt.
For a lost legacy recovery reply, add `--registration-id ID` to the original requested read.
Only an explicit committed `correlationRequestId` on that same receipt can return the preserved native request.
This one-time migration metadata does not redirect later requests or accept a historical success for another request.
A failed remote claim does not occupy the global maintenance pointer after its executor exits.
Shared software and disk cutovers retain exclusive recovery ownership.
`host-maintain` accepts the existing maintenance JSON request. Explicit `continue: true` opens another three-start Host restore window.
It preserves the original cutover receipt and cumulative attempt count. Exhaustion pauses restoration and does not erase data.

`repair --registration-id ID` restores software and observes that existing registration after VM health succeeds.
It does not enable the Registration or replace credentials.
Status separates loaded job, running process, readable Host, VM health, Guest health, and exact Registration connectivity.
A loaded job does not prove a running process. A failed observation remains unknown.

## Database compatibility

Schema 39 is the forward-migration baseline. Older data requires an approved reset of the exact installation.
The Host migrates supported SQLite schemas before admitting controllers or runtime operations.
One transaction commits all pending changes and version records. An interrupted transaction leaves the previous committed state.
`version --json` reports `minimumSchema` and `currentSchema` for the target database decoder.
It also reports `contractVersion=1` for the helper output contract.
`status` and non-null `maintenance` or `maintenance-status` receipts report the same contract version.
Unknown fields are additive. A consumer must understand the fields that its requested operation uses.
A contract mismatch does not authorize data deletion, re-enrollment, or termination of independent children.
If the installed helper cannot interpret the operation, use the existing update path from a verified target bundle.
Maintenance checks this range before stopping or replacing the current Host.
Its retained `target_older_than_data` receipt code means any unsupported source schema.
Direct Host startup applies the same compatibility rule.
Newer data, or populated data without a schema version, returns `STATE_SCHEMA_INCOMPATIBLE` without conversion.
Migration failure returns `STATE_MIGRATION_FAILED`. Repair the cause and retry forward, or use the controller's authorized reset reinstall policy.
The migration code never resets data or restores an older binary.
Reset requires the existing native maintenance authority and exact-process stop confirmation.
Report lost local work and required sign-in or enrollment after reset.
Guest disks, credential stores, and lifecycle journals retain their separate owners. SQLite migration does not modify them.

## Publication and uninstall

A verified payload helper can run `publish-host --source-app APP --service-type auto` before installation.
It publishes only to the configured user's standard shared app path under the lifecycle owner lock.
It does not start launchd or the VM. An existing different or damaged target requires update or maintenance.
`--shared-host` selects that fixed target. It does not accept an arbitrary path.

After an owner uninstall, publication returns `host_uninstalled` with reason `owner_uninstalled` and a nonzero exit.
An explicit setup can request the existing confirmed reinstall with preserved data, then continue its original request.
Background retries cannot create that reinstall. An old blocked request keeps its own refusal and fence.

Ordinary uninstall preserves data. `build/host/uninstall.sh --purge` requires approval for the exact deleted state.
Resolve the service, registrations, installations, and disks before approving purge.
Stop the exact VM and confirm disk release before deleting any data.
Controller registration cleanup remains a separate owner-authorized action.

## Maintenance

`maintenance` controls the shared Host. Update preserves data. Reinstall and uninstall permit preserve or confirmed reset.
These operations interrupt all VM workloads. They do not revoke controller registrations or grant controller permissions.
Exact environment and registration disposal use their own operations.

Run maintenance as the service user from a verified bundle outside the installed app and Host data.
Keep the canonical `Agent VMM Host.app` name. Verify the archive digest and size before extraction.
Retain that bundle until the request settles. The owner verifies the final replacement copy before publication.

```sh
"$verified_app/Contents/Helpers/agent-vmm-lifecycle" maintenance --service-type auto < request.json
"$verified_app/Contents/Helpers/agent-vmm-lifecycle" maintenance-status < status-request.json
```

| Request field | Contract |
| --- | --- |
| `version` | `1` |
| `requestId` | Stable request ID. Use it for every retry. |
| `action` | `update`, `reinstall`, or `uninstall` |
| `dataPolicy` | `preserve` or `reset`. Update permits preserve only. |
| `targetApp` | Service user's `~/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app` |
| `sourceApp` | Canonical source bundle outside the installed app and Host data. Omit for uninstall. |
| `targetReleaseId` | Selected release. Omit for uninstall. |
| `confirmSharedHost` | `true` after local confirmation of whole-Host interruption |
| `confirmDataDeletion` | `true` for reset |
| `supersedesRequestId` | Exact failed, retryable active request to replace after a new confirmation |

The owner rejects a reused request ID with a different action, policy, target, source, or service identity.
`maintenance-status` returns the receipt, stage, and outcome.
Retry the same complete request after `pending`.

| Failure code | Retryable | Action |
| --- | --- | --- |
| `maintenance_incomplete` | `true` | Retry the same request. |
| `target_older_than_data` | `false` | Keep the current Host or confirm a reset reinstall. |
| `host_start_failed` | `false` | Review the startup failure before a new explicit request. |

Daemon maintenance keeps the administrator boundary. The service user stops the VM and records the request.
An administrator uses `agent-vmm-service-executor` for the fixed Host job action.
The service user then retries the same request. Do not run maintenance business operations as root.
