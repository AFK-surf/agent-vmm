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
Signed agent-mode releases can use Keychain and Secure Enclave in that user's logged-in session.
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

A retryable stage has a 30-second cooldown and at most three attempts.
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

## Enrollment and observation

`install --operation-stdin --request-id REQUEST` accepts a managed operation descriptor through stdin.
The owner starts the Host and VM, exchanges the descriptor, saves a private plan, and acknowledges the handoff.
It then applies the exact registration and waits for `READY` and `CONNECTED`.
The secret plan never appears in arguments or reports.
A completed receipt survives plan cleanup.
`install-operation-ack --operation-id OPERATION` removes only the named secret plan after the controller acknowledges its report.

Remote exchange and acknowledgement require HTTPS without redirects.
Local development permits HTTP only on `localhost`, `127.0.0.1`, or `[::1]`, without proxies.
This exception does not weaken controller TLS verification.

`repair --registration-id ID` reconnects that existing registration after VM health succeeds.
It does not replace credentials or the installed release.
Status separates loaded job, running process, readable Host, VM health, Guest health, and exact registration connectivity.
A loaded job does not prove a running process. A failed observation remains unknown.
Controllers read the public CLI envelope rather than inventing another readiness interpretation.

## Database compatibility

The Host accepts only its current database schema.
A different schema returns `STATE_SCHEMA_INCOMPATIBLE` before admission.
There is no state conversion.
Before an incompatible upgrade, preserve required data and approve deletion of the exact installation.
Run `agent-vmm-lifecycle uninstall --purge`.
Then install the current package as a new installation.

## Publication and uninstall

A verified payload helper can run `publish-host --source-app APP --service-type auto` before installation.
It publishes only to the configured user's standard shared app path under the lifecycle owner lock.
It does not start launchd or the VM. An existing different or damaged target requires update or maintenance.
`--shared-host` selects that fixed target. It does not accept an arbitrary path.

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
