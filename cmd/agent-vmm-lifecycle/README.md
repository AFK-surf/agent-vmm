# Local lifecycle coordinator

The helper owns Cue/BFT Host service operations and managed remote enrollment.
It does not install the standalone MCP appliance. Standalone appliance operations
use `ApplianceAdminService` directly.

Every lifecycle command accepts `--service-type agent|daemon|auto` and
`--service-user USER`. Agent mode uses the user's existing `gui/<uid>` domain.
Daemon mode installs one system LaunchDaemon with `UserName` and `GroupName`.
The Host process remains non-root. Auto mode requires administrator access or
an existing GUI domain.

A plain `install` or `repair` reconciles the Host service and starts the VM.
`install --operation-stdin --request-id <id>` performs managed enrollment:

1. Reconcile the Host service.
2. Start the VM and observe Host health.
3. Exchange the operation descriptor for remote enrollment material.
4. Persist `plan.json` with mode 0600, then acknowledge the handoff.
5. Apply the exact remote registration and wait for `READY` and `CONNECTED`.
6. Observe the same registration and persist the completion receipt.

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

`status` reports Host service health. `status --registration-id <id>` also reads
that exact remote connector. It does not scan appliance installations or infer
a product connection from another registration. Salix admission and product
Node readiness remain server-owned observations.

BFT `compute-node install` invokes the same helper. A plain local install does
not prove a BFT runner heartbeat or Server Node admission.

The helper accepts `Agent VMM Host.app` with the fixed Host executable, the two
approved helper binaries, and the Guest payload. Release inventory is
diagnostic. Runtime bundles omit the standalone MCP OCI archive and sidecars.

The acknowledgement and resume tests cover the product handoff. The retained
`HostServiceOwnership` and `HostServiceExistingOwner` models cover local service
ownership. `HostVMRestartRecovery` covers VM admission and recovery.
Appliance activation is a separate protocol in `ApplianceActivationLifecycle`.
See [the model roster](../../spec/tla/README.md).
