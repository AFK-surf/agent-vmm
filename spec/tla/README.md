# Canonical lifecycle models

Host-side control is split by state ownership so independent protocols do not
form an artificial Cartesian product:

- `ControllerAllocationLifecycle.tla` owns one controller's ready
  acknowledgement, inventory watermark, epoch, command fence, allocation,
  lease, reconnect capability, revoke, and the atomic admission decision.
- `AggregateAdmission.tla` owns only cross-domain capacity accounting.
- `DomainAuthorityIsolation.tla` owns multi-domain command identity and failure
  isolation.
- `SystemInventorySeparation.tla` owns the tenant projection boundary for system
  resources.

`AggregateAdmission` and `ControllerAllocationLifecycle` form an
assume-guarantee boundary instead of a combined state product: the aggregate
owner computes whether capacity is available, while the controller model treats
that Boolean as an environment input and proves that `Allocate` cannot commit
when it is false. The production allocation transaction is the code anchor that
maps the computed aggregate decision to that input.

- `ApplianceActivationLifecycle.tla` owns one installation's coupled artifact,
  bootstrap, instance publication, sealed recovery, revoke, interrupted
  operation, and revision-fenced removal lifecycle.
- `ApplianceInstallationIsolation.tla` uses two installation identities only
  for the remove-ownership non-interference proof; it does not multiply two
  complete lifecycles.
- `HostServiceOwnership.tla` owns launchd agent/daemon selection and the
  single-service-owner invariant. `HostServiceExistingOwner.tla` separately
  checks that observation and removal follow the persisted owner rather than
  rerunning first-install `auto` selection. `HostVMRestartRecovery.tla` owns
  the one-shot post-install VM resume after a Host process start; it never
  re-enters installation and does not start a fresh or invalid VM state.

Run every normal configuration and single-mechanism negative configuration
with `make tla`.

The runner pins the current official `tla2tools` 1.8.0 release asset to SHA-256
`eabd140a70f49eb9305a3bd3f3df944eddf87e5a90d329789085f8953a80533a`,
downloads it only into `AGENT_VMM_DOWNLOAD_CACHE` or the OS temporary directory,
stores TLC state in a fresh scratch directory, and removes that scratch on exit.

## Implementation anchors

| Model action or invariant | Production owner after its implementation phase | Fault or conformance test |
| --- | --- | --- |
| `ControllerReady`, `Allocate`, `InventoryMutation`, `DeleteAllocation`, `OpenSession` | `internal/remoteconnector`, `internal/host/remote_compute_darwin.go`, host lease/session manager | ready/fence, admission, reconnect, capability replacement, inventory mutation, and leased-delete tests |
| `AggregateAdmission.Allocate`, `NoAllocationWithoutAdmission`, `AggregateNoOvercommit` | `internal/host/store`, host aggregate admission and allocation transaction | multi-domain admission race tests and disabled-admission counterexample |
| `AuthorizedCommand`, `ExactRegistrationIdentity`, `DomainNonInterference` | host controller transport and command authorization | wrong-identity and cross-domain failure tests |
| `SystemExcludedFromInventory` | host inventory projection | system inventory conformance |
| `Bootstrap`, `BootstrapSingleUse` | guest appliance bridge and host bootstrap store | replay, wrong nonce/instance, and secret scan tests |
| `Start`, `CurrentInstanceFence`, `Restart` | appliance supervisor and lifecycle owner | stale boot/container health, restart, and replacement tests |
| `PlanRemove`, `ExecuteRemove`, `RemoveRevisionExact` | `internal/host/appliance_admin_remove_darwin.go`, `internal/host/store/appliance_operation.go` | pending-destroy tombstone and revision-fence tests |
| `CrossDomainRemove`, `InstallationNonInterference` | appliance admin authorization | cross-domain remove tests |
| `Seal`, `RecoverSealed`, `SealedRecoveryIsBounded` | appliance supervisor and appliance admin operations | key loss, corruption, schema, publisher trust, and bounded repair tests |
| `HostVMRestartRecovery.ResumeSuccess`, `NoFreshVMStart` | `cmd/agent-vmm-host`, `internal/host.Controller.StartVM` | exact state-disk guard and Host restart physical smoke |

Consumer repositories may refine these actions, but they must not redefine host inventory, epoch, or lifecycle ownership.
