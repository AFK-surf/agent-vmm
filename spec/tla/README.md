# Canonical lifecycle models

`ControllerDomainControl.tla` owns the host-side domain control facts. `ApplianceLifecycle.tla` owns fixed-artifact installation, single-use bootstrap, current boot/container fencing, stop/restart/revoke, sealed recovery, and revision-fenced removal. Its finite instance gives one installation the lifecycle operation while the other installation can progress or stutter independently. Run both normal configurations and every single-mechanism negative configuration with `make tla`.

The runner pins `tla2tools` 1.8.0 to SHA-256 `ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f`, downloads it only into `AGENT_VMM_DOWNLOAD_CACHE` or the OS temporary directory, and stores TLC state in a fresh scratch directory.

## Implementation anchors

| Model action or invariant | Production owner after its implementation phase | Fault or conformance test |
| --- | --- | --- |
| `ControllerReady`, `NoCommandBeforeReady`, `ExactInventorySettlement`, `CommandFenceExact` | `internal/remoteconnector`, `internal/host/remote_compute_darwin.go` | ready identity/epoch/watermark table tests, stale-command counterexample, and remote reconnect E2E |
| `Allocate`, `AggregateNoOvercommit`, `SystemExcludedFromInventory` | `internal/host/store`, host aggregate admission | multi-domain admission race tests and system inventory conformance |
| `OpenSession`, `SingleCurrentCapability` | host remote lease/session manager | replacement capability and stale session fault tests |
| `Disconnect`, `RevokeIsTerminal`, `DomainNonInterference` | host controller transport and revoke path | cross-domain disconnect/revoke/restart fault tests |
| `Bootstrap`, `BootstrapSingleUse` | guest appliance bridge and host bootstrap store | replay, wrong nonce/instance, and secret scan tests |
| `Start`, `CurrentInstanceFence`, `Restart`, `Revoke` | appliance supervisor and lifecycle owner | stale boot/container health, restart and capability revocation tests |
| `PlanRemove`, `ExecuteRemove`, `RemoveRevisionExact` | `internal/host/appliance_admin_remove_darwin.go`, `internal/host/store/appliance_operation.go` | pending-destroy tombstone, revision fence, and cross-domain remove tests |
| `Seal`, `RecoverSealed`, `SealedRecoveryIsBounded` | appliance supervisor and appliance admin operations | key loss, corruption, schema, publisher trust, and bounded repair tests |

Consumer repositories may refine these actions, but they must not redefine host inventory, epoch, or lifecycle ownership.
