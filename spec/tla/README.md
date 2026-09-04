# Canonical lifecycle models

Host-side control is split by state ownership so independent protocols do not
form an artificial Cartesian product:

- `ControllerAllocationLifecycle.tla` owns one controller's ready
  acknowledgement, inventory watermark, epoch, command fence, allocation,
  lease, reconnect capability, revoke, and the atomic admission decision.
- `RemoteLeaseDisconnectGrace.tla` owns the persisted disconnect fence used by
  delayed lease revocation across reconnect and a later disconnect, and proves
  that revocation preserves the durable allocation and reservation.
- `OCIImageMaterialization.tla` owns exact-digest archive re-import after a
  logical image grant survives without its shared content or persistent overlay
  lower snapshot.
- `ManagedCredentialReplacement.tla` owns same-policy-revision managed
  credential idempotence and the expiry fence for a newly signed payload.
- `ResourceAdmissionQueue.tla` owns the exact Host logical/Guest physical
  reservation receipt and released-tombstone confirmation, five independent
  admission dimensions (memory is the aggregate `memory.high` entitlement;
  each `memory.max` remains an individual cgroup ceiling), FIFO/deadline claim, command epoch/generation fences,
  response loss, restart, stop, and exact release convergence.
- `OCIContentLifecycle.tla` owns the single global import slot, transient peak
  reservation, timeout/crash, the one shared content/lower-snapshot fact per
  digest, active container references, logical namespace grants, pressure
  watermarks, and ref-safe GC. The production private containerd image record
  refines the shared materialization fact; per-environment image records refine
  retained references rather than owning another physical snapshot.
- `GuestDiskBudget.tla` owns the narrow physical `U+W+I+E+M<=D` budget shared by
  quota admission, retained namespaces, imports, and cache/content/snapshot GC;
  all allocated bytes live in the single `U` fact.
- `DomainAuthorityIsolation.tla` owns multi-domain command identity and failure
  isolation.
- `SystemInventorySeparation.tla` owns the tenant projection boundary for system
  resources.

`ResourceAdmissionQueue` and `ControllerAllocationLifecycle` remain orthogonal:
the former proves physical resource/receipt ownership while the latter owns the
controller epoch, revision, lease, and command ordering fences. OCI identity and
aggregate disk arithmetic stay in their own narrow models to avoid a false
Cartesian product.

- `ApplianceActivationLifecycle.tla` owns one installation's coupled bundled
  artifact upgrade, bounded missing-instance recovery, bootstrap, instance
  publication, sealed recovery, revoke, interrupted operation, and
  revision-fenced removal lifecycle.
- `ApplianceInstallationIsolation.tla` uses two installation identities only
  for the remove-ownership non-interference proof; it does not multiply two
  complete lifecycles.
- `HostServiceOwnership.tla` owns launchd agent/daemon selection and the
  single-service-owner invariant. `HostServiceExistingOwner.tla` separately
  checks that observation and removal follow the persisted owner rather than
  rerunning first-install `auto` selection, and that a bundle update adopts the
  fresh fixed CPU, memory, and fully-backed state-disk geometry while preserving
  the restrictive personal-mesh pairing policy. Production
  anchors are `selectServiceModeForAction` and `renderServiceDefinition`, with
  regressions in `TestServiceDefinitionUsesFreshBundleResourceGeometry` and
  `TestRepairRetainsConfiguredPairingRestriction`.
  `HostVMRestartRecovery.tla` owns the one-shot post-install VM resume and the
  single-owner recovery after an unexpected VM stop. Recovery revokes admission
  and active lease capabilities before restart, requires a fresh boot with policies and connectors restored, and
  fails closed after its only attempt. Host shutdown is mutually exclusive with
  that owner. `RecoverySuccess` abstracts only the admission commit: the
  Go owner performs boot, policy replay, and connector restoration as ordered
  steps while admission remains revoked. Disk identity and filesystem contents
  remain Go configuration concerns outside this lifecycle model. The separate
  Manager regression proves that a closed VM notification channel fails closed;
  this model does not cover that input.

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
| `Disconnect`, `Reconnect`, `GraceExpires`, `NoStaleTimerRevocation` | `internal/host/remote_compute_darwin.go`, `internal/host/remote_lease_darwin.go`, persisted remote registration | disconnect grace timer behavior across reconnect and a newer disconnect |
| `RetryExactArchive`, `CommitExactArchive`, `CreateRequiresExactMaterial` | guest OCI import, image projection, and container creation | real archive import, forced VM restart, and rootfs creation E2E |
| `InstallValidReplacement`, `ReplacementRequiresExpired`, `ForgedCredentialNeverStored` | managed trust validation and atomic credential store | expired same-revision replacement and unexpired/forged rejection tests |
| `HostLogicalReserve`, `GuestExactReserve`, `ReconcileReceipt`, `ExactReleaseOnly`, `RenewLease`, `ExplicitReleaseLease`, `ExpireLease`, `HostRestart` | `internal/host/store/resource.go`, `internal/host/store/remote.go`, `internal/host/remote_lease_darwin.go`, `internal/host/remote_compute_darwin.go`, `internal/guest/control/resource_v2_linux.go` | exact response-loss retry, close/reopen projection recovery, connection replacement without nested-lock deadlock, stale timer after renewal, concurrent expiry/release, and stale-generation release tests |
| `ReserveImport`, `Commit`, `DeleteRetained`, `GC`, `SingleGlobalImportOwner`, `ReferencedContentNotDeleted` | `internal/guest/control/import_linux.go`, `internal/guest/control/oci_gc_linux.go`, `internal/ocimaterializer` | concurrent digest imports, unsafe concurrent-owner counterexample, and shared-digest release/GC tests |
| `AdmitQuota`, `ReserveImport`, `RetainRelease`, `CacheGrow`, `DiskInvariant` | `internal/guestdisk`, guest project quota and OCI budget owners | overflow, retained accounting, import reservation, and cache pressure tests |
| `AuthorizedCommand`, `ExactRegistrationIdentity`, `DomainNonInterference` | host controller transport and command authorization | wrong-identity and cross-domain failure tests |
| `SystemExcludedFromInventory` | host inventory projection | system inventory conformance |
| `Bootstrap`, `BootstrapSingleUse` | guest appliance bridge and host bootstrap store | replay, wrong nonce/instance, and secret scan tests |
| `Start`, `CurrentInstanceFence`, `Restart` | appliance supervisor and lifecycle owner | stale boot/container health, restart, and replacement tests |
| `BundledArtifactUpgrade`, `ActiveUsesBundledArtifact`, `RecoveryEventuallyStarts` | verified Host bundle reader, appliance supervisor, and appliance store | bundled artifact atomic switch and missing-runtime recovery tests |
| `PlanRemove`, `ExecuteRemove`, `RemoveRevisionExact` | `internal/host/appliance_admin_remove_darwin.go`, `internal/host/store/appliance_operation.go` | pending-destroy tombstone and revision-fence tests |
| `CrossDomainRemove`, `InstallationNonInterference` | appliance admin authorization | cross-domain remove tests |
| `Seal`, `RecoverSealed`, `SealedRecoveryIsBounded` | appliance supervisor and appliance admin operations | key loss, corruption, schema, and bounded repair tests |
| `HostVMRestartRecovery.ResumeSuccess`, `UnexpectedStop`, `RecoverySuccess`, `RecoveryCommit`, `RecoveryFailure`, `BeginShutdown`, `GuestQuiesceSuccess`, `VZTerminal`, `ReleaseDisk`, `ExitHost`, `NoFreshVMStart`, `RecoveryRevokesAdmission`, `RecoveryClearsLease`, `HostExitRequiresDiskRelease` | `cmd/agent-vmm-host.shutdown`, `internal/vmm.Manager.Stop`, `internal/vmm.Manager.observeStateChanges`, `internal/vmm.Manager.terminalTransition`, `internal/host.VMLifecycleOwner.Shutdown` | startup state-disk guard, lease and admission revocation, one shutdown-exclusive lifecycle owner, guest quiesce before clean stop, VZ terminal completion before disk release and host exit, fresh boot, closed-notification fail-closed behavior, and terminal failure tests |

Consumer repositories may refine these actions, but they must not redefine host inventory, epoch, or lifecycle ownership.
