# Canonical lifecycle models

Host-side control is split by state ownership so independent protocols do not
form an artificial Cartesian product:

- `ControllerAllocationLifecycle.tla` owns one controller's ready
  acknowledgement, inventory watermark, epoch, command fence, allocation,
  runtime session, revoke, and the atomic admission decision. A disconnect
  closes the command session but preserves the durable allocation.
  Local disposal acceptance refines its durable revoke fence. The fixed-target
  abstraction retains that fence after resource cleanup and Host restart.
  TERM-to-KILL stop observations remain in `WorkloadRunLifecycle`; namespace
  content references remain in `OCIContentLifecycle`. Force cleanup adds no
  new fairness or progress claim: an unreachable Guest can leave it pending.
  Cross-registration data isolation, row deletion, and receipt crash recovery
  require the Go store and sacrificial Host/Guest regressions.
- `OCIImageMaterialization.tla` owns exact-digest archive re-import after a
  logical image grant survives without its shared content or persistent overlay
  lower snapshot.
- `ManagedCredentialReplacement.tla` owns same-policy-revision managed
  credential idempotence and the expiry fence for a newly signed payload.
- `WorkloadRunLifecycle.tla` owns same-workload lifecycle exclusion, the bounded
  flat execution-activity set, exact activity/instance release, quiesce fencing,
  response loss, exact stop observation, and fail-closed Host restart. Two
  activity IDs are the bounded abstraction of the production limit of 64; they
  are independent activities, not holders under one execution.
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
- `SystemInventorySeparation.tla` owns the workload inventory boundary for system
  resources.

- `GuestOCIStartup.tla` owns fixed-root preparation and the initial OCI quota allowance.
- `OCIQuotaSettlementNamespace.tla` owns namespace metadata quota settlement and growth sealing.
- `ApplianceRecoverySelection.tla` owns current-revision missing-instance recovery selection.
- `BuildMaterializationBudget.tla` owns build admission, materialization slots, and reserve-to-settle ownership.

`WorkloadRunLifecycle` and `ControllerAllocationLifecycle` remain orthogonal.
The former owns process slots and execution admission. The latter owns the
controller epoch, revision, allocation, session, and command ordering fences. OCI identity and
physical disk arithmetic stay in their own narrow models.

- `ApplianceActivationLifecycle.tla` owns one installation's coupled bundled
  artifact upgrade, bounded missing-instance recovery, bootstrap, instance
  publication, sealed integrity, revoke, interrupted operation, and
  revision-fenced removal lifecycle. A sealed installation can stop, revoke,
  or enter removal. It cannot reopen through a repair operation.
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
  and active sessions before restart, requires a fresh boot with policies and connectors restored, and
  fails closed after `MaxRecoveryAttempts` attempts across stops. `RecoveryRetry`
  abstracts the Go backoff delay. The model does not reset the count after a
  stable run, so it checks the stricter bound. macOS Host shutdown is mutually exclusive with
  that owner. Its shutdown and disk-release actions cover the macOS VZ Host.
  Linux Host exit closes control authority and preserves the independent VMM.
  Explicit VM stop still requires Guest shutdown and disk release on both platforms.
  `RecoverySuccess` abstracts only the admission commit: the
  Go owner performs boot, policy replay, and connector restoration as ordered
  steps while admission remains revoked. Disk identity and filesystem contents
  remain Go configuration concerns outside this lifecycle model. The separate
  Manager regression proves that a closed VM notification channel fails closed;
  this model does not cover that input.

Run every normal configuration and single-mechanism negative configuration
with `make tla`.

`make tla` downloads its pinned TLC release.

## Implementation anchors

| Model action or invariant | Production owner after its implementation phase | Fault or conformance test |
| --- | --- | --- |
| `ControllerReady`, `Allocate`, `InventoryMutation`, `DeleteAllocation`, `OpenSession`, `Disconnect` | `internal/remoteconnector`, `internal/host` `remoteComputeServer.SetConnectionState`, `remoteComputeServer.SetRemoteAdmissionState`, and the Host session manager | ready fences, admission, reconnect, session replacement, inventory mutation, and allocation release tests |
| `PrepareOCIStorage` | `internal/guest/control` OCI startup owner | fresh and existing disk startup tests |
| `RetryExactArchive`, `CommitExactArchive`, `CreateRequiresExactMaterial` | guest OCI import, image projection, and container creation | real archive import, forced VM restart, and rootfs creation E2E |
| `InstallValidReplacement`, `ReplacementRequiresExpired`, `ForgedCredentialNeverStored` | managed trust validation and atomic credential store | expired same-revision replacement and unexpired/forged rejection tests |
| `BeginStart`, `CommitRunning`, `Acquire`, `RetryAcquire`, `Release`, `BeginQuiesce`, `BeginStop`, `ObserveStopped`, `HostRestart` | `internal/host` `Controller.admitWorkloadStart`, `Controller.acquireStreamExecution`, `agentServer.QuiesceContainer`, `Controller.recoverWorkloadRuns` | concurrent start, two independent activities, idempotent acquire, exact activity release, response loss, stale instance, activity/quiesce race, exact stop, and restart recovery tests |
| `ReserveImport`, `Commit`, `DeleteRetained`, `GC`, `SingleGlobalImportOwner`, `ReferencedContentNotDeleted` | `internal/guest/control` import and OCI GC owners | concurrent digest imports, unsafe concurrent-owner counterexample, and shared-digest release/GC tests |
| `AdmitQuota`, `ReserveImport`, `RetainRelease`, `CacheGrow`, `DiskInvariant` | `internal/guestdisk`, guest project quota and OCI budget owners | overflow, retained accounting, import reservation, and cache pressure tests |
| `AuthorizedCommand`, `ExactRegistrationIdentity`, `DomainNonInterference` | host controller transport and command authorization | wrong-identity and cross-domain failure tests |
| `SystemExcludedFromInventory` | host inventory projection | system inventory conformance |
| `Bootstrap`, `BootstrapSingleUse` | guest appliance bridge and host bootstrap store | replay, wrong nonce/instance, and secret scan tests |
| `Start`, `CurrentInstanceFence`, `Restart` | appliance supervisor and lifecycle owner | stale boot/container health, restart, and replacement tests |
| `BundledArtifactUpgrade`, `ActiveUsesBundledArtifact`, `RecoveryEventuallyStarts` | verified Host bundle reader, appliance supervisor, and appliance store | bundled artifact atomic switch and missing-runtime recovery tests |
| `PlanRemove`, `ExecuteRemove`, `RemoveRevisionExact` | `internal/host` `applianceAdminServer.PlanRemove`, `applianceAdminServer.ExecuteRemove`, and `internal/host/store` appliance operation store | pending-destroy tombstone and revision-fence tests |
| `CrossDomainRemove`, `InstallationNonInterference` | appliance admin authorization | cross-domain remove tests |
| `Seal`, `UnsafeReopenSealed`, `SealedRemainsClosed` | appliance supervisor and appliance admin operations | artifact/bootstrap integrity failure, sealed start rejection, and revision-fenced removal tests |
| `HostVMRestartRecovery.ResumeSuccess`, `UnexpectedStop`, `RecoverySuccess`, `RecoveryCommit`, `RecoveryRetry`, `RecoveryFailure`, `BeginShutdown`, `GuestQuiesceSuccess`, `VZTerminal`, `ReleaseDisk`, `ExitHost`, `NoFreshVMStart`, `RecoveryRevokesAdmission`, `RecoveryClearsSession`, `HostExitRequiresDiskRelease` | `cmd/agent-vmm-host.shutdown`, `internal/vmm.Manager.Stop`, `internal/vmm.Manager.observeStateChanges`, `internal/vmm.Manager.terminalTransition`, `internal/host/lifecycle.Owner.recover`, `internal/host/lifecycle.Owner.Shutdown` | startup state-disk guard, session and admission revocation, one shutdown-exclusive lifecycle owner, guest quiesce before clean stop, VZ terminal completion before disk release and host exit, fresh boot, closed-notification fail-closed behavior, and terminal failure tests |

Consumer repositories may refine these actions, but they must not redefine host inventory, epoch, or lifecycle ownership.
