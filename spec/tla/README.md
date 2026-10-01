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
- `SystemInventorySeparation.tla` owns the tenant projection boundary for system
  resources.

`WorkloadRunLifecycle` and `ControllerAllocationLifecycle` remain orthogonal.
The former owns process slots and execution admission. The latter owns the
controller epoch, revision, allocation, session, and command ordering fences. OCI identity and
physical disk arithmetic stay in their own narrow models.

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
  and active sessions before restart, requires a fresh boot with policies and connectors restored, and
  fails closed after its only attempt. Host shutdown is mutually exclusive with
  that owner. `RecoverySuccess` abstracts only the admission commit: the
  Go owner performs boot, policy replay, and connector restoration as ordered
  steps while admission remains revoked. Disk identity and filesystem contents
  remain Go configuration concerns outside this lifecycle model. The separate
  Manager regression proves that a closed VM notification channel fails closed;
  this model does not cover that input.

Run every normal configuration and single-mechanism negative configuration
with `make tla`.

The runner pins the `tla2tools` 1.7.4 release asset to SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
downloads it only into `AGENT_VMM_DOWNLOAD_CACHE` or the OS temporary directory,
stores TLC state in a fresh scratch directory, and removes that scratch on exit.

## Implementation anchors

| Model action or invariant | Production owner after its implementation phase | Fault or conformance test |
| --- | --- | --- |
| `ControllerReady`, `Allocate`, `InventoryMutation`, `DeleteAllocation`, `OpenSession`, `Disconnect` | `internal/remoteconnector`, `internal/host/remote_compute_darwin.go`, and the Host session manager | ready fences, admission, reconnect, session replacement, inventory mutation, and allocation release tests |
| `RetryExactArchive`, `CommitExactArchive`, `CreateRequiresExactMaterial` | guest OCI import, image projection, and container creation | real archive import, forced VM restart, and rootfs creation E2E |
| `InstallValidReplacement`, `ReplacementRequiresExpired`, `ForgedCredentialNeverStored` | managed trust validation and atomic credential store | expired same-revision replacement and unexpired/forged rejection tests |
| `BeginStart`, `CommitRunning`, `Acquire`, `RetryAcquire`, `Release`, `BeginQuiesce`, `BeginStop`, `ObserveStopped`, `HostRestart` | `internal/host/workload_run_darwin.go`, `internal/host/workload_execution_darwin.go`, `internal/host/workload_observation_darwin.go` | concurrent start, two independent activities, idempotent acquire, exact activity release, response loss, stale instance, activity/quiesce race, exact stop, and restart recovery tests |
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
| `HostVMRestartRecovery.ResumeSuccess`, `UnexpectedStop`, `RecoverySuccess`, `RecoveryCommit`, `RecoveryFailure`, `BeginShutdown`, `GuestQuiesceSuccess`, `VZTerminal`, `ReleaseDisk`, `ExitHost`, `NoFreshVMStart`, `RecoveryRevokesAdmission`, `RecoveryClearsSession`, `HostExitRequiresDiskRelease` | `cmd/agent-vmm-host.shutdown`, `internal/vmm.Manager.Stop`, `internal/vmm.Manager.observeStateChanges`, `internal/vmm.Manager.terminalTransition`, `internal/host.VMLifecycleOwner.Shutdown` | startup state-disk guard, session and admission revocation, one shutdown-exclusive lifecycle owner, guest quiesce before clean stop, VZ terminal completion before disk release and host exit, fresh boot, closed-notification fail-closed behavior, and terminal failure tests |

Consumer repositories may refine these actions, but they must not redefine host inventory, epoch, or lifecycle ownership.

`GuestOCIStartup` maps `PrepareOCIStorage` to its initial allowance phase,
then preserves containerd startup, stale-state recovery, existing-state GC
drain, quota tightening, and readiness publication. Establishing inheritance
on the fixed OCI roots instead of recursively rewriting image inodes does not
change those transitions. Re-establishing the imports root project before
archive creation preserves `OCIContentLifecycle`'s accounted import writes.
Assigning both overlay upper and work directories to the workload project
implements the same model's independent writable-snapshot ownership: copy-up
creates a temporary inode in work before linking it into upper. It adds no
distributed transition or recovery promise for existing misassigned snapshots.
Loading the Host-provided entropy device's kernel module lets containerd's
existing secure lease-ID generation run during startup; it changes neither the
GC barrier nor its deadline, fairness assumptions, or failure transitions.
The models do not represent Linux pathname resolution or inode flags; the
privileged ext4 startup and quota regressions check those implementation facts.
No new progress claim or fault assumption is introduced by these syscall fixes.

System archive import uses the existing role-aware namespace lookup after
system namespace admission and before full archive-header validation. This restores the system-resource
path already represented by `SystemInventorySeparation`; ordinary requests
still reject reserved system namespaces. The Host reads the existing final
Guest result when a streaming send reports EOF, without resending the archive
or adding a retry state. `OCIImageMaterialization` receipt and exact-archive
transitions are unchanged; streamed early-result regressions and the real
system-appliance import/activation E2E check this implementation mapping.

Stopped-container replacement and ordinary container deletion use the existing
`GuestDiskBudget` metadata mutation and `OCIQuotaSettlementNamespace` settlement
owner. Deleting a containerd record or snapshot can allocate copy-on-write
metadata before reclaiming space; it therefore needs the same temporary
allowance as creation. This is an additional caller of the existing mutation
transition, with unchanged floors, sealing, GC barrier, and idle ceiling.
Task termination and monitor waits remain outside that budget transaction.

BuildKit container/task creation, wait registration, and start form one short
metadata mutation. Task termination and exit waits remain outside it; task and
snapshot cleanup use a second short metadata mutation. The existing budget,
sealing, and settlement transitions are unchanged.

`OCIImageMaterialization` also models build output publication in
`Runtime.BuildImage` / `importImage`: `ImportBuild` registers content in the
runtime namespace, `UnpackBuild` creates its snapshot, then `PublishBuild`
updates the logical tag. Physically shared CAS blobs alone do not provide
containerd namespace metadata. The build-import negative represents the former
descriptor-only promotion and reaches an unpack without runtime content; the
build-commit negative publishes before materialization. A build canceled or failed before publication
may abandon its candidate and leaves the old logical image intact. An explicit
successful build can replace a tag; ordinary archive import still rejects a
conflicting digest. The model claims no build progress under failure or restart.

`ApplianceRecoverySelection` refines only missing-instance selection shared by
explicit lifecycle activation and the background appliance reconciler. Host candidates
precede Guest observations; `SelectRecovery` maps the installation revision
check in `recoverApplianceRuntime`, and `Launch` retains the second check in
`startApplianceLocked`. The existing installation owner spans preparation and
start. `Publish` abstracts the existing installation CAS and failed-start
cleanup; explicit desired-state changes can occur while that owner is held.
`SyncReturn` checks published Host facts for the current VM boot and instance,
not instantaneous Guest process liveness. No new polling or progress guarantee
is claimed. The bounded model includes two competing callers, an explicit
disable, and a Guest restart; transport failures return existing errors and
must not produce a successful synchronous completion. Full bootstrap, stale
capability rejection, and publication protocols remain in
`ApplianceActivationLifecycle`, rather than a Cartesian product here.
Negative configurations remove snapshot ordering, the recovery revision fence,
or synchronous completion validation. There is no negative claim that the old
prepare/start lock gap alone caused duplicate launches: its second start-time
revision check is real and must be retained.

`BuildMaterializationBudget` isolates the existing two admitted build workers,
one global materialization slot, and local reservation transaction. It maps
`Workspace.RunArchive` to a slot held through output consumption,
`Manager.LockBuild` to deadline/cancellation-aware waiting, and
`guardOCIPhysicalGrowth` / `Transaction.ReserveImportHeadroom` to the closed
reserve-convert-materialize-settle section. Metadata cannot see a local
reservation with zero slack. Cancellation/timeout/disconnect may settle a waiter;
restart clears volatile requests and orphan holds. Settlement failure seals
further growth. No network or native-operation liveness is claimed. Physical
capacity is sufficient in this focused model; `GuestDiskBudget` retains general
capacity and retained-writable accounting. Streaming import admission is unchanged.
Three expected counterexamples expose the split reservation transaction,
immediate rejection of an admitted build, and an unfenced canceled waiter.
