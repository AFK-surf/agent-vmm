#!/bin/sh
set -eu

version=1.7.4
expected_sha=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
cache_root=${AGENT_VMM_DOWNLOAD_CACHE:-${TMPDIR:-/tmp}/agent-vmm-download-cache}
jar="$cache_root/tla2tools-$version.jar"
temp_root=${TMPDIR:-/tmp}
scratch=$(mktemp -d "$temp_root/agent-vmm-tla.XXXXXX")
spec_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../../spec/tla" && pwd)

cleanup() {
  case "$scratch" in
    "$temp_root"/agent-vmm-tla.*) rm -rf -- "$scratch" ;;
    *) echo "refusing to clean unexpected TLC scratch path: $scratch" >&2 ;;
  esac
}
terminate() {
  trap - HUP INT TERM
  exit 130
}
trap cleanup EXIT
trap terminate HUP INT TERM

mkdir -p "$cache_root"
if [ ! -f "$jar" ] || [ "$(shasum -a 256 "$jar" | awk '{print $1}')" != "$expected_sha" ]; then
  candidate="$scratch/tla2tools.jar"
  curl -fsSL "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar" -o "$candidate"
  actual_sha=$(shasum -a 256 "$candidate" | awk '{print $1}')
  if [ "$actual_sha" != "$expected_sha" ]; then
    echo "tla2tools digest mismatch: got $actual_sha" >&2
    exit 1
  fi
  mv "$candidate" "$jar"
fi

run_model() {
  module=$1
  config=$2
  output="$scratch/$config.log"
  run_dir="$scratch/$config"
  mkdir -p "$run_dir/states"
  cp "$spec_dir/$module.tla" "$spec_dir/$config.cfg" "$run_dir/"
  (cd "$run_dir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -deadlock -workers 1 -metadir "$run_dir/states" -config "$config.cfg" "$module.tla") >"$output" 2>&1
  grep -q 'Model checking completed. No error has been found.' "$output"
}

run_negative() {
  module=$1
  config=$2
  invariant=$3
  expected_action=${4:-}
  output="$scratch/$config.log"
  run_dir="$scratch/$config"
  mkdir -p "$run_dir/states"
  cp "$spec_dir/$module.tla" "$spec_dir/$config.cfg" "$run_dir/"
  if (cd "$run_dir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -deadlock -workers 1 -metadir "$run_dir/states" -config "$config.cfg" "$module.tla") >"$output" 2>&1; then
    echo "$config unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -q "Invariant $invariant is violated" "$output"; then
    echo "$config did not violate $invariant" >&2
    sed -n '1,160p' "$output" >&2
    exit 1
  fi
  if [ -n "$expected_action" ] && ! grep -q "<$expected_action" "$output"; then
    echo "$config did not reach $expected_action in its counterexample" >&2
    sed -n '1,160p' "$output" >&2
    exit 1
  fi
}

run_model ControllerAllocationLifecycle ControllerAllocationLifecycle
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleReadyNegative NoCommandBeforeReady Allocate
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleWatermarkNegative ExactInventorySettlement ControllerReady
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleSequenceNegative CommandFenceExact Allocate
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleAdmissionNegative NoAllocationWithoutAdmission Allocate
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleSessionNegative SingleCurrentCapability OpenSession
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleRevokeNegative RevokeIsTerminal Allocate
run_negative ControllerAllocationLifecycle ControllerAllocationLifecycleDeleteLeaseNegative LeaseBlocksDelete DeleteAllocation
run_model RemoteLeaseDisconnectGrace RemoteLeaseDisconnectGrace
run_negative RemoteLeaseDisconnectGrace RemoteLeaseDisconnectGraceNegative NoStaleTimerRevocation GraceExpires
run_model OCIImageMaterialization OCIImageMaterialization
run_negative OCIImageMaterialization OCIImageMaterializationProjectionNegative UnavailableImageIsNotProjected ObserveImage
run_negative OCIImageMaterialization OCIImageMaterializationCreateNegative CreateRequiresExactMaterial CreateContainer
run_negative OCIImageMaterialization OCIImageMaterializationDigestNegative ConflictingArchiveDoesNotReplace RetryConflictingArchive
run_negative OCIImageMaterialization OCIImageMaterializationBuildImportNegative BuildUnpackUsesRuntimeContent UnpackBuild
run_negative OCIImageMaterialization OCIImageMaterializationBuildCommitNegative BuildPublicationRequiresMaterial PublishBuild
run_model BuildMaterializationBudget BuildMaterializationBudget
run_negative BuildMaterializationBudget BuildMaterializationBudgetReservationNegative NoSpuriousMetadataRejection Metadata
run_negative BuildMaterializationBudget BuildMaterializationBudgetSlotNegative AdmittedBuildWaits RejectOccupied
run_negative BuildMaterializationBudget BuildMaterializationBudgetCancelNegative CanceledWaiterDoesNotRun Acquire
run_model ManagedCredentialReplacement ManagedCredentialReplacement
run_negative ManagedCredentialReplacement ManagedCredentialReplacementUnexpiredNegative ReplacementRequiresExpired ReplaceBeforeExpiry
run_negative ManagedCredentialReplacement ManagedCredentialReplacementForgedNegative ForgedCredentialNeverStored InstallForged
run_model WorkloadRunLifecycle WorkloadRunLifecycle
run_negative WorkloadRunLifecycle WorkloadRunLifecycleAdmissionNegative ActiveBound BeginStart
run_negative WorkloadRunLifecycle WorkloadRunLifecycleExecutionNegative ExecutionRequiresCurrentInstance Acquire
run_negative WorkloadRunLifecycle WorkloadRunLifecycleQuiesceNegative QuiesceExcludesExecution Acquire
run_negative WorkloadRunLifecycle WorkloadRunLifecycleStopNegative ExactStopObservation ObserveStopped
run_model OCIContentLifecycle OCIContentLifecycle
run_negative OCIContentLifecycle OCIContentLifecycleReferenceNegative ReferencedContentNotDeleted GC
run_negative OCIContentLifecycle OCIContentLifecycleConcurrentImportNegative SingleGlobalImportOwner ReserveImport
run_negative OCIContentLifecycle OCIContentLifecycleCriticalNegative CriticalWatermarkRejectsAdmission ReserveImport
run_negative OCIContentLifecycle OCIContentLifecycleSealNegative NoGrowthAfterSeal Download
run_model GuestDiskBudget GuestDiskBudget
run_negative GuestDiskBudget GuestDiskBudgetRetainedNegative DiskInvariant RetainRelease
run_negative GuestDiskBudget GuestDiskBudgetCacheNegative DiskInvariant CacheGrow
run_negative GuestDiskBudget GuestDiskBudgetSealNegative NoGrowthAfterSeal WriteImport
run_model GuestOCIStartup GuestOCIStartup
run_negative GuestOCIStartup GuestOCIStartupDrainNegative ExistingDrainBeforeTighten TightenExisting
run_model OCIQuotaSettlementNamespace OCIQuotaSettlementNamespace
run_negative OCIQuotaSettlementNamespace OCIQuotaSettlementNamespaceRaceNegative SettlementNeverRecreatesDeletedNamespace RunSynchronousBarrier
run_model DomainAuthorityIsolation DomainAuthorityIsolation
run_negative DomainAuthorityIsolation DomainAuthorityIsolationIdentityNegative ExactRegistrationIdentity WrongIdentityCommand
run_negative DomainAuthorityIsolation DomainAuthorityIsolationIsolationNegative DomainNonInterference CrossDomainFailure
run_model SystemInventorySeparation SystemInventorySeparation
run_negative SystemInventorySeparation SystemInventorySeparationNegative SystemExcludedFromInventory ExposeSystem
run_model ApplianceActivationLifecycle ApplianceActivationLifecycle
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleVerificationNegative ActiveVerifiedExact Start
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBootstrapNegative BootstrapSingleUse ReplayBootstrap
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleInstanceNegative CurrentInstanceFence StaleInstance
run_negative ApplianceActivationLifecycle ApplianceActivationLifecyclePublicationNegative PublicationFence UnsafePublishBeforeFence
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBootstrapFenceNegative InitialSeedDeletionFence UnsafeConsumeSeed
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleSealedNegative SealedRecoveryIsBounded UnsafeReopenSealed
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleRemoveNegative RemoveRevisionExact ExecuteRemove
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBundleNegative ActiveUsesBundledArtifact BundledArtifactUpgrade
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBundleValidationNegative RejectedBundleDoesNotReplace RejectInvalidBundledArtifact
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBundleDomainNegative BundledUpgradePreservesDomain BundledArtifactUpgrade
run_negative ApplianceActivationLifecycle ApplianceActivationLifecycleBundleIsolationNegative BundledUpgradeIsIsolated BundledArtifactUpgrade
run_model ApplianceInstallationIsolation ApplianceInstallationIsolation
run_negative ApplianceInstallationIsolation ApplianceInstallationIsolationNegative InstallationNonInterference CrossDomainRemove
run_model HostServiceOwnership HostServiceOwnership
run_negative HostServiceOwnership HostServiceOwnershipExclusiveNegative SingleServiceOwner SelectAgent
run_negative HostServiceOwnership HostServiceOwnershipDefinitionNegative SingleServiceDefinition SelectAgent
run_negative HostServiceOwnership HostServiceOwnershipAutoNegative AgentRequiresGUI RejectUnavailable
run_model HostServiceExistingOwner HostServiceExistingOwner
run_negative HostServiceExistingOwner HostServiceExistingOwnerObserveNegative ObservedConfiguredOwner Observe
run_negative HostServiceExistingOwner HostServiceExistingOwnerRemoveNegative RemovedConfiguredOwner Remove
run_negative HostServiceExistingOwner HostServiceExistingOwnerResourceNegative UpdatedResourceGeometry Update
run_negative HostServiceExistingOwner HostServiceExistingOwnerPolicyNegative UpdatedPairingPolicy Update
run_model ApplianceRecoverySelection ApplianceRecoverySelection
run_negative ApplianceRecoverySelection ApplianceRecoverySelectionSnapshotNegative NoDuplicateLaunch Launch
run_negative ApplianceRecoverySelection ApplianceRecoverySelectionRevisionNegative NoDuplicateLaunch Launch
run_negative ApplianceRecoverySelection ApplianceRecoverySelectionReadyNegative SynchronousCompletion SyncReturn
run_model HostVMRestartRecovery HostVMRestartRecovery
run_model HostVMRestartRecovery HostVMRestartRecoveryFailure
run_negative HostVMRestartRecovery HostVMRestartRecoveryGuardNegative NoFreshVMStart ResumeSuccess

echo "Canonical TLC suites passed"
