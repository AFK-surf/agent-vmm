#!/bin/sh
set -eu

version=1.8.0
expected_sha=ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f
cache_root=${AGENT_VMM_DOWNLOAD_CACHE:-${TMPDIR:-/tmp}/agent-vmm-download-cache}
jar="$cache_root/tla2tools-$version.jar"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/agent-vmm-tla.XXXXXX")
spec_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../../spec/tla" && pwd)

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
  if [ -n "$expected_action" ] && ! grep -q "<$expected_action(" "$output"; then
    echo "$config did not reach $expected_action in its counterexample" >&2
    sed -n '1,160p' "$output" >&2
    exit 1
  fi
}

run_model ControllerDomainControl ControllerDomainControl
run_negative ControllerDomainControl ControllerDomainControlReadyNegative NoCommandBeforeReady
run_negative ControllerDomainControl ControllerDomainControlIdentityNegative ExactRegistrationIdentity
run_negative ControllerDomainControl ControllerDomainControlWatermarkNegative ExactInventorySettlement
run_negative ControllerDomainControl ControllerDomainControlAdmissionNegative AggregateNoOvercommit
run_negative ControllerDomainControl ControllerDomainControlSessionNegative SingleCurrentCapability
run_negative ControllerDomainControl ControllerDomainControlRevokeNegative RevokeIsTerminal
run_negative ControllerDomainControl ControllerDomainControlIsolationNegative DomainNonInterference
run_negative ControllerDomainControl ControllerDomainControlSystemNegative SystemExcludedFromInventory
run_negative ControllerDomainControl ControllerDomainControlSequenceNegative CommandFenceExact Allocate
run_model ApplianceLifecycle ApplianceLifecycle
run_negative ApplianceLifecycle ApplianceLifecycleVerificationNegative ActiveVerifiedExact Start
run_negative ApplianceLifecycle ApplianceLifecycleBootstrapNegative BootstrapSingleUse ReplayBootstrap
run_negative ApplianceLifecycle ApplianceLifecycleInstanceNegative CurrentInstanceFence StaleInstance
run_negative ApplianceLifecycle ApplianceLifecycleIsolationNegative InstallationNonInterference CrossDomainRemove
run_negative ApplianceLifecycle ApplianceLifecycleSealedNegative SealedRecoveryIsBounded UnsafeReopenSealed
run_negative ApplianceLifecycle ApplianceLifecycleRemoveNegative RemoveRevisionExact ExecuteRemove
run_negative ApplianceLifecycle ApplianceLifecyclePublicationNegative PublicationFence UnsafePublishBeforeFence
run_negative ApplianceLifecycle ApplianceLifecycleBootstrapFenceNegative InitialSeedDeletionFence UnsafeConsumeSeed

echo "RFC6 TLC suites passed"
