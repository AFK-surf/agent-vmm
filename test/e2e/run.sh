#!/bin/sh
set -u

# TestInstallationChainE2E is skipped here unless AGENT_VMM_E2E_INSTALL_CHAIN=1.
# It runs the real lifecycle helper and launchd under an isolated label and
# home. See docs/contributing/testing.md.

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
bundle=${AGENT_VMM_E2E_BUNDLE:-$scratch/guest-bundle}
host_binary=${AGENT_VMM_E2E_HOST_BINARY:-$scratch/agent-vmm-host}
data_worker_binary=${AGENT_VMM_E2E_DATA_BINARY:-$scratch/agent-vmm-data}
json=$repository/.build/test-results/e2e/test.json
report=$repository/.build/test-results/e2e/report.md
mkdir -p "$repository/.build/test-results/e2e"

if [ -z "${AGENT_VMM_E2E_BUNDLE:-}" ]; then
  "$repository/build/guest/build.sh" "$bundle"
  prepare_status=$?
else
  prepare_status=0
fi

if [ "$prepare_status" -eq 0 ]; then
  guest_manifest_digest=$(shasum -a 256 "$bundle/manifest.json" | awk '{print $1}') && \
  (cd "$repository" && go build -tags=e2e -ldflags "-X main.guestManifestDigest=$guest_manifest_digest" -o "$host_binary" ./cmd/agent-vmm-host) && \
    codesign --force --sign - --entitlements "$repository/build/host/vz.entitlements.plist" "$host_binary"
  prepare_status=$?
fi

if [ "$prepare_status" -eq 0 ] && [ -z "${AGENT_VMM_E2E_DATA_BINARY:-}" ]; then
  (cd "$repository" && cargo build --locked --release --manifest-path data/agent-vmm-data/Cargo.toml && cp data/agent-vmm-data/target/release/agent-vmm-data "$data_worker_binary")
  prepare_status=$?
fi

if [ "$prepare_status" -eq 0 ]; then
  (cd "$repository" && AGENT_VMM_E2E_BUNDLE="$bundle" AGENT_VMM_E2E_HOST_BINARY="$host_binary" \
    AGENT_VMM_E2E_DATA_BINARY="$data_worker_binary" \
    AGENT_VMM_E2E_ARCHIVE_CACHE="${AGENT_VMM_E2E_ARCHIVE_CACHE:-$repository/.build/e2e-archives}" \
    go test -json -tags=e2e ./test/e2e -count=1 -timeout=45m) >"$json"
  test_status=$?
else
  test_status=$prepare_status
  printf '{"Action":"fail","Package":"github.com/AFK-surf/agent-vmm/test/e2e","Output":"E2E preparation failed"}\n' >"$json"
fi

cat "$json"
(cd "$repository" && go run ./test/e2e/reportcmd -repository "$repository" -bundle "$bundle" -json "$json" -report "$report" -status "$test_status")
report_status=$?
if [ "$test_status" -ne 0 ]; then
  exit "$test_status"
fi
exit "$report_status"
