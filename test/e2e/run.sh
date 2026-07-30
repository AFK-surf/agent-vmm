#!/bin/sh
set -u

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
bundle=${AGENT_VMM_E2E_BUNDLE:-/tmp/agent-vmm-e2e-bundle}
host_binary=${AGENT_VMM_E2E_HOST_BINARY:-/tmp/agent-vmm-e2e-host}
json=$repository/.context/e2e/test.json
report=$repository/.context/e2e/report.md
mkdir -p "$repository/.context/e2e"

if [ -z "${AGENT_VMM_E2E_BUNDLE:-}" ]; then
  mkdir -p "$bundle"
  docker run --rm --platform linux/arm64 --privileged \
    -v "$repository:/src" -v "$bundle:/bundle" -w /src alpine:3.24 \
    sh -lc 'apk add --no-cache bash bzip2 coreutils cpio curl file findutils git go grep jq squashfs-tools tar xz && build/guest/build.sh /bundle'
  prepare_status=$?
else
  prepare_status=0
fi

if [ "$prepare_status" -eq 0 ]; then
  (cd "$repository" && go build -tags=e2e -o "$host_binary" ./cmd/agent-vmm-host) && \
    codesign --force --sign - --entitlements "$repository/build/host/vz.entitlements.plist" "$host_binary"
  prepare_status=$?
fi

if [ "$prepare_status" -eq 0 ]; then
  (cd "$repository" && AGENT_VMM_E2E_BUNDLE="$bundle" AGENT_VMM_E2E_HOST_BINARY="$host_binary" \
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
