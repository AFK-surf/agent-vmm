#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$repo"

go test -race ./test/remote/... -count=1

if [ -z "${AGENT_VMM_SALIX_ENDPOINT:-}" ]; then
	echo "environment_blocked: AGENT_VMM_SALIX_ENDPOINT is not configured; external Salix conformance was not claimed"
	exit 0
fi

echo "AGENT_VMM_SALIX_ENDPOINT is configured, but the external adapter belongs to the Salix repository" >&2
echo "run the Salix-owned adapter conformance against api/remote/v1/remote.proto" >&2
exit 2
