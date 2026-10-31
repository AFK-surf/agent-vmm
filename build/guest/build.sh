#!/bin/sh
set -eu
if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo 'usage: build.sh OUTPUT_DIRECTORY [BUILD_PROFILE] [BUNDLE_FLAVOR]' >&2
  exit 2
fi
output=$1
profile=${2:-development}
flavor=${3:-standalone}
case "$profile" in development|release) ;; *) echo 'build profile must be development or release' >&2; exit 2 ;; esac
case "$flavor" in runtime|standalone) ;; *) echo 'bundle flavor must be runtime or standalone' >&2; exit 2 ;; esac
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
set -- docker buildx build --platform linux/arm64 \
  --file "$repository/build/guest/Dockerfile" --build-arg "BUILD_PROFILE=$profile" \
  --build-arg "BUNDLE_FLAVOR=$flavor" --build-arg "GOTOOLCHAIN=$(awk '$1 == "toolchain" {print $2}' "$repository/go.mod")" --output "type=local,dest=$output"
if [ "${AGENT_VMM_REFRESH_DEPENDENCIES:-0}" = 1 ]; then
  set -- "$@" --pull --no-cache-filter tools,buildkit,dependencies
fi
if [ -n "${AGENT_VMM_BUILDX_BUILDER:-}" ]; then
  set -- "$@" --builder "$AGENT_VMM_BUILDX_BUILDER"
fi
"$@" "$repository"
if [ "$flavor" = runtime ]; then
  rm -f "$output/mcp-appliance.digest" "$output/mcp-appliance/manifest.pb"
  if [ -d "$output/mcp-appliance" ]; then
    rmdir "$output/mcp-appliance" 2>/dev/null || true
  fi
fi
