#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  echo 'usage: sbom.sh HOST_DIR OUTPUT' >&2
  exit 2
fi
host=$(CDPATH= cd -- "$1" && pwd)
mkdir -p "$2"
output=$(CDPATH= cd -- "$2" && pwd)
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
SYFT_GOLANG_SEARCH_LOCAL_MOD_CACHE_LICENSES=true syft scan "dir:$host" \
  --exclude '**/Contents/Resources/Guest/**' --exclude '**/guest/**' \
  -o "spdx-json=$output/host.spdx.json"
syft scan "dir:$repository/data/agent-vmm-data" -o "spdx-json=$output/data-worker.spdx.json"
