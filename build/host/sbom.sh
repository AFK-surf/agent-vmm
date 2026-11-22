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
syft scan "dir:$host" \
  --exclude '**/Contents/Resources/Guest/**' --exclude '**/guest/**' \
  -o "spdx-json=$output/host.spdx.json"
worker_source=$(mktemp -d)
trap 'rm -rf "$worker_source"' EXIT
mkdir -p "$worker_source/bins" "$worker_source/crates"
cp -R "$repository/bins/agent-vmm-data" "$worker_source/bins/"
cp -R "$repository/crates/vmm-data" "$repository/crates/netdev-compat" "$worker_source/crates/"
syft scan "dir:$worker_source" --source-name agent-vmm-data -o "spdx-json=$output/data-worker.spdx.json"
