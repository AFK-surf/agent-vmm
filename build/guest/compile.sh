#!/bin/sh
set -eu
set -o pipefail
mkdir -p /binaries
export GOOS=linux GOARCH=arm64 CGO_ENABLED=0
for command in agent-vmmd agent-vmm-supervisor agent-vmm-mcp-appliance; do
  go build -trimpath -ldflags='-s -w -buildid=' -o "/binaries/$command" "./cmd/$command"
done
for command in oci bundle; do
  go build -trimpath -ldflags='-s -w -buildid=' -o "/binaries/$command" "./build/guest/$command"
done
go build -trimpath -ldflags='-s -w -buildid=' -o /binaries/appliance-manifest ./build/appliance/manifest
go build -trimpath -ldflags='-s -w -buildid=' -o /binaries/notices ./build/notices
