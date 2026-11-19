#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  echo 'usage: sbom.sh ROOTFS OUTPUT' >&2
  exit 2
fi
rootfs=$(CDPATH= cd -- "$1" && pwd)
mkdir -p "$2"
output=$(CDPATH= cd -- "$2" && pwd)
syft scan "dir:$rootfs" -o "spdx-json=$output/guest-rootfs.spdx.json"
syft scan "oci-archive:$rootfs/usr/lib/agent-vmm/buildkit.oci.tar" -o "spdx-json=$output/guest-buildkit.spdx.json"
