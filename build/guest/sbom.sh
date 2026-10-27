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
for archive in "$rootfs"/usr/lib/agent-vmm/*.oci.tar; do
  [ -f "$archive" ] || continue
  name=$(basename "$archive" .oci.tar)
  syft scan "oci-archive:$archive" -o "spdx-json=$output/guest-$name.spdx.json"
done
