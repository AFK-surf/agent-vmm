#!/bin/sh
set -eu
if [ "$#" -ne 1 ]; then
  echo 'usage: smoke.sh ROOTFS' >&2
  exit 2
fi
"$1/usr/bin/runsc" --rootless --network=none do true
