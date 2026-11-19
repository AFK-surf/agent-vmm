#!/bin/sh
set -eu
if [ "$#" -ne 1 ]; then
  echo 'usage: smoke.sh ROOTFS' >&2
  exit 2
fi
root=$1
for name in l4h-dom0d vmm-guest-init; do
  test -x "$root/usr/bin/$name"
done
test -s "$root/usr/lib/agent-vmm/workload-initramfs.cpio"
test -s "$root/usr/lib/agent-vmm/buildkit.oci.tar"
test -s "$root/usr/lib/agent-vmm/buildkit.digest"
legacy=$(find "$root" \( -name runc -o -name containerd -o -name containerd-shim-runc-v2 \
  -o -name containerd-shim-runsc-v1 -o -name runsc \) -print)
if [ -n "$legacy" ]; then
  printf 'Obsolete Guest runtime files:\n%s\n' "$legacy" >&2
  exit 1
fi
