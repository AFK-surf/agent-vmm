#!/bin/busybox sh
set -eu
export PATH=/bin:/sbin
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs tmpfs /tmp
modprobe virtio_pci
modprobe virtiofs
mkdir -p /work
mount -t virtiofs kvm-dev /work
set +e
/bin/busybox sh /work/run.sh > /work/result.log 2>&1
result=$?
echo "$result" >/work/result.status
/bin/busybox sync
/bin/busybox poweroff -f
exit "$result"
