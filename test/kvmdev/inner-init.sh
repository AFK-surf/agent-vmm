#!/bin/busybox sh
set -eu
export PATH=/bin:/sbin
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mkdir -p /tmp
mount -t tmpfs tmpfs /tmp
modprobe virtio_mmio
modprobe virtio_blk
modprobe virtio_rng
modprobe vmw_vsock_virtio_transport
exec /smoke inner
