#!/bin/sh
set -eu
set -o pipefail
profile=$1
flavor=$2
repository=/src
binaries=/binaries
work=/assembly
root=$work/root
bundle=$work/bundle
mkdir -p "$work"
cp -a /dependencies/root "$root"
cp -a /dependencies/bundle "$bundle"
mkdir -p "$work/builder-root"
test -x "$work/builder-root/usr/bin/rootlesskit"
test -x "$work/builder-root/usr/bin/buildkit-runc"
mkdir -p "$work/builder-root/sys/fs/cgroup" "$work/builder-root/workspace" "$work/builder-root/output" "$work/builder-root/run/secrets" "$work/builder-root/home/user/.local/share/buildkit" "$work/builder-root/base-image"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$work/builder-root.tar" -C "$work/builder-root" .
mkdir -p "$root/usr/lib/agent-vmm"
("$binaries/oci" "$work/builder-root.tar" "$root/usr/lib/agent-vmm/buildkit.oci.tar")

install -m 0755 "$binaries/agent-vmmd" "$root/usr/bin/agent-vmmd"
install -m 0755 "$binaries/agent-vmm-supervisor" "$root/usr/bin/agent-vmm-supervisor"
if [ "$flavor" = standalone ]; then
  mcp_root=$work/mcp-root
  mkdir -p "$mcp_root/opt/agent-vmm/content" "$mcp_root/usr/bin" "$mcp_root/run/agent-vmm" "$mcp_root/var/lib/agent-vmm" "$mcp_root/etc"
  touch "$mcp_root/etc/resolv.conf" "$mcp_root/.agent-vmm-supervisor"
  chmod 0755 "$mcp_root/.agent-vmm-supervisor"
  install -m 0755 "$binaries/agent-vmm-mcp-appliance" "$mcp_root/usr/bin/agent-vmm-mcp-appliance"
  find "$mcp_root" -exec touch -h -d @0 {} +
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$work/mcp-root.tar" -C "$mcp_root" .
  ("$binaries/oci" "$work/mcp-root.tar" "$root/usr/lib/agent-vmm/mcp-appliance.oci.tar" /usr/bin/agent-vmm-mcp-appliance)
  tar -xOf "$root/usr/lib/agent-vmm/mcp-appliance.oci.tar" index.json | jq -er '.manifests | select(length == 1) | .[0].digest' >"$root/usr/lib/agent-vmm/mcp-appliance.digest"
  cp "$root/usr/lib/agent-vmm/mcp-appliance.digest" "$bundle/mcp-appliance.digest"
  mkdir -p "$bundle/mcp-appliance"
  ("$binaries/appliance-manifest" -output "$bundle/mcp-appliance" -image-digest "$(cat "$bundle/mcp-appliance.digest")" -release 1)
fi
cp -a "$repository/build/guest/root/." "$root/"
ln -snf /run/resolv.conf "$root/etc/resolv.conf"
mkdir -p "$root/control" "$root/state" "$root/var/lib/cni" "$root/opt/containerd" "$root/etc/cni/net.d"
test -L "$root/usr/bin/md5sum" || test -e "$root/usr/bin/md5sum" || ln -s /bin/busybox "$root/usr/bin/md5sum"
chmod 0755 "$root/etc/init.d/agent-vmmd"
ln -s /etc/init.d/agent-vmmd "$root/etc/runlevels/default/agent-vmmd"
for service in devfs dmesg procfs sysfs cgroups; do ln -s "/etc/init.d/$service" "$root/etc/runlevels/sysinit/$service"; done
for service in modules sysctl hostname networking; do ln -s "/etc/init.d/$service" "$root/etc/runlevels/boot/$service"; done
find "$root/usr/libexec/cni" -type f ! -name bridge ! -name host-local ! -name loopback -delete
rm -f "$root/usr/bin/runc" "$root/usr/bin/containerd-shim-runc-v2" "$root/usr/bin/ctr"
test ! -e "$root/usr/bin/runc"
test ! -e "$root/usr/bin/containerd-shim-runc-v2"
test ! -e "$root/usr/bin/ctr"

set -- -go-binary "$root" -go-binary "$work/builder-root" \
  -apk-db "$root/lib/apk/db/installed" -apk-db "$work/builder-root/lib/apk/db/installed"
if [ "$flavor" = standalone ]; then
  set -- "$@" -go-binary "$mcp_root"
fi
"$binaries/notices" "$@" -extra gvisor.dev/gvisor=Apache-2.0 \
  -licenses "$repository/third_party/licenses" -o "$bundle/THIRD_PARTY_NOTICES"

initramfs=$work/initramfs
mkdir -p "$initramfs/bin" "$initramfs/sbin" "$initramfs/lib" "$initramfs/dev" "$initramfs/proc" "$initramfs/sys" "$initramfs/newroot"
cp "$root/bin/busybox" "$initramfs/bin/busybox"
cp "$root/lib/ld-musl-aarch64.so.1" "$initramfs/lib/"
cp "$root/lib/libc.musl-aarch64.so.1" "$initramfs/lib/"
cp -a "$root/lib/modules" "$initramfs/lib/"
cp "$repository/build/guest/initramfs/init" "$initramfs/init"
chmod 0755 "$initramfs/init"
for applet in mount modprobe switch_root sh basename dirname tr mkdir; do ln -s /bin/busybox "$initramfs/bin/$applet"; done
if [ "$profile" = development ]; then
  initramfs_xz='-T0 -1'
else
  initramfs_xz='--threads=1 -9e'
fi
(cd "$initramfs" && find . -print0 | sort -z | cpio --null -o --format=newc --owner=0:0 --reproducible 2>/dev/null | xz $initramfs_xz --check=crc32 >"$bundle/initramfs.xz")

find "$root" -exec touch -h -d @0 {} +
find "$root" -printf '%p 0\n' | LC_ALL=C sort >"$work/squashfs.sort"
if [ "$profile" = development ]; then
  processors=$(nproc)
  [ "$processors" -le 8 ] || processors=8
  mksquashfs "$root" "$bundle/rootfs.squashfs" -noappend -all-root -all-time 0 -mkfs-time 0 -comp zstd -Xcompression-level 1 -b 1M -sort "$work/squashfs.sort" -processors "$processors"
  rootfs_compressor=zstd
  rootfs_dictionary=0
  rootfs_filter=none
else
  mksquashfs "$root" "$bundle/rootfs.squashfs" -noappend -all-root -all-time 0 -mkfs-time 0 -comp xz -b 1M -Xdict-size 1M -Xbcj arm64 -sort "$work/squashfs.sort" -processors 1
  rootfs_compressor=xz
  rootfs_dictionary=1048576
  rootfs_filter=arm64-bcj
fi
bundle_flags=
if [ "$flavor" = standalone ]; then
  bundle_flags=-bundled-mcp
fi
("$binaries/bundle" -directory "$bundle" -rootfs-compressor "$rootfs_compressor" -rootfs-dictionary "$rootfs_dictionary" -rootfs-filter "$rootfs_filter" $bundle_flags)
