#!/bin/sh
set -eu
set -o pipefail
profile=$1
repository=/src
binaries=/binaries
work=/assembly
root=$work/root
bundle=$work/bundle
builder=$work/builder-root
mkdir -p "$work"
cp -a /dependencies/root "$root"
cp -a /dependencies/bundle "$bundle"
install -m 0755 "$binaries/l4h-dom0d" "$binaries/vmm-guest-init" "$root/usr/bin/"
cp -a "$repository/build/guest/root/." "$root/"
rm -f "$root/etc/resolv.conf"
ln -s /etc/init.d/l4h-dom0d "$root/etc/runlevels/default/l4h-dom0d"
for service in devfs dmesg procfs sysfs cgroups; do ln -s "/etc/init.d/$service" "$root/etc/runlevels/sysinit/$service"; done
for service in modules sysctl hostname networking; do ln -s "/etc/init.d/$service" "$root/etc/runlevels/boot/$service"; done
for binary in l4h-dom0d vmm-guest-init; do
  file -b "$root/usr/bin/$binary" | grep -Eq 'ARM aarch64.*(statically|static-pie) linked'
done

for binary in rootlesskit buildkit-runc buildkitd buildctl; do
  test -x "$builder/usr/bin/$binary"
done
# The layer reader accepts files, directories, and links.
find "$builder" \( -type b -o -type c -o -type p -o -type s \) -delete
mkdir -p "$builder/sys/fs/cgroup" "$builder/workspace" "$builder/output" "$builder/run/secrets" \
  "$builder/home/user/.local/share/buildkit" "$builder/base-image"
find "$builder" -exec touch -h -d @0 {} +
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$work/builder-root.tar" -C "$builder" .
mkdir -p "$root/usr/lib/agent-vmm"
guest-oci "$work/builder-root.tar" "$root/usr/lib/agent-vmm/buildkit.oci.tar"
tar -xOf "$root/usr/lib/agent-vmm/buildkit.oci.tar" index.json \
  | jq -er '.manifests | select(length == 1) | .[0].digest' \
  > "$root/usr/lib/agent-vmm/buildkit.digest"

guest-notices -apk-db "$root/lib/apk/db/installed" -apk-db "$builder/lib/apk/db/installed" \
  -go-binary "$builder/usr/bin/buildkitd" -go-binary "$builder/usr/bin/buildctl" \
  -licenses "$repository/third_party/licenses" -o "$bundle/THIRD_PARTY_NOTICES"
cat "$binaries/THIRD_PARTY_NOTICES" >> "$bundle/THIRD_PARTY_NOTICES"

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

workload_initramfs=$work/workload-initramfs
mkdir -p "$workload_initramfs"
install -m 0755 "$root/usr/bin/vmm-guest-init" "$workload_initramfs/init"
touch -d @0 "$workload_initramfs/init"
(cd "$workload_initramfs" && printf './init\0' | cpio --null -o --format=newc --owner=0:0 --reproducible \
  > "$root/usr/lib/agent-vmm/workload-initramfs.cpio")

sh /usr/local/bin/guest-smoke "$root"
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
artifact() {
  path=$1
  digest=$(sha256sum "$bundle/$path" | cut -d ' ' -f 1)
  jq -cn --arg path "$path" --arg sha256 "$digest" --argjson size "$(wc -c < "$bundle/$path")" \
    '{path: $path, size: $size, sha256: $sha256}'
}
jq -cn --argjson kernel "$(artifact Image)" --argjson initramfs "$(artifact initramfs.xz)" \
  --argjson rootfs "$(artifact rootfs.squashfs)" --arg compressor "$rootfs_compressor" \
  --argjson dictionary "$rootfs_dictionary" --arg filter "$rootfs_filter" \
  '{schema_version: 1, architecture: "arm64", artifacts: {kernel: $kernel, initramfs: $initramfs, rootfs: $rootfs},
    rootfs: {filesystem: "squashfs", compressor: $compressor, block_size: 1048576, dictionary_size: $dictionary, filter: $filter}}' \
  > "$bundle/manifest.json"
