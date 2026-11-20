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
mkdir -p "$root/var/lib/agent-vmm"
rm -f "$root/etc/resolv.conf"
ln -s /run/resolv.conf "$root/etc/resolv.conf"
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
mkdir -p "$root/usr/lib/agent-vmm/helper-root"
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
if [ -d "$root/lib/modules" ]; then
  cp -a "$root/lib/modules" "$initramfs/lib/"
fi
cp "$repository/build/guest/initramfs/init" "$initramfs/init"
chmod 0755 "$initramfs/init"
for applet in mount modprobe switch_root sh basename dirname tr mkdir; do ln -s /bin/busybox "$initramfs/bin/$applet"; done
(cd "$initramfs" && find . -print0 | sort -z | cpio --null -o --format=newc --owner=0:0 --reproducible 2>/dev/null >"$work/dom0-initrd.cpio")
gzip -9 -n -c "$work/dom0-initrd.cpio" >"$work/dom0-initrd.cpio.gz"
if [ "$profile" = development ]; then
  initramfs_xz='-T0 -1'
else
  initramfs_xz='--threads=1 -9e'
fi
xz $initramfs_xz --check=crc32 -c "$work/dom0-initrd.cpio" >"$bundle/initramfs.xz"

# Assemble the l4h Apple VZ flat ARM64 Image (seL4 GICv3 + l4h-root + ARCH=l4h Linux 6.18.64 dom0 + dom0-initrd + l4h.conf).
l4h_dir=${L4H_ARTIFACTS_DIR:-/l4h-artifacts}
python3 "$repository/third_party/l4h/tools/add-modules.py" \
  "$l4h_dir/l4h-root.stripped" "$work/root-dom0.elf" \
  "guest-kernel=$l4h_dir/vmlinux.stripped" \
  "dom0-initrd=$work/dom0-initrd.cpio.gz" \
  "l4h-config=$repository/build/guest/l4h.conf"
"$l4h_dir/sel4-kernel-loader-add-payload" \
  --loader "$l4h_dir/sel4-kernel-loader" \
  --sel4-prefix "$l4h_dir/sel4-prefix" \
  --app "$work/root-dom0.elf" \
  -o "$work/l4h-dom0.elf"
python3 - "$work/l4h-dom0.elf" "$bundle/Image" << 'PY'
import struct, sys
elf = open(sys.argv[1], "rb").read()
phoff = struct.unpack_from("<Q", elf, 32)[0]
phentsize, phnum = struct.unpack_from("<HH", elf, 54)
loads = []
for i in range(phnum):
    p_type, p_flags, p_offset, p_vaddr, p_paddr, p_filesz, p_memsz, p_align = struct.unpack_from("<IIQQQQQQ", elf, phoff + i * phentsize)
    if p_type == 1 and p_memsz > 0:
        loads.append((p_paddr, p_offset, p_filesz, p_memsz))
base = min(x[0] for x in loads)
file_end = max(x[0] + x[2] for x in loads)
mem_end = max(x[0] + x[3] for x in loads)
img = bytearray(file_end - base)
for p_paddr, p_offset, p_filesz, p_memsz in loads:
    if p_filesz > 0:
        img[p_paddr - base : p_paddr - base + p_filesz] = elf[p_offset : p_offset + p_filesz]
image_size = ((mem_end - base) + 0x1fffff) & ~0x1fffff
struct.pack_into("<Q", img, 16, image_size)
open(sys.argv[2], "wb").write(img)
PY
test -s "$bundle/Image"

cat >> "$bundle/THIRD_PARTY_NOTICES" << 'NOTICE'

================================================================================
seL4 Microkernel (GPL-2.0-only) / rust-sel4 (Apache-2.0 / MIT) / l4h / Linux 6.18.64 (GPL-2.0-only)
================================================================================
This guest image embeds the seL4 microkernel (aarch64 apple-vz GICv3),
sel4-kernel-loader, l4h host components (l4h-root, l4h-logd, l4h-memd, l4h-blkd,
l4h-netd, l4h-nicd-virtio, l4h-vmmgr, l4h-vmm), and the ARCH=l4h Linux 6.18.64 kernel.
NOTICE

workload_initramfs=$work/workload-initramfs
mkdir -p "$workload_initramfs/sbin" "$workload_initramfs/etc" "$workload_initramfs/lib" "$workload_initramfs/usr/lib"
install -m 0755 "$root/usr/bin/vmm-guest-init" "$workload_initramfs/init"
if [ -x "$root/sbin/mkfs.ext4" ]; then
  cp -L "$root/sbin/mkfs.ext4" "$workload_initramfs/sbin/mkfs.ext4"
  chmod 0755 "$workload_initramfs/sbin/mkfs.ext4"
  [ -f "$root/etc/mke2fs.conf" ] && cp -L "$root/etc/mke2fs.conf" "$workload_initramfs/etc/mke2fs.conf"
  for lib in ld-musl-aarch64.so.1 libc.musl-aarch64.so.1; do
    [ -e "$root/lib/$lib" ] && cp -a "$root/lib/$lib" "$workload_initramfs/lib/"
  done
  for pat in 'libext2fs.so*' 'libe2p.so*' 'libcom_err.so*' 'libblkid.so*' 'libuuid.so*' 'libeconf.so*' 'libgcc_s.so*'; do
    for src in "$root/lib"/$pat "$root/usr/lib"/$pat; do
      [ -e "$src" ] || continue
      case "$src" in
        "$root/lib"/*) cp -a "$src" "$workload_initramfs/lib/" ;;
        "$root/usr/lib"/*) cp -a "$src" "$workload_initramfs/usr/lib/" ;;
      esac
    done
  done
  # Also resolve full transitive DT_NEEDED closure when readelf is available.
  if command -v readelf >/dev/null 2>&1; then
    queue="$workload_initramfs/sbin/mkfs.ext4"
    seen=""
    while [ -n "$queue" ]; do
      cur=$(printf '%s\n' "$queue" | head -n1)
      queue=$(printf '%s\n' "$queue" | tail -n +2)
      case " $seen " in *" $cur "*) continue ;; esac
      seen="$seen $cur"
      for needed in $(readelf -d "$cur" 2>/dev/null | sed -n 's/.*(NEEDED).*Shared library: \[\(.*\)\].*/\1/p'); do
        for dir in lib usr/lib; do
          if [ -e "$root/$dir/$needed" ] && [ ! -e "$workload_initramfs/$dir/$needed" ]; then
            cp -L "$root/$dir/$needed" "$workload_initramfs/$dir/$needed"
            queue="${queue:+$queue
}$workload_initramfs/$dir/$needed"
          fi
        done
      done
    done
  fi
fi
python3 - "$workload_initramfs" "$root/usr/lib/agent-vmm/workload-initramfs.cpio" << 'PY'
import os, stat, sys

src_root, out_path = sys.argv[1], sys.argv[2]

def entry(name, mode, data=b"", rdev_maj=0, rdev_min=0, ino=1):
    name_b = name.encode() + b"\x00"
    nlink = 2 if stat.S_ISDIR(mode) else 1
    hdr = (
        f"070701{ino:08x}{mode:08x}{0:08x}{0:08x}{nlink:08x}{0:08x}"
        f"{len(data):08x}{0:08x}{0:08x}{rdev_maj:08x}{rdev_min:08x}"
        f"{len(name_b):08x}{0:08x}"
    ).encode()
    def pad4(n): return b"\x00" * ((4 - (n % 4)) % 4)
    return hdr + name_b + pad4(len(hdr) + len(name_b)) + data + pad4(len(data))

ino = 1
chunks = []
# Device nodes required before PID 1 main() runs (Rust std sanitize_standard_fds + kernel console).
for name, mode, maj, min_ in [
    ("dev", stat.S_IFDIR | 0o755, 0, 0),
    ("dev/console", stat.S_IFCHR | 0o600, 5, 1),
    ("dev/null", stat.S_IFCHR | 0o666, 1, 3),
]:
    chunks.append(entry(name, mode, rdev_maj=maj, rdev_min=min_, ino=ino))
    ino += 1

paths = []
for dirpath, dirnames, filenames in os.walk(src_root):
    dirnames.sort()
    filenames.sort()
    rel_dir = os.path.relpath(dirpath, src_root)
    if rel_dir != ".":
        paths.append(rel_dir)
    for fn in filenames:
        paths.append(fn if rel_dir == "." else os.path.join(rel_dir, fn))

for rel in sorted(paths):
    full = os.path.join(src_root, rel)
    st = os.lstat(full)
    if stat.S_ISDIR(st.st_mode):
        chunks.append(entry(rel, stat.S_IFDIR | stat.S_IMODE(st.st_mode), ino=ino))
    elif stat.S_ISLNK(st.st_mode):
        chunks.append(entry(rel, stat.S_IFLNK | 0o777, os.readlink(full).encode(), ino=ino))
    else:
        chunks.append(entry(rel, stat.S_IFREG | stat.S_IMODE(st.st_mode), open(full, "rb").read(), ino=ino))
    ino += 1

chunks.append(entry("TRAILER!!!", 0, ino=0))
with open(out_path, "wb") as f:
    for c in chunks:
        f.write(c)
PY

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
