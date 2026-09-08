#!/bin/sh
set -eu
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != aarch64 ]; then
  echo 'guest artifacts must be built on Linux arm64' >&2
  exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
  echo 'guest artifacts require root for the isolated chroot package assembly' >&2
  exit 1
fi
command -v cpio >/dev/null 2>&1 || {
  echo 'guest artifact builds require cpio for the initramfs archive' >&2
  exit 1
}
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo 'usage: build.sh OUTPUT_DIRECTORY [BUILD_PROFILE]' >&2
  exit 2
fi

output=$1
profile=${2:-development}
case "$profile" in
  development|release) ;;
  *) echo 'build profile must be development or release' >&2; exit 2 ;;
esac
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
root=$work/root
bundle=$work/bundle
mkdir -p "$root" "$bundle" "$work/downloads"

fetch() {
  url=$1
  destination=$2
  partial=$destination.part
  curl -fsSL --retry 5 --retry-all-errors --output "$partial" "$url"
  mv "$partial" "$destination"
}

alpine_releases=$work/downloads/alpine-releases.yaml
fetch "https://dl-cdn.alpinelinux.org/alpine/latest-stable/releases/aarch64/latest-releases.yaml" "$alpine_releases"
alpine_miniroot=$(awk '
  $1 == "flavor:" { flavor = $2 }
  flavor == "alpine-minirootfs" && $1 == "file:" { print $2; exit }
' "$alpine_releases")
test -n "$alpine_miniroot"
miniroot=$work/downloads/alpine-minirootfs.tar.gz
fetch "https://dl-cdn.alpinelinux.org/alpine/latest-stable/releases/aarch64/$alpine_miniroot" "$miniroot"
tar -xzf "$miniroot" -C "$root"
cp /etc/resolv.conf "$root/etc/resolv.conf"
chroot "$root" /sbin/apk upgrade --no-cache
chroot "$root" /sbin/apk add --no-cache \
  openrc ca-certificates-bundle iproute2 nftables e2fsprogs e2fsprogs-extra \
  cni-plugins cryptsetup libcrypto3 libssl3 xz linux-virt containerd
for package in openrc ca-certificates-bundle iproute2 nftables e2fsprogs e2fsprogs-extra cni-plugins cryptsetup libcrypto3 libssl3 xz linux-virt containerd; do
  chroot "$root" /sbin/apk info -e "$package"
done

kernel_config=$(find "$root/boot" -name 'config-*' -type f)
for symbol in ARM64 BLK_DEV_DM BRIDGE CGROUPS CGROUP_BPF CRYPTO_AES CRYPTO_SHA256 CRYPTO_XTS DM_CRYPT DM_VERITY EXT4_FS INET NETFILTER NF_TABLES OVERLAY_FS RD_XZ SECCOMP SECCOMP_FILTER SQUASHFS SQUASHFS_XZ SQUASHFS_ZSTD TUN VETH VIRTIO VIRTIO_BALLOON VIRTIO_BLK VIRTIO_CONSOLE VIRTIO_NET VIRTIO_VSOCKETS VSOCKETS XZ_DEC XZ_DEC_ARM64; do
  grep -Eq "^CONFIG_${symbol}=[ym]$" "$kernel_config"
done
extract_vmlinux=$work/downloads/extract-vmlinux
fetch "https://raw.githubusercontent.com/torvalds/linux/master/scripts/extract-vmlinux" "$extract_vmlinux"
chmod 0755 "$extract_vmlinux"
"$extract_vmlinux" "$root/boot/vmlinuz-"* >"$bundle/Image"
test -s "$bundle/Image"

runsc_archive=$work/downloads/gvisor.tar.bz2
fetch "https://storage.googleapis.com/gvisor/releases/release/latest/aarch64/gvisor.tar.bz2" "$runsc_archive"
tar -xjf "$runsc_archive" -C "$work"
install -m 0755 "$work/runsc" "$root/usr/bin/runsc"
cp -a "$work/gvisor-bin" "$root/usr/bin/gvisor-bin"
install -m 0755 "$work/containerd-shim-runsc-v1" "$root/usr/bin/containerd-shim-runsc-v1"

mkdir -p "$work/builder-root"
if [ -n "${AGENT_VMM_BUILDKIT_ROOT_TAR:-}" ]; then
  test -f "$AGENT_VMM_BUILDKIT_ROOT_TAR"
  cp "$AGENT_VMM_BUILDKIT_ROOT_TAR" "$work/builder-root.tar"
else
  GOBIN=$work/tools go install github.com/google/go-containerregistry/cmd/crane@latest
  "$work/tools/crane" export --platform linux/arm64 docker.io/moby/buildkit:rootless "$work/builder-root.tar"
fi
tar -xf "$work/builder-root.tar" -C "$work/builder-root"
test -x "$work/builder-root/usr/bin/rootlesskit"
test -x "$work/builder-root/usr/bin/buildkit-runc"
mkdir -p "$work/builder-root/workspace" "$work/builder-root/output" "$work/builder-root/run/secrets" "$work/builder-root/home/user/.local/share/buildkit"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$work/builder-root.tar" -C "$work/builder-root" .
mkdir -p "$root/usr/lib/agent-vmm"
(cd "$repository" && go run ./build/guest/oci "$work/builder-root.tar" "$root/usr/lib/agent-vmm/buildkit.oci.tar")

(cd "$repository" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o "$root/usr/bin/agent-vmmd" ./cmd/agent-vmmd)
(cd "$repository" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o "$root/usr/bin/agent-vmm-supervisor" ./cmd/agent-vmm-supervisor)
mcp_root=$work/mcp-root
mkdir -p "$mcp_root/usr/bin"
(cd "$repository" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o "$mcp_root/usr/bin/agent-vmm-mcp-appliance" ./cmd/agent-vmm-mcp-appliance)
find "$mcp_root" -exec touch -h -d @0 {} +
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$work/mcp-root.tar" -C "$mcp_root" .
(cd "$repository" && go run ./build/guest/oci "$work/mcp-root.tar" "$root/usr/lib/agent-vmm/mcp-appliance.oci.tar" /usr/bin/agent-vmm-mcp-appliance)
tar -xOf "$root/usr/lib/agent-vmm/mcp-appliance.oci.tar" index.json | jq -er '.manifests | select(length == 1) | .[0].digest' >"$root/usr/lib/agent-vmm/mcp-appliance.digest"
cp "$root/usr/lib/agent-vmm/mcp-appliance.digest" "$bundle/mcp-appliance.digest"
mkdir -p "$bundle/mcp-appliance"
(cd "$repository" && go run ./build/appliance/manifest -output "$bundle/mcp-appliance" -image-digest "$(cat "$bundle/mcp-appliance.digest")" -release 1)
cp -a "$repository/build/guest/root/." "$root/"
ln -snf /run/resolv.conf "$root/etc/resolv.conf"
mkdir -p "$root/state" "$root/var/lib/cni" "$root/opt/containerd" "$root/etc/cni/net.d"
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
  mksquashfs "$root" "$bundle/rootfs.squashfs" -noappend -all-root -all-time 0 -mkfs-time 0 -comp xz -b 1M -Xdict-size 1M -Xbcj arm -sort "$work/squashfs.sort" -processors 1
  rootfs_compressor=xz
  rootfs_dictionary=1048576
  rootfs_filter=arm64-bcj
fi
(cd "$repository" && go run ./build/guest/bundle -directory "$bundle" -rootfs-compressor "$rootfs_compressor" -rootfs-dictionary "$rootfs_dictionary" -rootfs-filter "$rootfs_filter")

mkdir -p "$output"
cp -a "$bundle/." "$output/"
