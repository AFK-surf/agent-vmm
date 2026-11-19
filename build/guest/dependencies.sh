#!/bin/sh
set -eu
set -o pipefail
work=/dependencies
root=$work/root
bundle=$work/bundle
cache=/var/cache/agent-vmm-downloads
mkdir -p "$root" "$bundle" "$work/downloads" "$cache/apk" "$cache/files"

fetch() {
  curl -fsSL --connect-timeout 15 --max-time 120 \
    --retry 2 --retry-all-errors --retry-max-time 240 --output "$2.part" "$1"
  mv "$2.part" "$2"
}

fetch_current() {
  cached=$cache/files/$(printf '%s' "$1" | sha256sum | cut -d ' ' -f 1)
  url=$1
  destination=$2
  set -- "$url"
  if [ -f "$cached" ] && [ -s "$cached.etag" ]; then
    set -- "$@" --etag-compare "$cached.etag"
  fi
  status=$(curl -fsSL --connect-timeout 15 --max-time 120 \
    --retry 2 --retry-all-errors --retry-max-time 240 --etag-save "$cached.etag.part" \
    --output "$cached.part" --write-out '%{http_code}' "$@")
  case "$status" in
    200)
      rm -f "$cached.etag"
      mv "$cached.part" "$cached"
      if [ -s "$cached.etag.part" ]; then
        mv "$cached.etag.part" "$cached.etag"
      fi
      ;;
    304)
      test -f "$cached"
      test -s "$cached.etag"
      rm -f "$cached.part" "$cached.etag.part"
      echo "reuse current download $url"
      ;;
    *) echo "unexpected download status: $status" >&2; exit 1 ;;
  esac
  cp "$cached" "$destination"
}

alpine_releases=$work/downloads/alpine-releases.yaml
fetch_current "https://dl-cdn.alpinelinux.org/alpine/latest-stable/releases/aarch64/latest-releases.yaml" "$alpine_releases"
alpine_release=$(awk '
  $1 == "flavor:" { flavor = $2 }
  flavor == "alpine-minirootfs" && $1 == "file:" { filename = $2 }
  flavor == "alpine-minirootfs" && $1 == "sha256:" { print filename " " $2; exit }
' "$alpine_releases")
alpine_miniroot=${alpine_release%% *}
alpine_sha256=${alpine_release#* }
test -n "$alpine_miniroot"
miniroot=$work/downloads/alpine-minirootfs.tar.gz
url=https://dl-cdn.alpinelinux.org/alpine/latest-stable/releases/aarch64/$alpine_miniroot
cached=$cache/files/$(printf '%s' "$url" | sha256sum | cut -d ' ' -f 1)
if [ -f "$cached" ]; then
  cp "$cached" "$miniroot"
  if printf '%s  %s\n' "$alpine_sha256" "$miniroot" | sha256sum -c -; then
    echo "reuse miniroot $alpine_miniroot"
  else
    rm -f "$cached"
  fi
fi
if [ ! -f "$cached" ]; then
  fetch "$url" "$miniroot"
  printf '%s  %s\n' "$alpine_sha256" "$miniroot" | sha256sum -c -
  cp "$miniroot" "$cached.part"
  mv "$cached.part" "$cached"
fi
tar -xzf "$miniroot" -C "$root"
cp /etc/resolv.conf "$root/etc/resolv.conf"
set -- openrc ca-certificates-bundle iproute2 nftables erofs-utils jq e2fsprogs \
  e2fsprogs-extra device-mapper cryptsetup kmod xz squashfs-tools linux-virt
apk_repositories=/tmp/agent-vmm-apk-repositories
: > "$root$apk_repositories"
apk_repository=0
arch=$(chroot "$root" /sbin/apk --print-arch)
while IFS= read -r apk_origin; do
  case "$apk_origin" in ''|'#'*) continue ;; esac
  apk_repository=$((apk_repository + 1))
  directory=/tmp/agent-vmm-apk/$apk_repository
  mkdir -p "$root$directory"
  printf '%s\n' "$apk_origin" > "$root$directory/origin"
  fetch_current "$apk_origin/$arch/APKINDEX.tar.gz" "$root$directory/APKINDEX.tar.gz"
  printf '%s\n' "$directory/APKINDEX.tar.gz" >> "$root$apk_repositories"
done < "$root/etc/apk/repositories"
chroot "$root" /sbin/apk --no-network --repositories-file "$apk_repositories" update
chroot "$root" /sbin/apk --no-network --repositories-file "$apk_repositories" \
  fetch --simulate --recursive --world --from repositories --url "$@" > "$work/apk-packages"
while IFS= read -r package; do
  apk_origin=$(cat "$root${package%/*}/origin")
  url=$apk_origin/$arch/${package##*/}
  cached=$cache/apk/$(printf '%s' "$url" | sha256sum | cut -d ' ' -f 1)
  if [ -f "$cached" ]; then
    cp "$cached" "$root$package"
    if chroot "$root" /sbin/apk verify "$package"; then
      echo "reuse APK ${package##*/}"
    else
      rm -f "$cached"
    fi
  fi
  if [ ! -f "$cached" ]; then
    echo "download APK ${package##*/}"
    fetch "$url" "$root$package"
    chroot "$root" /sbin/apk verify "$package"
    cp "$root$package" "$cached.part"
    mv "$cached.part" "$cached"
  fi
done < "$work/apk-packages"
chroot "$root" /sbin/apk --no-network --repositories-file "$apk_repositories" upgrade
chroot "$root" /sbin/apk --no-network --repositories-file "$apk_repositories" add "$@"
rm -rf "$root/tmp/agent-vmm-apk" "$root$apk_repositories"
for package do
  chroot "$root" /sbin/apk info -e "$package"
done

kernel_config=$(find "$root/boot" -name 'config-*' -type f)
for symbol in ARM64 BRIDGE CGROUPS CGROUP_BPF EXT4_FS INET NETFILTER NF_TABLES OVERLAY_FS RD_XZ SECCOMP SECCOMP_FILTER SQUASHFS SQUASHFS_XZ SQUASHFS_ZSTD TUN VETH VIRTIO VIRTIO_BALLOON VIRTIO_BLK VIRTIO_CONSOLE VIRTIO_NET VIRTIO_VSOCKETS VSOCKETS XZ_DEC XZ_DEC_ARM64; do
  grep -Eq "^CONFIG_${symbol}=[ym]$" "$kernel_config" || { echo "missing CONFIG_$symbol" >&2; exit 1; }
done
for symbol in RTC_DRV_PL031 RTC_HCTOSYS; do
  grep -Eq "^CONFIG_${symbol}=y$" "$kernel_config" || { echo "missing CONFIG_$symbol" >&2; exit 1; }
done
guest-kernel "$root/boot/vmlinuz-"* >"$bundle/Image"
test -s "$bundle/Image"

find "$root" "$bundle" -exec touch -h -d @0 {} +
