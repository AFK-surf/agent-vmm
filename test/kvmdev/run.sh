#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then echo 'usage: run.sh EXISTING_GUEST_BUNDLE TEST_DIRECTORY' >&2; exit 2; fi
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
bundle=$(CDPATH= cd -- "$1" && pwd)
work=$(CDPATH= cd -- "$2" && pwd)
test -f "$work/run.sh"
mkdir -p "$repository/.build/kvmdev"
if ! docker image inspect agent-vmm-guest-tools:local >/dev/null 2>&1; then
 docker buildx build --target tools --load -t agent-vmm-guest-tools:local -f "$repository/build/guest/Dockerfile" "$repository"
fi
docker run --rm --pull=never -v "$bundle:/bundle:ro" -v "$repository/test/kvmdev:/scripts:ro" -v "$work:/output" agent-vmm-guest-tools:local sh -ec '
 mkdir -p /tmp/initramfs
 cd /tmp/initramfs
 xz -dc /bundle/initramfs.xz | cpio -id --quiet
 cp /scripts/init.sh init
 chmod +x init
 mkdir -p tmp work
 find . -print0 | cpio --null -o --format=newc --quiet | xz -T0 -1 --check=crc32 > /output/dev-initramfs.xz
'
go build -o "$repository/.build/kvmdev/runner" "$repository/test/kvmdev"
cat > "$repository/.build/kvmdev/entitlements.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>com.apple.security.virtualization</key><true/></dict></plist>
PLIST
codesign --force --sign - --entitlements "$repository/.build/kvmdev/entitlements.plist" "$repository/.build/kvmdev/runner"
rm -f "$work/result.status"
"$repository/.build/kvmdev/runner" -kernel "$bundle/Image" -initrd "$work/dev-initramfs.xz" -shared "$work" -log "$work/outer.log"
test -f "$work/result.status"
cat "$work/result.log"
test "$(cat "$work/result.status")" = 0
