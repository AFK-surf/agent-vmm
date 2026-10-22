#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then echo 'usage: prepare-smoke.sh EXISTING_GUEST_BUNDLE TEST_DIRECTORY' >&2; exit 2; fi
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
bundle=$(CDPATH= cd -- "$1" && pwd)
mkdir -p "$2"
work=$(CDPATH= cd -- "$2" && pwd)
docker image inspect agent-vmm-guest-tools:local >/dev/null
(cd "$repository" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$work/smoke" ./test/kvmdev)
cp "$bundle/Image" "$work/Image"
docker run --rm --pull=never --network=none -v "$bundle:/bundle:ro" -v "$repository/test/kvmdev:/scripts:ro" -v "$work:/output" agent-vmm-guest-tools:local sh -ec '
 mkdir -p /tmp/initramfs
 cd /tmp/initramfs
 xz -dc /bundle/initramfs.xz | cpio -id --quiet
 cp /scripts/inner-init.sh init
 cp /output/smoke smoke
 chmod +x init smoke
 find . -print0 | cpio --null -o --format=newc --quiet | xz -T0 -1 --check=crc32 > /output/inner-initramfs.xz
'
cat > "$work/run.sh" <<'RUN'
#!/bin/sh
set -eu
/bin/busybox uname -a
/bin/busybox ls -l /dev/kvm
exec /work/smoke
RUN
chmod +x "$work/run.sh"
