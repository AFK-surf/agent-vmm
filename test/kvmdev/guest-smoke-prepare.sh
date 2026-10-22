#!/bin/sh
set -eu
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then echo 'usage: guest-smoke-prepare.sh GUEST_BUNDLE TEST_DIRECTORY [--built-bundle]' >&2; exit 2; fi
repack=1
if [ "$#" -eq 3 ]; then
 if [ "$3" != --built-bundle ]; then echo 'third argument must be --built-bundle' >&2; exit 2; fi
 repack=0
fi
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
bundle=$(CDPATH= cd -- "$1" && pwd)
mkdir -p "$2"
work=$(CDPATH= cd -- "$2" && pwd)
docker image inspect agent-vmm-guest-tools:local >/dev/null
if [ "$repack" -eq 1 ]; then
(cd "$repository" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$work/agent-vmmd" ./cmd/agent-vmmd)
(cd "$repository" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$work/agent-vmm-supervisor" ./cmd/agent-vmm-supervisor)
fi
(cd "$repository" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$work/guest-smoke" ./test/kvmdev/guest-smoke_linux.go)
cp "$bundle/Image" "$work/Image"
docker run --rm --pull=never --network=none -e REPACK="$repack" -v "$bundle:/bundle:ro" -v "$repository/build/guest:/source:ro" -v "$work:/output" agent-vmm-guest-tools:local sh -ec '
 unsquashfs -d /tmp/root /bundle/rootfs.squashfs >/dev/null
 if [ "$REPACK" -eq 1 ]; then
 cp /output/agent-vmmd /tmp/root/usr/bin/agent-vmmd
 cp /output/agent-vmm-supervisor /tmp/root/usr/bin/agent-vmm-supervisor
 cp -a /source/root/. /tmp/root/
 chmod 755 /tmp/root/etc/init.d/agent-vmmd
 mksquashfs /tmp/root /output/rootfs.squashfs -noappend -all-root -comp zstd -Xcompression-level 1 -b 1M -processors 4 >/dev/null
 mkdir -p /tmp/initramfs
 cd /tmp/initramfs
 xz -dc /bundle/initramfs.xz | cpio -id --quiet
 cp /source/initramfs/init init
 chmod +x init
 find . -print0 | cpio --null -o --format=newc --quiet | xz -T0 -1 --check=crc32 >/output/guest-initramfs.xz
 else
 cp /bundle/rootfs.squashfs /output/rootfs.squashfs
 cp /bundle/initramfs.xz /output/guest-initramfs.xz
 fi
 mkdir -p /tmp/workload/bin /tmp/workload/lib /tmp/workload/etc
 cp /tmp/root/bin/busybox /tmp/workload/bin/busybox
 cp /tmp/root/lib/ld-musl-aarch64.so.1 /tmp/workload/lib/
 cp /tmp/root/lib/libc.musl-aarch64.so.1 /tmp/workload/lib/
 touch /tmp/workload/.agent-vmm-supervisor /tmp/workload/etc/resolv.conf
 chmod 755 /tmp/workload/.agent-vmm-supervisor
 tar -cf /output/workload-root.tar -C /tmp/workload .
'
(cd "$repository" && go run ./build/guest/oci "$work/workload-root.tar" "$work/workload.oci.tar" /bin/busybox)
cat >"$work/run.sh" <<'RUN'
#!/bin/sh
set -eu
/bin/busybox uname -a
/bin/busybox date -u
/bin/busybox ls -l /dev/kvm
exec /work/guest-smoke
RUN
chmod +x "$work/run.sh"
