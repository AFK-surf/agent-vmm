#!/bin/sh
set -eu
# Run only in the isolated fixture builder: /input is a read-only Guest bundle,
# /fixture contains this directory, and /output is a dedicated test volume.
apk add --no-cache squashfs-tools python3 >/dev/null
mkdir -p /output/bundle
cp -a /input/. /output/bundle/
work=$(mktemp -d /output/root.XXXXXX)
trap 'rm -rf "$work"' EXIT
unsquashfs -no-progress -d "$work/root" /input/rootfs.squashfs >/dev/null
cp /fixture/observe.sh "$work/root/usr/bin/pressure-fixture-observe"
chmod +x "$work/root/usr/bin/pressure-fixture-observe"
cat >> "$work/root/etc/init.d/agent-vmmd" <<'EOF'

start_post() {
  /usr/bin/pressure-fixture-observe >/dev/hvc0 2>&1 &
}
EOF
mksquashfs "$work/root" /output/bundle/rootfs.squashfs -noappend -all-root -comp zstd -Xcompression-level 1 -b 1M -processors 4 -no-progress >/dev/null
python3 - <<'PY'
import json,hashlib,pathlib
root=pathlib.Path('/output/bundle')
p=root/'manifest.json';m=json.loads(p.read_text());f=root/'rootfs.squashfs';data=f.read_bytes()
m['artifacts']['rootfs'].update(size=len(data),sha256=hashlib.sha256(data).hexdigest())
p.write_text(json.dumps(m,separators=(',',':')))
PY
