#!/usr/bin/env bash
set -euo pipefail

# Fast local development Guest bundle repacker (no Docker required).
# Cross-compiles l4h-dom0d and vmm-guest-init for aarch64-unknown-linux-musl using zig cc,
# patches l4h-config in place inside the base bundle's Image (L4HMODS1 table),
# repacks rootfs.squashfs (including workload-initramfs.cpio), and updates manifest.json.

REPO_ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
BASE_BUNDLE=""
OUT_DIR=""

usage() {
  cat >&2 <<'EOF'
usage: scripts/repack-dev-guest-bundle.sh --base <BASE_BUNDLE_DIR> --out <OUT_BUNDLE_DIR>
EOF
  exit 2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --base) BASE_BUNDLE=$2; shift 2 ;;
    --out) OUT_DIR=$2; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

[ -n "$BASE_BUNDLE" ] && [ -n "$OUT_DIR" ] || usage
[ -f "$BASE_BUNDLE/Image" ] || { echo "missing $BASE_BUNDLE/Image" >&2; exit 1; }
[ -f "$BASE_BUNDLE/rootfs.squashfs" ] || { echo "missing $BASE_BUNDLE/rootfs.squashfs" >&2; exit 1; }
[ -f "$BASE_BUNDLE/initramfs.xz" ] || { echo "missing $BASE_BUNDLE/initramfs.xz" >&2; exit 1; }

for tool in cargo zig unsquashfs mksquashfs python3 jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 1; }
done

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/agent-vmm-repack.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT

# 1. Cross-compile static aarch64-unknown-linux-musl l4h-dom0d and vmm-guest-init via zig cc.
ZIG_CC="$WORK_DIR/zig-aarch64-musl-cc"
ZIG_AR="$WORK_DIR/zig-ar"
cat > "$ZIG_CC" <<'EOF'
#!/usr/bin/env python3
import os, sys
out = ["zig", "cc", "-target", "aarch64-linux-musl"]
it = iter(sys.argv[1:])
for a in it:
    if a == "-target":
        next(it, None)
        continue
    if a.startswith("--target=") or a == "-Wl,--fix-cortex-a53-843419":
        continue
    out.append(a)
os.execvp("zig", out)
EOF
cat > "$ZIG_AR" <<'EOF'
#!/usr/bin/env sh
exec zig ar "$@"
EOF
chmod +x "$ZIG_CC" "$ZIG_AR"

if [ -z "${PROTOC:-}" ] && ! command -v protoc >/dev/null 2>&1; then
  PROTOC=$(sh "$REPO_ROOT/build/protoc.sh" "$WORK_DIR")
  export PROTOC
fi

(
  cd "$REPO_ROOT"
  CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="$ZIG_CC" \
  CC_aarch64_unknown_linux_musl="$ZIG_CC" \
  AR_aarch64_unknown_linux_musl="$ZIG_AR" \
  RUSTFLAGS="-C target-feature=+crt-static -C link-self-contained=no" \
  cargo build --locked --release --target aarch64-unknown-linux-musl \
    -p l4h-dom0d --bin l4h-dom0d \
    -p vmm-guest-init --bin vmm-guest-init
)

DOM0D_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/l4h-dom0d"
GUEST_INIT_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/vmm-guest-init"

mkdir -p "$OUT_DIR"
cp "$BASE_BUNDLE/initramfs.xz" "$OUT_DIR/initramfs.xz"
[ -f "$BASE_BUNDLE/THIRD_PARTY_NOTICES" ] && cp "$BASE_BUNDLE/THIRD_PARTY_NOTICES" "$OUT_DIR/THIRD_PARTY_NOTICES"

# 2. Patch l4h-config in place inside Image's L4HMODS1 table.
python3 - "$BASE_BUNDLE/Image" "$REPO_ROOT/build/guest/l4h.conf" "$OUT_DIR/Image" <<'PY'
import struct, sys

src_img, conf_path, dst_img = sys.argv[1], sys.argv[2], sys.argv[3]
TABLE_MAGIC = b"L4HMODS1"
NAME_LEN = 64
PAGE = 4096

def align(n, a):
    return (n + a - 1) // a * a

img = bytearray(open(src_img, "rb").read())
new_conf = open(conf_path, "rb").read()

pos = img.find(TABLE_MAGIC)
if pos < 0 or img.find(TABLE_MAGIC, pos + 1) >= 0:
    sys.exit(f"{src_img}: expected unique L4HMODS1 module table")

(count,) = struct.unpack_from("<Q", img, pos + 8)
found = False
for i in range(count):
    entry_off = pos + 16 + i * (NAME_LEN + 16)
    raw_name = bytes(img[entry_off : entry_off + NAME_LEN])
    name = raw_name.split(b"\x00", 1)[0].decode("ascii", errors="replace")
    mod_off, mod_len = struct.unpack_from("<QQ", img, entry_off + NAME_LEN)
    if name == "l4h-config":
        slot_cap = align(max(mod_len, 1), PAGE)
        if len(new_conf) > slot_cap:
            sys.exit(f"new l4h.conf ({len(new_conf)}B) exceeds module slot ({slot_cap}B)")
        struct.pack_into("<Q", img, entry_off + NAME_LEN + 8, len(new_conf))
        abs_off = pos + mod_off
        img[abs_off : abs_off + slot_cap] = new_conf.ljust(slot_cap, b"\x00")
        found = True
        break

if not found:
    sys.exit(f"{src_img}: l4h-config entry not found in L4HMODS1 table")

open(dst_img, "wb").write(img)
PY

# 3. Unpack base rootfs.squashfs, replace binaries/init script, rebuild workload-initramfs.cpio, repack squashfs.
ROOT="$WORK_DIR/root"
unsquashfs -q -d "$ROOT" "$BASE_BUNDLE/rootfs.squashfs"
install -m 0755 "$DOM0D_BIN" "$ROOT/usr/bin/l4h-dom0d"
install -m 0755 "$GUEST_INIT_BIN" "$ROOT/usr/bin/vmm-guest-init"
install -m 0755 "$REPO_ROOT/build/guest/root/etc/init.d/l4h-dom0d" "$ROOT/etc/init.d/l4h-dom0d"

# Rebuild workload-initramfs.cpio by replacing /init inside the existing cpio (preserving mkfs.ext4 & libs).
python3 - "$ROOT/usr/lib/agent-vmm/workload-initramfs.cpio" "$GUEST_INIT_BIN" <<'PY'
import sys

cpio_path, new_init_path = sys.argv[1], sys.argv[2]
data = open(cpio_path, "rb").read()
new_init = open(new_init_path, "rb").read()

def pad4(n):
    return (4 - (n % 4)) % 4

out = bytearray()
off = 0
replaced = False
while off < len(data):
    hdr = data[off : off + 110]
    if len(hdr) < 110 or hdr[:6] != b"070701":
        sys.exit(f"invalid cpio header at offset {off}")
    filesize = int(hdr[54:62], 16)
    namesize = int(hdr[94:102], 16)
    name_start = off + 110
    name_bytes = data[name_start : name_start + namesize]
    name = name_bytes.rstrip(b"\x00").decode("utf-8")
    body_start = name_start + namesize + pad4(110 + namesize)
    body = data[body_start : body_start + filesize]
    next_off = body_start + filesize + pad4(filesize)

    if name == "init":
        body = new_init
        hdr = hdr[:54] + f"{len(body):08x}".encode("ascii") + hdr[62:]
        replaced = True

    out.extend(hdr)
    out.extend(name_bytes)
    out.extend(b"\x00" * pad4(110 + namesize))
    out.extend(body)
    out.extend(b"\x00" * pad4(len(body)))
    off = next_off
    if name == "TRAILER!!!":
        break

if not replaced:
    sys.exit("init entry not found in workload-initramfs.cpio")
open(cpio_path, "wb").write(out)
PY

rm -f "$OUT_DIR/rootfs.squashfs"
mksquashfs "$ROOT" "$OUT_DIR/rootfs.squashfs" -noappend -all-root -all-time 0 -mkfs-time 0 -comp zstd -Xcompression-level 1 -b 1M -quiet

# 4. Regenerate manifest.json with updated sizes and SHA-256 digests.
artifact() {
  p=$1
  digest=$(shasum -a 256 "$OUT_DIR/$p" | awk '{print $1}')
  size=$(wc -c < "$OUT_DIR/$p" | tr -d ' ')
  jq -cn --arg path "$p" --arg sha256 "$digest" --argjson size "$size" \
    '{path: $path, size: $size, sha256: $sha256}'
}
jq -cn --argjson kernel "$(artifact Image)" --argjson initramfs "$(artifact initramfs.xz)" \
  --argjson rootfs "$(artifact rootfs.squashfs)" \
  '{schema_version: 1, architecture: "arm64", artifacts: {kernel: $kernel, initramfs: $initramfs, rootfs: $rootfs},
    rootfs: {filesystem: "squashfs", compressor: "zstd", block_size: 1048576, dictionary_size: 0, filter: "none"}}' \
  > "$OUT_DIR/manifest.json"
