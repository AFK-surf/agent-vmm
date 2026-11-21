#!/usr/bin/env bash
set -euo pipefail

# Fast local macOS verification against a live Virtualization.framework Carrier VM.
# Uses ad-hoc codesign (0 notarization wait) and builds a tiny ~2 MiB arm64 busybox OCI
# archive locally from the guest bundle's rootfs.squashfs (0 R2 downloads).

REPO_ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
BUNDLE=""
REPACK_FROM=""
SKIP_BUILD=0
TEST_POOL_GROW=0
MEMORY_ONLY=0
CPUS=8
STATE_SIZE=$((128 * 1024 * 1024 * 1024))
OCI_ARCHIVE=""

usage() {
  cat >&2 <<'EOF'
usage: scripts/macos-fast-verify.sh (--bundle <DIR> | --repack-from <BASE_BUNDLE_DIR>) [options]

Options:
  --bundle <DIR>            Path to guest bundle directory (Image, initramfs.xz, rootfs.squashfs, manifest.json)
  --repack-from <BASE_DIR>  Repack a dev guest bundle from BASE_DIR using local l4h-dom0d, vmm-guest-init, and l4h.conf
  --skip-build              Skip cargo build of host/carrier (reuse existing target/debug binaries)
  --test-pool-grow          Also verify 64 GiB -> 128 GiB monotonic pool upgrade in place
  --memory-only             Only boot 2 containers and emit memory_two_containers JSON (for baseline comparisons)
  --cpus <N>                Carrier vCPU count (default: 8)
  --state-size <BYTES>      Carrier pool.raw logical size in bytes (default: 128 GiB)
  --oci-archive <TAR>       Use a pre-built arm64 OCI tar instead of packaging busybox from rootfs.squashfs
EOF
  exit 2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bundle) BUNDLE=$2; shift 2 ;;
    --repack-from) REPACK_FROM=$2; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --test-pool-grow) TEST_POOL_GROW=1; shift ;;
    --memory-only) MEMORY_ONLY=1; shift ;;
    --cpus) CPUS=$2; shift 2 ;;
    --state-size) STATE_SIZE=$2; shift 2 ;;
    --oci-archive) OCI_ARCHIVE=$2; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

[ -n "$BUNDLE" ] || [ -n "$REPACK_FROM" ] || usage
[ "$(uname -s)" = "Darwin" ] || { echo "macos-fast-verify.sh must run on macOS" >&2; exit 1; }

# Use /tmp directly so Unix socket paths stay well below macOS SUN_LEN (104 bytes).
WORK_DIR=$(mktemp -d /tmp/avmm-fv.XXXXXX)
trap 'rm -rf "$WORK_DIR"' EXIT

if [ -n "$REPACK_FROM" ]; then
  BUNDLE="$WORK_DIR/repacked-bundle"
  "$REPO_ROOT/scripts/repack-dev-guest-bundle.sh" --base "$REPACK_FROM" --out "$BUNDLE"
fi
[ -f "$BUNDLE/manifest.json" ] || { echo "missing $BUNDLE/manifest.json" >&2; exit 1; }

if [ "$SKIP_BUILD" -eq 0 ]; then
  if [ -z "${PROTOC:-}" ] && ! command -v protoc >/dev/null 2>&1; then
    PROTOC=$(sh "$REPO_ROOT/build/protoc.sh" "$WORK_DIR")
    export PROTOC
  fi
  (cd "$REPO_ROOT" && cargo build --locked -p agent-vmm-carrier --bin agent-vmm-carrier -p agent-vmm-host --example fast_verify)
fi

CARRIER_BIN="$REPO_ROOT/target/debug/agent-vmm-carrier"
VERIFY_BIN="$REPO_ROOT/target/debug/examples/fast_verify"
[ -x "$CARRIER_BIN" ] || { echo "missing $CARRIER_BIN" >&2; exit 1; }
[ -x "$VERIFY_BIN" ] || { echo "missing $VERIFY_BIN" >&2; exit 1; }

# Ad-hoc sign carrier with Virtualization.framework entitlement (0 notarization wait).
SIGNED_CARRIER="$WORK_DIR/agent-vmm-carrier"
cp "$CARRIER_BIN" "$SIGNED_CARRIER"
codesign -s - --force --entitlements "$REPO_ROOT/build/host/vz.entitlements.plist" "$SIGNED_CARRIER" >/dev/null 2>&1

# Package a tiny arm64 busybox OCI archive locally if not provided.
if [ -z "$OCI_ARCHIVE" ]; then
  OCI_ARCHIVE="$WORK_DIR/tiny-busybox.oci.tar"
  ROOTFS_EXTRACT="$WORK_DIR/tiny-root"
  mkdir -p "$ROOTFS_EXTRACT"
  if command -v unsquashfs >/dev/null 2>&1; then
    unsquashfs -q -d "$ROOTFS_EXTRACT" "$BUNDLE/rootfs.squashfs" \
      bin/busybox lib/ld-musl-aarch64.so.1 lib/libc.musl-aarch64.so.1 etc/ssl/certs/ca-certificates.crt
  else
    CACHE_DIR="${TMPDIR:-/tmp}/agent-vmm-fast-verify-cache"
    mkdir -p "$CACHE_DIR"
    MINIROOT="$CACHE_DIR/alpine-minirootfs-3.21.3-aarch64.tar.gz"
    if [ ! -s "$MINIROOT" ]; then
      curl -fsSL -o "$MINIROOT.part" \
        "https://dl-cdn.alpinelinux.org/alpine/v3.21/releases/aarch64/alpine-minirootfs-3.21.3-aarch64.tar.gz"
      mv "$MINIROOT.part" "$MINIROOT"
    fi
    tar -xzf "$MINIROOT" -C "$ROOTFS_EXTRACT" \
      bin/busybox lib/ld-musl-aarch64.so.1 lib/libc.musl-aarch64.so.1
  fi
  python3 - "$ROOTFS_EXTRACT" "$OCI_ARCHIVE" <<'PY'
import hashlib, io, json, os, stat, sys, tarfile

src_root, out_path = sys.argv[1], sys.argv[2]

layer_buf = io.BytesIO()
with tarfile.open(fileobj=layer_buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
    for d in ["bin", "lib", "etc", "etc/ssl", "etc/ssl/certs", "workspace", "srv", "tmp", "dev", "proc", "sys"]:
        ti = tarfile.TarInfo(d)
        ti.type = tarfile.DIRTYPE
        ti.mode = 0o755
        ti.mtime = 0
        tf.addfile(ti)
    for rel in [
        "bin/busybox",
        "lib/ld-musl-aarch64.so.1",
        "lib/libc.musl-aarch64.so.1",
        "etc/ssl/certs/ca-certificates.crt",
    ]:
        full = os.path.join(src_root, rel)
        if not os.path.exists(full):
            continue
        data = open(full, "rb").read()
        ti = tarfile.TarInfo(rel)
        ti.size = len(data)
        ti.mode = 0o755 if rel.startswith(("bin/", "lib/")) else 0o644
        ti.mtime = 0
        tf.addfile(ti, io.BytesIO(data))
    for applet in ["sh", "cat", "sleep", "nslookup", "wget", "nc", "dd", "sync", "head"]:
        ti = tarfile.TarInfo(f"bin/{applet}")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "busybox"
        ti.mode = 0o777
        ti.mtime = 0
        tf.addfile(ti)

layer_bytes = layer_buf.getvalue()
layer_digest = hashlib.sha256(layer_bytes).hexdigest()

config_obj = {
    "architecture": "arm64",
    "os": "linux",
    "config": {
        "Env": ["PATH=/bin:/usr/bin", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"],
        "Cmd": ["/bin/busybox", "sh"],
        "WorkingDir": "/workspace",
    },
    "rootfs": {"type": "layers", "diff_ids": [f"sha256:{layer_digest}"]},
}
config_bytes = json.dumps(config_obj, separators=(",", ":")).encode()
config_digest = hashlib.sha256(config_bytes).hexdigest()

manifest_obj = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "config": {
        "mediaType": "application/vnd.oci.image.config.v1+json",
        "digest": f"sha256:{config_digest}",
        "size": len(config_bytes),
    },
    "layers": [
        {
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": f"sha256:{layer_digest}",
            "size": len(layer_bytes),
        }
    ],
}
manifest_bytes = json.dumps(manifest_obj, separators=(",", ":")).encode()
manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()

index_obj = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.index.v1+json",
    "manifests": [
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": f"sha256:{manifest_digest}",
            "size": len(manifest_bytes),
            "platform": {"architecture": "arm64", "os": "linux"},
        }
    ],
}
index_bytes = json.dumps(index_obj, separators=(",", ":")).encode()
layout_bytes = b'{"imageLayoutVersion":"1.0.0"}'

with tarfile.open(out_path, "w", format=tarfile.USTAR_FORMAT) as tf:
    for name, data in [
        ("oci-layout", layout_bytes),
        ("index.json", index_bytes),
        (f"blobs/sha256/{config_digest}", config_bytes),
        (f"blobs/sha256/{layer_digest}", layer_bytes),
        (f"blobs/sha256/{manifest_digest}", manifest_bytes),
    ]:
        ti = tarfile.TarInfo(name)
        ti.size = len(data)
        ti.mode = 0o644
        ti.mtime = 0
        tf.addfile(ti, io.BytesIO(data))
PY
fi

# Extract manifest digest and archive sha256.
OCI_META=$(python3 - "$OCI_ARCHIVE" <<'PY'
import hashlib, json, sys, tarfile
path = sys.argv[1]
h = hashlib.sha256(open(path, "rb").read()).hexdigest()
with tarfile.open(path, "r") as tf:
    idx = json.loads(tf.extractfile("index.json").read())
    md = idx["manifests"][0]["digest"]
print(f"{md} {h}")
PY
)
OCI_MANIFEST_DIGEST=${OCI_META%% *}
OCI_ARCHIVE_SHA256=${OCI_META#* }

# Create tiny BuildImage context tar (FROM scratch + COPY hello.txt).
BUILD_CTX="$WORK_DIR/build-context.tar"
python3 - "$BUILD_CTX" <<'PY'
import io, sys, tarfile
with tarfile.open(sys.argv[1], "w", format=tarfile.USTAR_FORMAT) as tf:
    for name, data in [
        ("Dockerfile", b"FROM scratch\nCOPY hello.txt /hello.txt\n"),
        ("hello.txt", b"built-by-fast-verify\n"),
    ]:
        ti = tarfile.TarInfo(name)
        ti.size = len(data)
        ti.mode = 0o644
        ti.mtime = 0
        tf.addfile(ti, io.BytesIO(data))
PY

EXTRA_ARGS=()
if [ "$TEST_POOL_GROW" -eq 1 ]; then
  EXTRA_ARGS+=(--test-pool-grow)
fi
if [ "$MEMORY_ONLY" -eq 1 ]; then
  EXTRA_ARGS+=(--memory-only)
fi

exec "$VERIFY_BIN" \
  --carrier-bin "$SIGNED_CARRIER" \
  --bundle "$BUNDLE" \
  --work-dir "$WORK_DIR/run" \
  --oci-archive "$OCI_ARCHIVE" \
  --oci-manifest-digest "$OCI_MANIFEST_DIGEST" \
  --oci-archive-sha256 "$OCI_ARCHIVE_SHA256" \
  --build-context-tar "$BUILD_CTX" \
  --cpus "$CPUS" \
  --state-size "$STATE_SIZE" \
  "${EXTRA_ARGS[@]}"
