#!/usr/bin/env bash
set -euo pipefail

profile=${1:-development}
l4h_root=${L4H_ROOT:-/l4h-src}
dl_cache=${L4H_DL_CACHE:-/var/cache/l4h-downloads}
build_cache=${L4H_BUILD_CACHE:-/l4h-build-cache}
out_dir=${L4H_OUT_DIR:-/l4h-artifacts}

L4H_REPO=${L4H_REPO:-losfair/l4h}
L4H_COMMIT=62ce7f517bbdd909b5445f49929cebe779567630
L4H_SHA256=15f841bd01940eba92f4954e5ff256d0ca9ff3b17bcbe72bfcc0af2d7995ce6a
FL4_REPO=${FL4_REPO:-losfair/FL4}
FL4_COMMIT=99320e2b2558e5eecd65af7032c9827149cc91be
FL4_SHA256=b9b29f717a79aacd31a625aeaad312db7c7284050d07a7788d41f122ff037658
# Build-time Python interface/bitfield generators imported by FL4's CMakeLists.txt
FL4_VENDOR_SEL4_COMMIT=c98d70adf15e8be7d3b99085c28d057069ba7155
FL4_VENDOR_SEL4_SHA256=3916dd86885eb693e84858993b26d7ca97f55ea9087cff25f400fa9d123dd85d
RUST_SEL4_COMMIT=86a0dbf4f53e6766734331c5df61851489ed38a1
RUST_SEL4_SHA256=1e29856658b0c0ee1a6f1e00b3f65cc330597e590b451c5ea7d2c9d47aa8d19f
KEMU_COMMIT=6bd4fa37efc1f6dc3c5b2d27715942dc830ca94c
KEMU_SHA256=e7ca2b5ea064ac45fa541e193ac6734389723f25560b6b8a21c370206759e193
LINUX_VERSION=6.18.65
LINUX_SHA256=c92606f956f09dc50b5a0e2af9e86763fce27ada1647de97f8d1ac8cfc95801d

mkdir -p "$dl_cache" "$build_cache" "$out_dir"
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-1700000000}

fetch_cached() {
  local url=$1 dest=$2 sha256=${3:-}
  if [ ! -f "$dest" ]; then
    curl -fsSL --retry 3 --retry-all-errors "$url" -o "$dest.part"
    mv "$dest.part" "$dest"
  fi
  if [ -n "$sha256" ]; then
    printf '%s  %s\n' "$sha256" "$dest" | sha256sum -c -
  fi
}

# 1. Fetch and unpack pinned l4h source + Python venv
l4h_stamp=$build_cache/l4h-src.stamp
l4h_want="${L4H_REPO}@${L4H_COMMIT}"
if [ ! -f "$l4h_root/tools/build-fl4-aarch64.sh" ] || [ "$(cat "$l4h_stamp" 2>/dev/null || true)" != "$l4h_want" ]; then
  fetch_cached "https://github.com/${L4H_REPO}/archive/${L4H_COMMIT}.tar.gz" \
    "$dl_cache/l4h-${L4H_COMMIT}.tar.gz" "$L4H_SHA256"
  mkdir -p "$l4h_root"
  find "$l4h_root" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  tar -xzf "$dl_cache/l4h-${L4H_COMMIT}.tar.gz" --strip-components=1 -C "$l4h_root"
  echo "$l4h_want" > "$l4h_stamp"
fi

venv_dir=${L4H_VENV:-/opt/sel4-venv}
if [ ! -x "$venv_dir/bin/python3" ]; then
  python3 -m venv "$venv_dir"
  "$venv_dir/bin/pip" install --no-cache-dir -r "$l4h_root/tools/sel4-requirements.txt"
fi
export PATH="$venv_dir/bin:$PATH"
ln -sfn "$venv_dir" "$l4h_root/.venv"

mkdir -p "$build_cache/l4h-build"
rm -rf "$l4h_root/build"
ln -sfn "$build_cache/l4h-build" "$l4h_root/build"

# 2. Fetch and unpack pinned FL4 + vendor/seL4 into a detached git worktree
fl4_src=$build_cache/fl4-src
fl4_stamp=$build_cache/fl4-src.stamp
fl4_want="${FL4_REPO}@${FL4_COMMIT}+${FL4_VENDOR_SEL4_COMMIT}"
if [ ! -d "$fl4_src/.git" ] || [ "$(cat "$fl4_stamp" 2>/dev/null || true)" != "$fl4_want" ]; then
  fetch_cached "https://github.com/${FL4_REPO}/archive/${FL4_COMMIT}.tar.gz" \
    "$dl_cache/fl4-${FL4_COMMIT}.tar.gz" "$FL4_SHA256"
  fetch_cached "https://github.com/seL4/seL4/archive/${FL4_VENDOR_SEL4_COMMIT}.tar.gz" \
    "$dl_cache/seL4-${FL4_VENDOR_SEL4_COMMIT}.tar.gz" "$FL4_VENDOR_SEL4_SHA256"
  rm -rf "$fl4_src"
  mkdir -p "$fl4_src"
  tar -xzf "$dl_cache/fl4-${FL4_COMMIT}.tar.gz" --strip-components=1 -C "$fl4_src"
  rm -rf "$fl4_src/vendor/seL4"
  mkdir -p "$fl4_src/vendor/seL4"
  tar -xzf "$dl_cache/seL4-${FL4_VENDOR_SEL4_COMMIT}.tar.gz" --strip-components=1 -C "$fl4_src/vendor/seL4"
  git -C "$fl4_src/vendor/seL4" init -q
  git -C "$fl4_src/vendor/seL4" add -A
  git -C "$fl4_src/vendor/seL4" -c user.name=l4h -c user.email=l4h@localhost commit -q -m init
  git -C "$fl4_src" init -q
  git -C "$fl4_src" config -f .gitmodules submodule.vendor/seL4.url "$fl4_src/vendor/seL4"
  git -C "$fl4_src" add -A
  git -C "$fl4_src" -c user.name=l4h -c user.email=l4h@localhost commit -q -m init
  git -C "$fl4_src" checkout -q --detach HEAD
  echo "$fl4_want" > "$fl4_stamp"
fi

# 3. Build FL4 kernel (AArch64) and l4h-fl4-image loader stub
release_flag=0
if [ "$profile" = release ]; then
  release_flag=1
fi
L4H_RELEASE="$release_flag" L4H_FL4_SOURCE="$fl4_src" bash "$l4h_root/tools/build-fl4-aarch64.sh"
L4H_FL4_SOURCE="$fl4_src" bash "$l4h_root/tools/build-fl4-image-loader.sh" "$out_dir/l4h-fl4-image.bin"
sel4_prefix="$l4h_root/build/sel4-fl4-aarch64"
aarch64-linux-gnu-objcopy --strip-debug "$sel4_prefix/bin/kernel.elf" "$out_dir/kernel.elf"

# 4. Prepare unpatched rust-sel4 source
rs_src=$build_cache/rust-sel4-src
rs_stamp=$build_cache/rust-sel4-src.stamp
if [ ! -f "$rs_src/Cargo.toml" ] || [ "$(cat "$rs_stamp" 2>/dev/null || true)" != "$RUST_SEL4_COMMIT" ]; then
  fetch_cached "https://github.com/seL4/rust-sel4/archive/${RUST_SEL4_COMMIT}.tar.gz" \
    "$dl_cache/rust-sel4-${RUST_SEL4_COMMIT}.tar.gz" "$RUST_SEL4_SHA256"
  rm -rf "$rs_src"
  mkdir -p "$rs_src"
  tar -xzf "$dl_cache/rust-sel4-${RUST_SEL4_COMMIT}.tar.gz" --strip-components=1 -C "$rs_src"
  echo "$RUST_SEL4_COMMIT" > "$rs_stamp"
fi
rm -rf "$l4h_root/third_party/rust-sel4"
ln -s "$rs_src" "$l4h_root/third_party/rust-sel4"

# 5. Build l4h host components + l4h-root against FL4
host_target=$build_cache/host-target-$profile
export SEL4_PREFIX="$sel4_prefix"
export CC_aarch64_sel4=aarch64-linux-gnu-gcc
export CC_aarch64_sel4_roottask=aarch64-linux-gnu-gcc
export LIBCLANG_PATH=$(dirname "$(find /usr/lib -name 'libclang.so*' -print -quit)")
export SOURCE_DATE_EPOCH=1700000000
export L4H_VERSION=0.1.0-fl4

(
  cd "$l4h_root/host"
  nicds=(virtio)
  nicd_pkgs=()
  for d in "${nicds[@]}"; do nicd_pkgs+=(-p "l4h-nicd-$d"); done
  cargo +nightly-2026-08-21 build --release --target aarch64-sel4 --target-dir "$host_target" \
    -Z build-std=core,alloc,compiler_builtins -Z build-std-features=compiler-builtins-mem \
    -p l4h-vmm -p l4h-hvm -p l4h-blkd -p l4h-netd -p l4h-trustd -p l4h-memd -p l4h-logd -p l4h-usbd \
    "${nicd_pkgs[@]}"

  out="$host_target/aarch64-sel4/release"
  mkdir -p "$out/stripped"
  for c in vmm hvm blkd netd trustd memd logd usbd "${nicds[@]/#/nicd-}"; do
    aarch64-linux-gnu-objcopy --strip-debug "$out/l4h-$c.elf" "$out/stripped/l4h-$c.elf"
    export "L4H_$(echo "$c" | tr a-z- A-Z_)_ELF=$out/stripped/l4h-$c.elf"
  done
  drivers=()
  for d in "${nicds[@]}"; do drivers+=("$out/stripped/l4h-nicd-$d.elf"); done
  python3 "$l4h_root/tools/mkdrivers.py" "$out/stripped/l4h-drivers.bin" "${drivers[@]}"
  export L4H_DRIVERS_BIN="$out/stripped/l4h-drivers.bin"

  cargo +nightly-2026-08-21 build --release --target aarch64-sel4 --target-dir "$host_target" \
    -Z build-std=core,alloc,compiler_builtins -Z build-std-features=compiler-builtins-mem \
    -p l4h-vmmgr
  aarch64-linux-gnu-objcopy --strip-debug "$out/l4h-vmmgr.elf" "$out/stripped/l4h-vmmgr.elf"
  export L4H_VMMGR_ELF="$out/stripped/l4h-vmmgr.elf"

  cargo +nightly-2026-08-21 build --release --target aarch64-sel4-roottask --target-dir "$host_target" \
    -Z build-std=core,alloc,compiler_builtins -Z build-std-features=compiler-builtins-mem \
    -p l4h-root
  aarch64-linux-gnu-objcopy --strip-debug "$host_target/aarch64-sel4-roottask/release/l4h-root.elf" "$out_dir/l4h-root.stripped"
)

# 6. Prepare kemu + linux-${LINUX_VERSION} and build ARCH=l4h SUBARCH=aarch64 vmlinux
kemu_src=$build_cache/kemu-src
kemu_stamp=$build_cache/kemu-src.stamp
if [ ! -f "$kemu_src/overlay/kernel/kemu/Kconfig" ] || [ "$(cat "$kemu_stamp" 2>/dev/null || true)" != "$KEMU_COMMIT" ]; then
  rm -rf "$kemu_src"
  mkdir -p "$kemu_src"
  fetch_cached "https://github.com/losfair/kemu/archive/${KEMU_COMMIT}.tar.gz" \
    "$dl_cache/kemu-${KEMU_COMMIT}.tar.gz" "$KEMU_SHA256"
  tar -xzf "$dl_cache/kemu-${KEMU_COMMIT}.tar.gz" --strip-components=1 -C "$kemu_src"
  git -C "$kemu_src" init -q
  git -C "$kemu_src" add -A
  git -C "$kemu_src" -c user.name=l4h -c user.email=l4h@localhost commit -q -m init
  echo "$KEMU_COMMIT" > "$kemu_stamp"
fi
rm -rf "$l4h_root/third_party/kemu"
ln -s "$kemu_src" "$l4h_root/third_party/kemu"

linux_src=$build_cache/linux-6.18
linux_stamp=$build_cache/linux-src.stamp
if [ ! -f "$linux_src/Makefile" ] || [ "$(cat "$linux_stamp" 2>/dev/null || true)" != "$LINUX_VERSION" ]; then
  fetch_cached "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-${LINUX_VERSION}.tar.xz" \
    "$dl_cache/linux-${LINUX_VERSION}.tar.xz" "$LINUX_SHA256"
  rm -rf "$linux_src"
  mkdir -p "$linux_src"
  tar -xJf "$dl_cache/linux-${LINUX_VERSION}.tar.xz" --strip-components=1 -C "$linux_src"
  echo "$LINUX_VERSION" > "$linux_stamp"
fi
mkdir -p "$build_cache/linux-l4h-aarch64"
ln -sfn "$linux_src" "$l4h_root/build/linux-6.18"
ln -sfn "$build_cache/linux-l4h-aarch64" "$l4h_root/build/linux-l4h-aarch64"
SEL4_PREFIX="$sel4_prefix" L4H_ARCH=aarch64 bash "$l4h_root/tools/build-linux.sh" vmlinux
aarch64-linux-gnu-objcopy --strip-debug "$build_cache/linux-l4h-aarch64/vmlinux" "$out_dir/vmlinux.stripped"

# 7. Export assembly helpers for downstream assemble.sh
cp "$l4h_root/tools/add-modules.py" "$out_dir/add-modules.py"
cp "$l4h_root/tools/pack-fl4-image.py" "$out_dir/pack-fl4-image.py"

ls -lh "$out_dir"
