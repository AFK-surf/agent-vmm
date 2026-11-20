#!/usr/bin/env bash
set -euo pipefail

profile=${1:-development}
l4h_root=/l4h-src
dl_cache=/var/cache/l4h-downloads
build_cache=/l4h-build-cache
out_dir=/l4h-artifacts

SEL4_COMMIT=62e345c240a15671484530b4e6e069ce266c28d7
SEL4_SHA256=8cb77d4b4966874c51083d120d6ad39c98d62fa684f709ec3ff478b9fb1d05ae
RUST_SEL4_COMMIT=86a0dbf4f53e6766734331c5df61851489ed38a1
RUST_SEL4_SHA256=1e29856658b0c0ee1a6f1e00b3f65cc330597e590b451c5ea7d2c9d47aa8d19f
KEMU_COMMIT=e738d0e4b3ab04bd26a75814cc964db8a4bca964
KEMU_SHA256=acb23280248c60db18c54c42138362a27b4000029a5db61a84c06121f6be62a0
LINUX_VERSION=6.18.64
LINUX_SHA256=020d45510c539972e6f14c5c71566878d3b7ef56fb082b3e09134f2ebaade562

mkdir -p "$dl_cache" "$build_cache" "$out_dir"

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

# 1. Prepare seL4 source + apply patches
sel4_src=$build_cache/seL4-src
sel4_stamp=$build_cache/seL4-src.stamp
sel4_want=$(cat "$l4h_root"/kernel/patches/*.patch | sha256sum | cut -d' ' -f1)-$SEL4_COMMIT
if [ ! -d "$sel4_src" ] || [ "$(cat "$sel4_stamp" 2>/dev/null || true)" != "$sel4_want" ]; then
  rm -rf "$sel4_src"
  mkdir -p "$sel4_src"
  if [ -f "$l4h_root/kernel/seL4/CMakeLists.txt" ]; then
    cp -a "$l4h_root/kernel/seL4/." "$sel4_src/"
  else
    fetch_cached "https://github.com/seL4/seL4/archive/${SEL4_COMMIT}.tar.gz" "$dl_cache/seL4-${SEL4_COMMIT}.tar.gz" "$SEL4_SHA256"
    tar -xzf "$dl_cache/seL4-${SEL4_COMMIT}.tar.gz" --strip-components=1 -C "$sel4_src"
  fi
  for p in "$l4h_root"/kernel/patches/*.patch; do
    patch -d "$sel4_src" -p1 -s < "$p"
  done
  echo "$sel4_want" > "$sel4_stamp"
fi

# 2. Build seL4 kernel for apple-vz
sel4_build=$build_cache/kernel-apple-vz-$profile
sel4_prefix=$out_dir/sel4-prefix
debug=ON
printing=ON
if [ "$profile" = release ]; then
  debug=OFF
  printing=OFF
fi
rm -rf "$sel4_prefix"
cmake -G Ninja -S "$sel4_src" -B "$sel4_build" \
  -C "$l4h_root/kernel/configs/l4h-apple-vz.cmake" \
  -DKernelDebugBuild="$debug" \
  -DKernelPrinting="$printing" \
  -DCMAKE_INSTALL_PREFIX="$sel4_prefix" \
  -DCROSS_COMPILER_PREFIX=aarch64-linux-gnu-
ninja -C "$sel4_build" all
ninja -C "$sel4_build" install

# 3. Prepare rust-sel4 source + apply patches
rs_src=$build_cache/rust-sel4-src
rs_stamp=$build_cache/rust-sel4-src.stamp
rs_want=$(cat "$l4h_root"/third_party/patches/rust-sel4/*.patch | sha256sum | cut -d' ' -f1)-$RUST_SEL4_COMMIT
if [ ! -d "$rs_src" ] || [ "$(cat "$rs_stamp" 2>/dev/null || true)" != "$rs_want" ]; then
  rm -rf "$rs_src"
  mkdir -p "$rs_src"
  if [ -f "$l4h_root/third_party/rust-sel4/Cargo.toml" ]; then
    cp -a "$l4h_root/third_party/rust-sel4/." "$rs_src/"
  else
    fetch_cached "https://github.com/seL4/rust-sel4/archive/${RUST_SEL4_COMMIT}.tar.gz" "$dl_cache/rust-sel4-${RUST_SEL4_COMMIT}.tar.gz" "$RUST_SEL4_SHA256"
    tar -xzf "$dl_cache/rust-sel4-${RUST_SEL4_COMMIT}.tar.gz" --strip-components=1 -C "$rs_src"
  fi
  for p in "$l4h_root"/third_party/patches/rust-sel4/*.patch; do
    patch -d "$rs_src" -p1 -s < "$p"
  done
  echo "$rs_want" > "$rs_stamp"
fi
# Ensure l4h host workspace sees patched rust-sel4
rm -rf "$l4h_root/third_party/rust-sel4"
ln -s "$rs_src" "$l4h_root/third_party/rust-sel4"

# 4. Build sel4-kernel-loader + static musl sel4-kernel-loader-add-payload
loader_target=$build_cache/loader-target-$profile
host_musl="$(uname -m)-unknown-linux-musl"
(
  cd "$rs_src"
  CC=aarch64-linux-gnu-gcc SEL4_PREFIX="$sel4_prefix" \
    cargo +nightly-2026-08-21 build --release \
    -Z build-std=core,alloc,compiler_builtins -Z build-std-features=compiler-builtins-mem \
    --target aarch64-unknown-none -p sel4-kernel-loader --target-dir "$loader_target"
  RUSTFLAGS="-C target-feature=+crt-static" \
    cargo +nightly-2026-08-21 build --release --target "$host_musl" \
    -p sel4-kernel-loader-add-payload --target-dir "$loader_target"
)
cp "$loader_target/aarch64-unknown-none/release/sel4-kernel-loader" "$out_dir/sel4-kernel-loader"
cp "$loader_target/$host_musl/release/sel4-kernel-loader-add-payload" "$out_dir/sel4-kernel-loader-add-payload"

# 5. Build l4h host components + l4h-root
host_target=$build_cache/host-target-$profile
export SEL4_PREFIX="$sel4_prefix"
export CC_aarch64_sel4=aarch64-linux-gnu-gcc
export CC_aarch64_sel4_roottask=aarch64-linux-gnu-gcc
export LIBCLANG_PATH=$(dirname "$(find /usr/lib -name 'libclang.so*' -print -quit)")
export SOURCE_DATE_EPOCH=1700000000
export L4H_VERSION=0.1.0-apple-vz

(
  cd "$l4h_root/host"
  nicds=(virtio gvnic ena ixgbe e1000e mlx5 r8125)
  nicd_pkgs=()
  for d in "${nicds[@]}"; do nicd_pkgs+=(-p "l4h-nicd-$d"); done
  cargo +nightly-2026-08-21 build --release --target aarch64-sel4 --target-dir "$host_target" \
    -Z build-std=core,alloc,compiler_builtins -Z build-std-features=compiler-builtins-mem \
    -p l4h-vmm -p l4h-blkd -p l4h-netd -p l4h-trustd -p l4h-memd -p l4h-logd -p l4h-usbd \
    "${nicd_pkgs[@]}"

  out="$host_target/aarch64-sel4/release"
  mkdir -p "$out/stripped"
  for c in vmm blkd netd trustd memd logd usbd "${nicds[@]/#/nicd-}"; do
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

# 6. Prepare kemu + linux-6.18.64 and build ARCH=l4h SUBARCH=aarch64 vmlinux
if [ ! -f "$l4h_root/third_party/kemu/overlay/kernel/kemu/Kconfig" ]; then
  kemu_src=$build_cache/kemu-src
  if [ ! -f "$kemu_src/overlay/kernel/kemu/Kconfig" ]; then
    rm -rf "$kemu_src"
    mkdir -p "$kemu_src"
    fetch_cached "https://github.com/losfair/kemu/archive/${KEMU_COMMIT}.tar.gz" "$dl_cache/kemu-${KEMU_COMMIT}.tar.gz" "$KEMU_SHA256"
    tar -xzf "$dl_cache/kemu-${KEMU_COMMIT}.tar.gz" --strip-components=1 -C "$kemu_src"
    git -C "$kemu_src" init -q
    git -C "$kemu_src" add -A
    git -C "$kemu_src" -c user.name=l4h -c user.email=l4h@localhost commit -q -m init
  fi
  rm -rf "$l4h_root/third_party/kemu"
  ln -s "$kemu_src" "$l4h_root/third_party/kemu"
fi

linux_src=$build_cache/linux-6.18
if [ ! -f "$linux_src/Makefile" ]; then
  fetch_cached "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-${LINUX_VERSION}.tar.xz" \
    "$dl_cache/linux-${LINUX_VERSION}.tar.xz" "$LINUX_SHA256"
  rm -rf "$linux_src"
  mkdir -p "$linux_src"
  tar -xJf "$dl_cache/linux-${LINUX_VERSION}.tar.xz" --strip-components=1 -C "$linux_src"
fi
mkdir -p "$l4h_root/build" "$build_cache/linux-l4h-aarch64"
ln -sfn "$linux_src" "$l4h_root/build/linux-6.18"
ln -sfn "$build_cache/linux-l4h-aarch64" "$l4h_root/build/linux-l4h-aarch64"
L4H_ARCH=aarch64 bash "$l4h_root/tools/build-linux.sh" vmlinux
aarch64-linux-gnu-objcopy --strip-debug "$build_cache/linux-l4h-aarch64/vmlinux" "$out_dir/vmlinux.stripped"

ls -lh "$out_dir"
