#!/bin/sh
set -eu

target=aarch64-unknown-linux-musl
# Native Alpine builds use musl for C dependencies as well as Rust.
cargo build --locked --release --target "$target" -p l4h-dom0d -p vmm-guest-init
mkdir -p /binaries
install -m 0755 "target/$target/release/l4h-dom0d" "target/$target/release/vmm-guest-init" /binaries/
cargo run --locked --release -p xtask -- notices -o /binaries/THIRD_PARTY_NOTICES -licenses /src/third_party/licenses \
  -cargo-manifest /src/bins/l4h-dom0d/Cargo.toml -cargo-manifest /src/bins/vmm-guest-init/Cargo.toml \
  -cargo-target "$target"
install -m 0755 target/release/xtask /usr/local/bin/xtask
