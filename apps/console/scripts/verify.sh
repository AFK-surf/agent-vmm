#!/bin/sh
# Check the console workspace: `verify.sh` runs every gate, `verify.sh <cargo args>` one command.
set -eu
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
if [ "$#" -gt 0 ]; then
  exec cargo "$@"
fi
# `--all` would also check the Host crates this workspace depends on.
cargo fmt -p ui -p agent-vmm-console -- --check
cargo clippy --locked --workspace --all-targets --features harness -- -D warnings
cargo test --locked --workspace --features harness
