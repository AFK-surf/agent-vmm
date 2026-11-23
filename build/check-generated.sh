#!/bin/sh
set -eu
repository=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$repository"
case "${1:-}" in
  docs)
    buf build --as-file-descriptor-set -o "$work/api-descriptors.binpb"
    PROTOC=$(sh build/protoc.sh "$work") cargo run --locked -p agent-vmm-proto --bin protodoc -- "$work/api-descriptors.binpb" "$work/api-docs"
    diff -r docs/reference/api "$work/api-docs"
    ;;
  *) echo 'usage: check-generated.sh docs' >&2; exit 2 ;;
esac
