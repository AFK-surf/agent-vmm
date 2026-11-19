#!/bin/sh
set -eu
repository=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$repository"
case "${1:-}" in
  go)
    buf generate --exclude-path api/google --output "$work"
    find api -name '*.pb.go' -type f | while IFS= read -r path; do
      diff -u "$path" "$work/$path"
    done
    find "$work" -name '*.pb.go' -type f | while IFS= read -r path; do
      test -f "${path#"$work/"}"
    done
    buf build --as-file-descriptor-set -o "$work/api-descriptors.binpb"
    go run ./internal/cmd/protodoc "$work/api-descriptors.binpb" "$work/api-docs"
    diff -r docs/reference/api "$work/api-docs"
    ;;
  swift)
    sh build/generate-swift.sh "$work"
    diff -r apps/macos/AgentVMM/Generated "$work/apps/macos/AgentVMM/Generated"
    ;;
  *) echo 'usage: check-generated.sh go|swift' >&2; exit 2 ;;
esac
