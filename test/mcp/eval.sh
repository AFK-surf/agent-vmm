#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
if [ "$#" -ne 1 ]; then
  echo "usage: $0 <provider-config.json>" >&2
  exit 2
fi
cd "$repository"
go test ./internal/mcpserver/... -count=1
go run ./test/mcp/evalcmd "$1"
