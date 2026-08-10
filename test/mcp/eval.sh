#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$repository"
go test ./internal/mcpserver/... -count=1
go run ./test/mcp/evalcmd .context/token.json
