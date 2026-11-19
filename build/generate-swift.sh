#!/bin/sh
set -eu
output=${1:-.}
buf generate --template buf.gen.swift.yaml --output "$output" \
  --path api/appliance/v1 --path api/host/v1 --path api/trust/v1 \
  --path api/service/v1 --path api/google/rpc
