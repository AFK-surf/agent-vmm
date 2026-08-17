.PHONY: e2e proto proto-check tla swift-project release-bundle test-data-worker lint-data-worker audit-data-worker

proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" buf generate
	buf generate --template buf.gen.swift.yaml --path api/appliance/v1 --path api/host/v1 --path api/trust/v1 --path api/service/v1

proto-check:
	buf lint
	buf breaking --against api/proto-baseline.binpb

tla:
	sh tools/tla/run.sh

swift-project:
	cd apps/macos/AgentVMM && xcodegen generate

release-bundle:
	sh build/host/package.sh

install-bundle:
	sh build/host/install.sh

e2e:
	sh test/e2e/run.sh

test-data-worker:
	cargo test --manifest-path data/agent-vmm-data/Cargo.toml --all-targets

lint-data-worker:
	cargo clippy --manifest-path data/agent-vmm-data/Cargo.toml --all-targets -- -D warnings

audit-data-worker:
	cargo audit --file data/agent-vmm-data/Cargo.lock
	cargo metadata --manifest-path data/agent-vmm-data/Cargo.toml --format-version 1 --locked | jq -e '[.packages[] | select(.name == "pakery-core" or .name == "pakery-crypto" or .name == "pakery-spake2") | select(.version == "0.2.1" and .license == "MIT OR Apache-2.0" and .repository == "https://github.com/djx-y-z/pakery")] | length == 3' >/dev/null
