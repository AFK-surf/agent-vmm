PROTO_BASE_REF ?= origin/main

.PHONY: check fmt-check vet test check-api-docs check-swift-generated proto proto-docs proto-check proto-breaking tla swift-project release-bundle headless-release install-bundle test-rust lint-rust test-data-worker lint-data-worker audit-data-worker

check: fmt-check vet test proto-check

fmt-check:
	cargo fmt --check -p xtask

vet:
	cargo clippy --locked -p xtask --all-targets -- -D warnings

test:
	cargo test --locked -p xtask

check-api-docs:
	sh build/check-generated.sh docs

check-swift-generated:
	sh build/check-generated.sh swift

proto:
	sh build/generate-swift.sh
	$(MAKE) proto-docs

proto-docs:
	mkdir -p .build
	buf build --as-file-descriptor-set -o .build/api-descriptors.binpb
	PROTOC=$$(sh build/protoc.sh .build) cargo run --locked -p agent-vmm-proto --bin protodoc -- .build/api-descriptors.binpb docs/reference/api

proto-check:
	buf lint

proto-breaking:
	buf breaking --against '.git#ref=$(PROTO_BASE_REF)'

tla:
	sh tools/tla/run.sh

swift-project:
	cd apps/macos/AgentVMM && xcodegen generate

release-bundle:
	sh build/host/package.sh

headless-release: release-bundle

install-bundle:
	sh build/host/install.sh

# The contract test of the lifecycle helper needs a build that stays away from the production Host.
test-rust:
	cargo test --workspace --locked
	cargo test --locked -p agent-vmm-lifecycle --features e2e-isolation --test contract

lint-rust:
	cargo clippy --workspace --locked --all-targets -- -D warnings
	cargo clippy --locked -p agent-vmm-lifecycle --features e2e-isolation --all-targets -- -D warnings

test-data-worker:
	cargo test --locked --manifest-path data/agent-vmm-data/Cargo.toml -p agent-vmm-data -p netdev@0.46.3+comma.1 --all-targets

lint-data-worker:
	cargo clippy --locked --manifest-path data/agent-vmm-data/Cargo.toml -p agent-vmm-data -p netdev@0.46.3+comma.1 --all-targets -- -D warnings

audit-data-worker:
	cargo audit --file Cargo.lock
