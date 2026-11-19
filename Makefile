PROTO_BASE_REF ?= origin/main
GOTESTPACKAGES ?= ./...

.PHONY: check fmt-check vet test tidy-check check-generated check-swift-generated e2e proto proto-docs proto-check proto-breaking tla swift-project release-bundle headless-release install-bundle test-rust lint-rust test-data-worker lint-data-worker audit-data-worker

check: fmt-check vet test tidy-check proto-check check-generated

fmt-check:
	test -z "$$(gofmt -l $$(git ls-files '*.go'))"

vet:
	go vet ./...

test:
	go test $(GOTESTFLAGS) $(GOTESTPACKAGES) -count=1

tidy-check:
	go mod tidy -diff

check-generated:
	sh build/check-generated.sh go

check-swift-generated:
	sh build/check-generated.sh swift

proto:
	buf generate --exclude-path api/google
	sh build/generate-swift.sh
	$(MAKE) proto-docs

proto-docs:
	mkdir -p .build
	buf build --as-file-descriptor-set -o .build/api-descriptors.binpb
	go run ./internal/cmd/protodoc .build/api-descriptors.binpb docs/reference/api

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

e2e:
	test -n "$$AGENT_VMM_EXTERNAL_E2E_CONFIG"
	go test -tags=e2e ./test/e2e -run '^TestExternalSettingsBridgeE2E$$' -v -count=1 -timeout=20m

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
