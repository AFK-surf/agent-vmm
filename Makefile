.PHONY: e2e proto proto-check swift-project release-bundle

proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" buf generate
	buf generate --template buf.gen.swift.yaml --path api/host/v1

proto-check:
	buf lint
	buf breaking --against api/proto-baseline.binpb

swift-project:
	cd apps/macos/AgentVMM && xcodegen generate

release-bundle:
	sh build/host/package.sh

install-bundle:
	sh build/host/install.sh

e2e:
	sh test/e2e/run.sh
