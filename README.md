# agent-vmm

Agent VMM runs isolated AI agent workloads in one Linux virtual machine on Apple Silicon. A Go host controller owns the VM and policy database; agents use private namespace-bound sessions, while the optional macOS app uses the operator API.

The macOS host uses Virtualization.framework. The guest uses containerd and one gVisor sandbox for each workload.

`agent-vmmd` controls containers, storage, networks, image builds, and cross-namespace access inside the guest.

## Status

This project is new and has no production release.

The current implementation targets Apple Silicon and macOS 15 or later. Guest artifacts target Linux arm64.

## Interfaces

`agent-vmm-host` exposes one `0600` operator Unix socket and creates a separate `0600` Unix socket for each agent session. An agent cannot choose or replace its guest namespace in a request. The private guest protocol is reachable only by the host controller; no raw runtime relay is installed.

The SwiftUI `Agent VMM.app` is a development harness for observing and operating
the host API; it is not a production runtime dependency. Production packaging
uses the headless, product-neutral `Agent VMM Host.app`, whose main executable
is `Contents/MacOS/agent-vmm-host` and whose background-only bundle contains
only the host, data/lifecycle helpers, and verified Guest payload.

## Design

Read [the architecture design](docs/architecture.md) for the security model, component boundaries, and design decisions.

## Verification

Run the Go checks on macOS:

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

Run the complete macOS and guest test suite:

```sh
make e2e
```

Regenerate and validate the product protocols with `make proto` and `make proto-check`. Generate the development Xcode harness with `make swift-project`; create the headless Host App and LaunchAgent payload with `make headless-release`, then use `build/host/install.sh`. The default development guest requires the default ad-hoc test identity; a release signing identity requires a release-signed guest bundle. Test packaging may use the guest SBOM/provenance as explicitly marked fixture metadata; release packaging requires `AGENT_VMM_HOST_SBOM` and `AGENT_VMM_HOST_PROVENANCE`. Real signing, notarization, and publication are release-owner steps and are not performed by local conformance checks. The uninstaller retains VM data unless invoked with `--purge`; build-identity transitions and pre-release cleanup are documented in [the recovery runbook](docs/recovery.md).

The E2E runner creates its report at `.build/test-results/e2e/report.md`.

## License

The agent-vmm source code uses the [MIT License](LICENSE).

Guest artifacts also contain third-party components. Their own licenses continue to apply.

The signed bundle metadata records the pinned components. The guest SBOM records the installed packages.
