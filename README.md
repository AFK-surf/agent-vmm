# agent-vmm

Agent VMM runs isolated AI agent workloads in one Linux virtual machine on Apple Silicon. A Go host controller owns the VM and policy database; agents use private namespace-bound sessions, while the optional macOS app uses the operator API.

The macOS host uses Virtualization.framework. The guest uses containerd and one gVisor sandbox for each workload.

`agent-vmmd` controls containers, storage, networks, image builds, and cross-namespace access inside the guest.

## Status

This project is new and has no production release.

The current implementation targets Apple Silicon and macOS 15 or later. Guest artifacts target Linux arm64.

## Interfaces

`agent-vmm-host` exposes one `0600` operator Unix socket and creates a separate `0600` Unix socket for each agent session. An agent cannot choose or replace its guest namespace in a request. The private guest protocol is reachable only by the host controller; no raw runtime relay is installed.

The SwiftUI app shows system health, Agent environments, resource use, structured logs, activity, and diagnostics. Closing the app does not stop the sidecar, VM, sessions, or workloads.

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

Regenerate and validate the product protocols with `make proto` and `make proto-check`. Generate the Xcode project with `make swift-project`; create the app/helper and LaunchAgent payload with `make release-bundle`, then use `build/host/install.sh`. The default development guest requires the default ad-hoc app signature; a release signing identity requires a release-signed guest bundle. The uninstaller retains VM data unless invoked with `--purge`.

The E2E runner creates its report at `.context/e2e/report.md`.

## License

The agent-vmm source code uses the [MIT License](LICENSE).

Guest artifacts also contain third-party components. Their own licenses continue to apply.

The signed bundle metadata records the pinned components. The guest SBOM records the installed packages.
