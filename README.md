# agent-vmm

Agent VMM runs isolated AI agent workloads in one Linux virtual machine on Apple Silicon.

The macOS host uses Virtualization.framework. The guest uses containerd and one gVisor sandbox for each workload.

`agent-vmmd` controls containers, storage, networks, image builds, and cross-namespace access inside the guest.

## Status

This project is new and has no production release.

The current implementation targets Apple Silicon and macOS 14 or later. Guest artifacts target Linux arm64.

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

The E2E runner creates its report at `.context/e2e/report.md`.

## License

The agent-vmm source code uses the [MIT License](LICENSE).

Guest artifacts also contain third-party components. Their own licenses continue to apply.

The signed bundle metadata records the pinned components. The guest SBOM records the installed packages.
