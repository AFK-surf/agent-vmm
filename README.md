# agent-vmm

Agent VMM runs isolated AI agent workloads in one Linux virtual machine.
A Go Host owns the VM and policy database.
The Guest uses containerd and one gVisor sandbox for each workload.

## Status

The project has no production release. Interfaces can change without notice.

| Host platform | Status |
| --- | --- |
| macOS 15+, Apple Silicon | Supported, Virtualization.framework |
| Linux arm64, KVM | Experimental, `agent-vmm-host.service` |

Guest artifacts target Linux arm64.

## Get started

Read the [quickstart](docs/quickstart.md) to install a Host package.
Read the [build guide](docs/build.md) to build one from source.
This repository does not publish binaries.

## Concepts

- **Host:** owns the VM, local policy, and operator API.
- **Guest:** runs the container runtime inside the VM.
- **Environment:** owns one namespace, workloads, and private volumes.
- **Appliance:** runs a system service with an approved manifest and data volume.
- **Controller:** registers remote compute and manages its own allocations.

The [glossary](docs/glossary.md) defines these terms and their identifiers.

## Interfaces

Use [MCP](docs/user/mcp.md) to run workloads. Each issued token selects one environment.
Controllers use the [public protocols](docs/protocols/remote.md).
The Guest protocol is private to the Host.

The Host package is `Agent VMM Host.app`.
`apps/macos` is a development and preview debugging tool. Use MCP for workloads.

## Documentation

- [MCP workloads](docs/user/mcp.md)
- [Operator CLI](docs/operator/cli.md)
- [Install and update](docs/operator/install-and-update.md)
- [Cleanup and local resources](docs/operator/cleanup-and-local-resources.md)
- [Backup](docs/operator/backup.md) and [troubleshooting](docs/operator/troubleshooting.md)
- [Design](docs/design/overview.md) and [security model](docs/design/security-model.md)
- [Remote protocol](docs/protocols/remote.md), [trust and service](docs/protocols/trust-service.md), and [appliances](docs/protocols/appliance.md)
- [API](docs/reference/api/README.md), [CLI commands](docs/reference/cli.md), and [errors](docs/reference/errors.md)
- [Linux Host development](test/kvmdev/README.md), [external controller E2E](test/e2e/README.md), and [TLA+ models](spec/tla/README.md)

Read [SECURITY.md](SECURITY.md) for trust boundaries and vulnerability reports.
Read [CONTRIBUTING.md](CONTRIBUTING.md) before you send a change.

## License

The source uses `AGPL-3.0-or-later`. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Derived files retain the licenses listed in NOTICE.
