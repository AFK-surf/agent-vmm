# agent-vmm

Agent VMM runs each isolated AI agent workload in its own l4h virtual machine.
The Rust Host owns the policy database and operator API.
The dom0 service manages workload VM instances.

## Status

The project has no production release. Interfaces can change without notice.

| Host platform | Status |
| --- | --- |
| macOS 15+, Apple Silicon | Host package with a separate Carrier, awaiting l4h support for Virtualization.framework |
| Linux arm64 | Experimental, requires an l4h dom0 |

Guest artifacts target Linux arm64.

## Get started

Read the [quickstart](docs/quickstart.md) to install a Host package.
Read the [build guide](docs/build.md) to build one from source.
This repository does not publish binaries.

## Concepts

- **Host:** owns local policy and the operator API.
- **Guest:** runs the workload and its private runtime service.
- **Environment:** owns workloads and private volumes.
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
- [Linux Host packaging](docs/build.md#linux-host-package) and [TLA+ models](spec/tla/README.md)

Read [SECURITY.md](SECURITY.md) for trust boundaries and vulnerability reports.
Read [CONTRIBUTING.md](CONTRIBUTING.md) before you send a change.

## License

The source uses `AGPL-3.0-or-later`. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Derived files retain the licenses listed in NOTICE.
