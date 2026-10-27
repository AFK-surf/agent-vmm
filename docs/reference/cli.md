# Managed CLI reference

<!-- Generated from cmd/agent-vmm/managed_cli.go by make cli-docs. -->

The formal Host bundle includes these commands. Each command accepts only its listed options.
Use `--json` for the structured result. Unknown or incomplete arguments return `invalid_arguments` and exit code 2.

| Command | Options |
| --- | --- |
| `agent-vmm cleanup execute` | `--json`, `--operator-socket`, `--environment-id`, `--kind`, `--request-id`, `--plan-revision`, `--confirm-volume`, `--confirm-irreversible` |
| `agent-vmm cleanup preview` | `--json`, `--operator-socket`, `--environment-id`, `--kind` |
| `agent-vmm diagnose` | `--operator-socket`, `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--registration-id`, `--environment-id`, `--workload-id` |
| `agent-vmm inspect` | `--operator-socket`, `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--registration-id`, `--environment-id`, `--workload-id` |
| `agent-vmm logs` | `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--operator-socket`, `--registration-id`, `--environment-id`, `--container-id`, `--component`, `--operation-id`, `--after-cursor`, `--limit`, `--include-container-output` |
| `agent-vmm operation follow` | `--operator-socket`, `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--owner`, `--operation-id`, `--follow` |
| `agent-vmm operation get` | `--operator-socket`, `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--owner`, `--operation-id` |
| `agent-vmm repair` | `--json`, `--service-type`, `--service-user`, `--service-app`, `--lifecycle-helper`, `--request-id`, `--registration-id` |
| `agent-vmm update` | `--json`, `--service-type`, `--service-user`, `--lifecycle-helper`, `--request-id`, `--target-release-id`, `--artifact-url`, `--artifact-sha256`, `--artifact-size` |
| `agent-vmm vm start` | `--json`, `--operator-socket`, `--request-id` |
| `agent-vmm vm status` | `--json`, `--operator-socket` |
| `agent-vmm vm stop` | `--json`, `--operator-socket`, `--request-id`, `--force` |
