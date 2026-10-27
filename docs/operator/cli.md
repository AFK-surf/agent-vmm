# Read the managed CLI

Use [the generated command reference](../reference/cli.md) for accepted options.
Observation returns facts for the selected service, registration, environment, or container.
It does not contact a controller or read controller credentials.

```sh
agent-vmm inspect --json
agent-vmm diagnose --json
agent-vmm logs --component host --limit 50 --json
agent-vmm logs --environment-id ENV --container-id CONTAINER --json
agent-vmm operation get --owner lifecycle --operation-id OPERATION --json
agent-vmm operation follow --owner host --operation-id OPERATION
```

One observation returns one JSON object. Operation follow emits bounded JSON lines.
The envelope includes target, source, observation time, freshness, stage, issue, original operation, and a finite next action.
Unknown facts remain unknown. Do not treat a failed read as absence.
Use the returned disposition to continue work, wait, retry the same request, or request the reported administrator action.

Inspect has a 30-second budget. Diagnose has a 30-second and 2 MiB budget.
Logs return at most 100 records and 1 MiB.
Recognized secret fields and headers are redacted.
Container output remains hidden unless `--include-container-output` is present.
A log cursor identifies retained file content. `gap=true` reports expired or omitted bytes.
Repeat the read with `--after-cursor` to continue within its bounds.

## Whole-VM actions

```sh
agent-vmm vm status --json
agent-vmm vm stop --request-id REQUEST --json
```

Start and stop authorize the complete VM action, including workloads absent from the bounded summary.
The summary is review evidence. Partial or overflow status does not narrow the action.
The CLI captures VM state, boot ID, owner references, and explicit whole-VM acknowledgement.
The Host checks that exact fence before it accepts the mutation.
A changed fence requires refreshed facts and a new decision.

Use [the retry contract](../reference/errors.md#retry-and-unknown-outcomes) for request identity, admission, and unknown outcomes.

## Update dispositions

| Disposition | Caller action |
| --- | --- |
| `continue_normal_work` | Continue normal work. |
| `exclusive_transition_in_progress` | Wait for the shared Host transition. Retry the same request ID. |
| `retry_same_request_at` | Retry the same request after the reported time. |
| `administrator_action_required` | Ask an administrator to perform the reported fixed service action. |
| `terminal_action_required` | Review the failure before a new explicit action. |

Use [troubleshooting](troubleshooting.md) to select a remedy from the first exact failure.
Use [cleanup](cleanup-and-local-resources.md) for environment deletion.
