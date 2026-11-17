# Troubleshooting

Preserve the first exact Host, Guest, and controller failure before another action.
Use `agent-vmm diagnose --json`, bounded logs, and the original operation receipt.
A last successful stage shows progress. It does not identify the next failure's cause.
Unknown observations do not authorize state deletion.

macOS streams runtime OCI downloads through the fixed system `/usr/bin/curl`. Linux uses the Go HTTPS client.
Both paths check every resolved IPv4 address and pin one accepted address for the request.
They reject proxies and redirects and verify TLS 1.2 or later through the platform trust store.
The Guest commits a new image only after download completion and the existing size, hash, manifest, and namespace checks.

## Symptoms

| Symptom | Check | Action |
| --- | --- | --- |
| Host refuses its database | Read `hostStartFailure` and `version --json` | Preserve state. Select a compatible Host or obtain exact approval for purge and reinstallation. |
| Host job is loaded but unreadable | Read process facts, exit status, and Host logs | Resolve the exact service owner and first startup failure. |
| VM starts but VMMD health never appears | Read the current serial log and mapped VM-start operation | Identify the failed Guest stage and preserve the disk. |
| Several Virtualization processes exist | Match open state files to the exact Host generation | Stop only the confirmed stale owner after graceful shutdown expires. |
| Update ends observation during transition | Read the lifecycle operation and disposition | Retry the same request and exact artifact. Perform the reported fixed administrator action. |
| Import stream ends with EOF | Observe the original import or image receipt | Confirm the committed result before an exact bounded retry. |
| MCP runtime is unavailable | Read VM, appliance, allocation, and frontend status | Repair that exact owner and let the client reconnect. |
| Service route is unavailable after restart | Read source instance, export, import, and authorization | Refresh current facts and re-resolve the virtual name. |
| One appliance service import fails | Read that import's error and fences | Retry that import through ordinary reconciliation. |
| Copy-up reports invalid cross-device link | Identify the exact snapshot's upper/work project IDs | Preserve data and repair only the affected snapshot. |
| State disk lacks physical backing | Check exact file length, allocated storage, and VMM lock | Stop offline and repair Host storage. Current startup restores backing only without a live VMM. |
| PID or disk capacity is exhausted | Read the requested contract and actual owner usage | Reduce that request or free resources within the approved scope. |

## Guest and Host startup

VMMD reports bootstrap, OCI storage, containerd, recovery, and listener stages through the serial log.
The Host operation maps those records to the current lifecycle request.
An older orphan serial record is not evidence for the current request.
Guest logs prefer the newest bytes within their response bound and retain one rotated segment.
Use `--after-cursor` to continue. `gap=true` reports unavailable or omitted bytes.

A VM recovery attempt preserves `state.raw` and uses fresh boot-scoped authority.
Unexpected VZ stop recovery has bounded retries. Exhaustion requires operator action through VM status.
Linux Host restart can adopt a running Guest and must preserve non-retired workloads.
Do not infer a new Guest boot from a new Host process.

## Connector reachability

The connector tries the system route, then bounded configured interface and DNS candidates.
Each direct pass has a 20-second budget and retains normal capped reconnect backoff.
Every candidate uses the original hostname, configured roots, TLS 1.3, and HTTP/2.
DNS and interface selection establish reachability. Stock TLS verification establishes endpoint authority.
No fixed IP, public DNS server, certificate bypass, or persistent route cache supplies a fallback.
A successful TLS probe does not prove enrollment or command admission.

Authentication and configuration failures remain observable and retry under the connector's owner policy.
`DATA_WORKER_UNAVAILABLE` means that the local data worker did not answer. The connector retries.
The Host restarts an exited worker with a backoff from 1 to 30 seconds.
The connector reads the worker endpoint every 5 seconds. When the endpoint changes, the connector reconnects with a new endpoint observation.
Only explicit credential revocation ends that registration's connector authority.
Read [the remote protocol](../protocols/remote.md) before resolving an unknown result.

For one registration, read `status --registration-id ID` through the lifecycle helper.
`remoteProtocol` appears only for the current protocol refusal and can be absent after Host restart.
It does not report Host health or registration admission.

| Protocol refusal | Recovery |
| --- | --- |
| `remoteProtocol.hostSide=too_old` | Update the local component through existing maintenance. |
| `remoteProtocol.hostSide=too_new` | Wait for a compatible controller update. Do not reinstall the Host. |
| Helper output contract is missing or too old | Use the existing update path from a verified target bundle. |
| Helper output contract is too new | Update Comma before using operations that it cannot interpret. |

Use [install and update](install-and-update.md) for application replacement.
Use [cleanup](cleanup-and-local-resources.md) for exact resource deletion.
