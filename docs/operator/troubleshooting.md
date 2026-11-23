# Troubleshooting

Preserve the first exact Host, dom0, Guest, and controller failure before another action.
Use `agent-vmm diagnose --json`, bounded logs, and the original operation receipt.
A last successful stage shows progress. It does not identify the cause of the next failure.
Unknown observations do not authorize state deletion.

The Host downloads an archive URL through the fixed system `/usr/bin/curl`.
Registry pulls use the HTTPS client of the Host.
Both paths check every resolved IPv4 address and pin one accepted address for the request.
Both paths reject proxies. The archive path rejects redirects, and the registry path checks each redirect target.
The Host commits a new image only after the download completes and the size, hash, and manifest checks pass.

## Symptoms

### Host and VM startup

**Host refuses its database**: Read `hostStartFailure` and `version --json`.
Preserve state. Select a compatible Host, or get exact approval for purge and reinstallation.

**Host job has a loaded state but no readable facts**: Read process facts, exit status, and Host logs.
Resolve the exact service owner and first startup failure.

**VM starts but dom0 never becomes ready**: Read the current serial log and the mapped VM-start operation.
Identify the failed boot stage and preserve the pool file.

**Several Virtualization processes exist**: Match the open pool file to the exact Carrier.
Stop only the confirmed stale owner after graceful shutdown expires.

**Update ends observation during transition**: Read the lifecycle operation and disposition.
Retry the same request and exact artifact. Do the reported fixed administrator action.

### Workloads and images

**Workload VM does not become healthy**: Read the start error. It contains the end of the VM console.
Correct the image or the workload command, then start the container again.

**Import stream ends with EOF**: Observe the original import or image receipt.
Confirm the committed result before an exact bounded retry.

**MCP runtime is unavailable**: Read VM, appliance, allocation, and frontend status.
Repair that exact owner and let the client reconnect.

**Pool allocation reports storage headroom**: Read the free space of the storage that backs the pool.
Free that storage. The pool keeps an emergency reserve and refuses new allocations below it.

**PID or disk capacity has no room**: Read the requested contract and actual owner usage.
Reduce that request or free resources within the approved scope.

### Service routes

**Service route is unavailable after restart**: Read source instance, export, import, and authorization.
Refresh current facts and resolve the virtual name again.

**One appliance service import fails**: Read the error and fences for that import.
Retry that import through ordinary reconciliation.

## Carrier, dom0, and Guest startup

On macOS, the Carrier appends the console of seL4, l4h, and dom0 to the serial log.
The Carrier waits 90 seconds for the dom0 control listener. A failed start stops the VM and keeps the error.
The Carrier reports that error and the VM state through `agent-vmm vm status`.
An older serial record is not evidence for the current request.

The dom0 service waits 60 seconds for the health reply of a new workload VM.
A start that exceeds this time fails with the last 4096 bytes of the VM console.
Container logs keep the newest bytes within the log limit of the container.
Use `--after-cursor` to continue. `gap=true` reports unavailable or omitted bytes.

A VM start preserves `pool.raw` and uses a new boot ID.
Host restart adopts the running workload VMs and preserves workloads that are not retired.
Do not infer a new VM boot from a new Host process.

## Connector reachability

The connector connects to the configured gateway host name with TLS 1.3.
It uses the configured roots and the original host name. Each connection attempt has a 20-second budget.
The connector retries with a capped 30-second backoff and jitter.
No fixed IP, certificate bypass, or persistent route cache supplies a fallback.
A successful TLS probe does not prove enrollment or command admission.

A rejected credential or an unsupported protocol stops the connector until its owner acts.
Other failures remain observable and retry under the connector owner policy.
Only explicit credential revocation ends the connector authority of that registration.
Read [the remote protocol](../protocols/remote.md) before resolving an unknown result.

For one registration, read `status --registration-id ID` through the lifecycle helper.
`remoteProtocol` appears only for the current protocol refusal and can be absent after Host restart.
It does not report Host health or registration admission.

**`remoteProtocol.hostSide=too_old`**: Update the local component through existing maintenance.

**`remoteProtocol.hostSide=too_new`**: Wait for a compatible controller update. Do not reinstall the Host.

**The helper output contract is absent or too old**: Use the existing update path from a verified target bundle.

**The helper output contract is too new**: Update Comma before using operations that it cannot interpret.

Use [install and update](install-and-update.md) for application replacement.
Use [cleanup](cleanup-and-local-resources.md) for exact resource deletion.
