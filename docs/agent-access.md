# Agent access

## Local MCP

Build or install the signed bundle, then configure the MCP host to execute:

```text
~/Applications/Agent VMM.app/Contents/Helpers/agent-vmm-mcp
```

The server communicates only on stdio. One process owns one LOCAL environment;
ending the process releases its session and loopback forwards but retains the
environment and named volumes.

## Remote controller

In Settings, enter a TLS `host:port`, controller name, one-time enrollment token,
and bounded pool size. The app creates the host-owned registration and passes the
token once to the signed connector through stdin. The returned opaque credential
is stored in macOS Keychain and is absent from SQLite, snapshots, logs, and support
bundles. The connector LaunchAgent discovers non-secret registration metadata
through the restricted local socket and then connects outbound.

The public contract is `api/remote/v1/remote.proto`. Controller adapters map their
worker identity to registration/allocation/opaque workload references and use the
per-lease AgentRuntimeService tunnel. They must not send local environment or
namespace identities. No Salix or cue SDK is required by Agent VMM.

Enable admits new leases. Drain prevents new work while existing leases finish.
Revoke deletes the Keychain credential and closes remote access. Release retains
containers and volumes for local inspection; discard is irreversible and requires
the current cleanup revision and exact volume confirmation.

The restricted socket prevents accidental OperatorService use but shares the
logged-in user's UID. A compromised connector is therefore not claimed to have
OS-level isolation. V0 also excludes multiple controllers, public ingress,
dynamic VM resizing, oversubscription, and a cloud scheduler.
