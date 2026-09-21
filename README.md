# agent-vmm

Agent VMM runs isolated AI agent workloads in one Linux virtual machine on Apple Silicon. A Go host controller owns the VM and policy database; agents use private namespace-bound sessions, while the optional macOS app uses the operator API.

The macOS host uses Virtualization.framework. The guest uses containerd and one gVisor sandbox for each workload.

`agent-vmmd` controls containers, storage, networks, image builds, and cross-namespace access inside the guest.

## Managed Host CLI

The managed Host bundle includes the `agent-vmm` CLI for bounded local
observation and explicit Host operations. The CLI does not contact the BFT or
Salix control plane and does not read cluster credentials. Use explicit
registration, environment, workload, and container identifiers from BFT when
those scopes are required.

```sh
agent-vmm inspect --json
agent-vmm diagnose --json
agent-vmm logs --component host --limit 50 --json
agent-vmm logs --environment-id ENV --container-id CONTAINER --json
agent-vmm operation get --owner lifecycle --operation-id OPERATION --json
agent-vmm operation follow --owner host --operation-id OPERATION
agent-vmm vm status --json
agent-vmm vm stop --request-id REQUEST --json
agent-vmm update --request-id REQUEST --target-release-id RELEASE \
  --artifact-url HTTPS_URL --artifact-sha256 SHA256 --artifact-size BYTES --json
agent-vmm cleanup preview --environment-id ENV --kind discard --json
agent-vmm cleanup execute --environment-id ENV --kind discard \
  --plan-revision REVISION --request-id REQUEST \
  --confirm-volume VOLUME --confirm-irreversible --json
```

A single observation writes one JSON object. `operation follow` writes JSONL
for one existing operation. It polls no more often than every two seconds and
stops after 60 seconds. Restart follow with the same owner and operation ID.
The observation timeout does not cancel or extend the owner operation.

The envelope reports the exact target, fact sources, observation time,
freshness, state, stage, issue, original operation reference, and a finite
next action. It never returns a shell command as a next action. Inspect has a
five-second budget. Diagnose has a 30-second and 2 MiB budget. Log reads use at
most 100 records and 1 MiB. Logs redact recognized token, secret, password,
API-key, access-key, private-key, authorization, and cookie fields. They report
a gap when an opaque cursor is no longer available. Container log data is
hidden unless the caller passes `--include-container-output`; the same labeled
field and header redaction still applies.

`vm start` and `vm stop` are whole-VM actions. Invoking either command confirms
that the action affects all workloads in the VM, including workloads that are
not individually listed. The output contains a bounded Host-owned summary for
review. A partial or overflowing summary does not narrow the action scope.

The Host acquires the VM lifecycle owner before it stores a start or stop
receipt. Accepted work uses a separate two-minute context, so client
disconnection does not cancel it. Terminal state uses a separate five-second
write context. A typed admission timeout proves that the Host did not accept
the operation. An untyped transport error does not prove rejection; retry only
the same request ID. A terminal request replays its stored result. After the
lifecycle lock is available, an orphaned running receipt becomes interrupted
and requires VM inspection plus a new request ID.

`update` selects one exact HTTPS artifact by release ID, SHA-256, and byte
size. Its 20-minute budget includes a 10-minute download budget. The lifecycle
owner binds these values to the request before it records download or
preparation failure. A retryable failure has a 30-second cooldown. The same
request can make at most three attempts at one failed lifecycle stage. A target
conflict or exhausted stage retry budget requires administrator action.

The result includes one disposition: continue normal work, pause for an
exclusive transition, retry the same request at the reported time, request an
administrator action, or require a terminal operator decision. Download,
extraction, signature, or cancellation failures do not replace the installed
bundle. After replacement, retry the same request and exact artifact to repair
forward. Do not start an old binary against the updated state. A daemon update
requests the administrator installer before download.

`cleanup preview|execute` delegates to the existing Host cleanup owner. The
Guest rebuilds an in-memory owner index from containerd before VMMD starts. A
bounded cleanup lookup rejects 101 or more raw container or image records from
that index before it loads an exact record. Volume lookup uses a bounded
directory read. The Host reads for at most 30 seconds and admits at most 100
exact objects across the cleanup plan. Public list and diagnostic calls can
still request an unlimited inventory; this cleanup bound does not change those
contracts.

Execute sends the selected revision and confirmation set without a second
preview request. The Host is the only component that re-reads the plan. It
takes the exclusive inventory transition fence before this read. The read
checks the revision, references, container and volume generations, and exact
volume confirmation set. Container, volume, image, and network-link mutations
join the same fence. An image import holds its read side through the durable
import.

A new volume gets a random generation marker in a private sibling directory.
The workload mount does not expose this marker. Volume deletion removes the
marker, and namespace deletion removes the marker directory. A volume that
predates this layout gets a marker when the Guest owner first reads it. File
metadata changes do not change this generation.

Cleanup waits at most 30 seconds for the inventory, remote, and environment
locks. This deadline also includes the final plan read. The Host creates the
durable operation before it revokes a lease or session. Before a typed
admission timeout, the Host reads the request receipt again with a separate
five-second context. The typed timeout proves that the Host did not accept an
operation or start cleanup. Wait for the conflicting mutation, then request a
new plan. An untyped timeout, cancellation, or connection failure does not
prove rejection. Retry only the same request ID and plan revision.

An accepted cleanup uses a separate two-minute owner execution budget. Client
disconnection does not cancel this work. A retry can replay a running receipt
while the first request still holds an admission lock. After all three locks
are available, a remaining running receipt has no active executor. The Host
then saves an interrupted result. Request a new preview and request ID to
continue a failed or interrupted cleanup. A terminal request ID only replays
its stored result. The Agent reset and discard APIs also check this receipt
before they read a new plan.
If the Host cannot save this repair, retry the same request and restart the Host
service if the failure remains. Reclaimable bytes are unavailable when the
owner cannot supply them. Remote allocation cleanup uses the same admission
fence and budgets.

## Status

This project is new and has no production release.

The current implementation targets Apple Silicon and macOS 15 or later. Guest artifacts target Linux arm64.

## Interfaces

`agent-vmm-host` exposes one `0600` operator Unix socket and creates a separate `0600` Unix socket for each agent session. An agent cannot choose or replace its guest namespace in a request. The private guest protocol is reachable only by the host controller; no raw runtime relay is installed.

The SwiftUI `Agent VMM.app` is a development harness for observing and operating
the host API; it is not a production runtime dependency. Production packaging
uses the headless, product-neutral `Agent VMM Host.app`, whose main executable
is `Contents/MacOS/agent-vmm-host` and whose background-only bundle contains
only the host, managed CLI, data/lifecycle helpers, fixed service executor, and
verified Guest payload.

The managed bundle provides the `agent-vmm` CLI and the one-shot
`agent-vmm-service-executor`. The CLI delegates VM and cleanup changes to the
Host owner and bundle replacement to the lifecycle owner.
The administrator executor can only manage the fixed Host and BFT runner
system jobs, and both jobs run as the configured non-root service user.

## Design

Read [the architecture design](docs/architecture.md) for the security model, component boundaries, and design decisions.

## Verification

For final convergence, run the Go checks on macOS. During development, run the
smallest package or test set that covers the changed boundary first:

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

Run the complete macOS and guest test suite:

```sh
make e2e
```

Regenerate and validate the product protocols with `make proto` and `make proto-check`. Generate the development Xcode harness with `make swift-project`; create the headless Host App and launchd payload with `make headless-release`, then use `build/host/install.sh`. The same Host App supports `--service-type agent` for a logged-in user's GUI launchd domain and `--service-type daemon --service-user USER` for a system LaunchDaemon that runs as that non-root user. `auto` selects daemon when the installer has administrator access and otherwise selects agent only when the user's GUI domain exists. The Host executable embeds the canonical Guest manifest digest at link time. Real signing, notarization, and publication are release-owner steps and are not performed by local conformance checks. The uninstaller retains VM data unless invoked with `--purge`; build-identity transitions and pre-release cleanup are documented in [the recovery runbook](docs/recovery.md).

The E2E runner creates its report at `.build/test-results/e2e/report.md`.

## License

The agent-vmm source code uses the [MIT License](LICENSE).

Guest artifacts also contain third-party components. Their own licenses continue to apply.

The canonical Guest manifest records only the artifacts and filesystem format needed to start the VM. CI unpacks the built SquashFS and generates the Guest SPDX inventory from that output. The inventory check requires every installed APK to appear with a declared license; it does not apply a legal license allow/deny policy. Trivy separately blocks fixable high or critical dependency advisories. The SBOM is CI inventory, not runtime authority.
