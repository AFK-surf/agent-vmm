# Agent VMM Architecture

- Status: Accepted
- Date: 2026-07-29
- Scope: Apple Silicon with macOS 15 or later

## Decision

Agent VMM runs one persistent Linux virtual machine through Virtualization.framework.
Multiple agents share this virtual machine.

Each agent has an agent namespace inside the guest.
The agent namespace is the primary resource, identity, and policy boundary.

Each agent namespace maps to these resources:

- one containerd namespace
- one cgroup subtree and its resource limits
- one isolated network and IP range
- a set of containers, volumes, image references, and build caches
- a set of guest-owned named volumes
- a set of host-approved cross-namespace links and grants

The guest uses gVisor `runsc` as the only runtime for agent workloads.
The privileged `agent-vmmd` service controls containers, networks, volumes, builds, and grants.

Agent namespaces deny cross-namespace access by default.
The host controller can grant specific network, data, or control access.
A grant does not merge agent namespaces or expose a runtime socket.

Agent VMM does not use Docker Engine, Podman, or libpod as its resource model.
It does not use Firecracker.
It supports Dockerfiles through isolated BuildKit jobs.

## Scope

The system must let multiple agents do these tasks in one guest:

1. Change files and install tools in separate Linux workspaces.
2. Keep selected data after a container or virtual machine restart.
3. Start services that can discover approved peers.
4. Mount named volumes from the encrypted guest state disk.
5. Build OCI images from Dockerfiles.
6. Start containers and use exec or a TTY.
7. Grant specific access between agent namespaces.
8. Run untrusted code without control of the guest or the macOS host.

The first release excludes distributed scheduling inside Agent VMM, Kubernetes, live migration, GPUs, and arbitrary guest operating systems. A provider-neutral outbound connector may expose a user-approved capacity pool to one remote controller; scheduling remains controller-owned.
It also excludes full Docker API and Compose compatibility.

RFC16 extends that provider-neutral boundary without changing VM ownership.
`agent-vmm-host` remains the only local trust, runtime, and policy owner.
It owns the platform root-key provider, SQLite trust/mesh CAS state, local deny
ledger, and separate operator/remote-compute/connector-trust Unix listeners.
The connector authenticates to the trust listener with an install-scoped
capability and transports `remote.v1` plus host-signed registry/endpoint evidence. The connector-supervised
Rust child owns only an Iroh endpoint, the three fixed ALPNs, bounded streams,
explicit connect timeouts, drain, and traffic counters; its local probe proves
direct service-tunnel reachability without granting route authority. It is not
a product daemon or persistent fact owner.
The worker terminates Iroh and bridges service and mesh-control ALPNs to
separate host Unix sockets. It injects an install-scoped preface only after the
remote transport terminates; that secret never crosses Iroh. The connector
renews the signed observation and reconnects before expiry, then synchronizes
host-owned registry operations, snapshots, and the bounded endpoint projection.

## Security Facts

- Virtualization.framework isolates macOS from the Linux guest. It does not isolate agents inside one guest.
- A containerd namespace is an object scope. Access to the containerd socket bypasses this scope.
- Linux namespaces, cgroups, capabilities, and seccomp still share the guest kernel.
- Workspaces, services, and image builds can all run hostile code.
- Network, volume, and control access are separate capabilities.
- Persistent data and live process state are separate facts.
- A named volume is a capability from the host controller. A guest path cannot select host filesystem content.
- A Dockerfile defines a portable build input. BuildKit internal state is not a product contract.

## Compatibility Position

The project has no released API, persistent format, or legacy client.
It does not need a compatibility layer or a migration engine.

The target design starts with one shared virtual machine, one host controller, and one guest controller.
It does not use a temporary Docker or Podman backend.

## Design Convergence

### Use one shared virtual machine

A virtual machine for each agent conflicts with the shared-machine requirement.
The final design uses the agent namespace as the policy key.
The virtual machine remains the boundary between macOS and Linux.

### Remove direct runtime access

A workspace with a Docker or containerd socket has guest-root authority.
It can inspect, mount, or delete resources from another agent namespace.

Only `agent-vmmd` can access containerd, CNI, nftables, and BuildKit control interfaces.
Agents use high-level requests with a host-authenticated identity.

### Use explicit capabilities

One broad isolation switch cannot express safe collaboration.
The design uses `NetworkLink`, `VolumeGrant`, and `ControlGrant` as separate resources.

Each resource records its parties, scope, rights, expiry, and revocation state.

### Use containerd namespaces as resource scopes

Docker Engine has no resource namespace that maps to one agent.
Names and labels require authorization checks on every engine method.

Containerd applies a namespace through the request context.
`agent-vmmd` selects that context after host authorization.
An agent cannot provide or replace the context.

### Remove Podman and libpod

Podman adds its own storage, network, process, build, and database layers.
These layers do not remove any Agent VMM policy component.

A rootless Podman service for each agent also creates separate storage and lifecycle domains.
Cross-user collaboration then needs another privileged policy layer.

The final design uses the containerd client and the isolated BuildKit worker directly.

### Put gVisor on each untrusted execution boundary

An escape from a native container can reach every agent in the shared guest.
GVisor reduces direct access to the guest kernel syscall surface.

Each normal container has an independent runsc sandbox.
Infrastructure services run on the guest host and never accept an agent-controlled OCI specification.

The guest provides no native runc mode for agent workloads.
The only runc binary runs inside an outer BuildKit runsc sandbox.
This inner runtime is not an agent isolation boundary.

### Isolate every image build

Dockerfile `RUN` commands execute untrusted code.
A shared BuildKit daemon would mix build control, contexts, secrets, and caches.

`agent-vmmd` starts a short-lived outer runsc sandbox for each build.
The sandbox runs locked RootlessKit, BuildKit, and `buildkit-runc` components as UID 1000.

The sandbox mounts only the current context, temporary secrets, one cache, and one OCI output.
`agent-vmmd` verifies the output before import into the caller's containerd namespace.

## Components

### macOS host controller

`agent-vmm-host` is the sole VM owner and host control plane. It stores policy facts in one SQLite database, exposes a private operator Unix socket, and creates one revocable `0600` Unix socket per agent session. The session identity fixes the environment and guest namespace; request messages cannot replace either identity.

The SwiftUI app is an optional operator client. It consumes snapshots and bounded event, metric, and log streams. Its lifecycle is independent of the sidecar, VM, and agent sessions.

The host controller has these responsibilities:

- create, start, stop, and recover the shared virtual machine
- authenticate an agent session and assign its agent namespace
- store quotas, guest-owned volumes, desired resources, and grants
- authorize each agent request before a vsock call
- provide local TTY, file transfer, and port forwarding
- reconcile desired policy after a virtual machine restart

The host controller uses one local SQLite database for control-plane facts.
It does not copy container runtime state or volume contents into this database.

The same host will own the stable random device ID, platform-protected P-256
root, trust anchors, personal-mesh membership/tombstones, endpoint generations,
service exports/imports, and local route deny state. Root private keys never
enter SQLite or leave the platform key provider. Managed-controller and
personal-mesh authorities use the same `trust.v1`/`service.v1` verifier but have
separate credentials and membership state.

Environment ownership is explicit and persistent. A local environment has no registration or allocation identity. A remote environment belongs to one registration and one controller-scoped allocation ID; its host environment ID and guest namespace ID are generated locally and are never returned to the controller.

The operator socket and the remote-compute socket are separate `0600` Unix sockets. The remote-compute service exposes only remote inventory, allocation admission, lease lifecycle, cleanup plan/execute, connection state, and filtered runtime session creation. It cannot manage the VM or local environments. This is an application least-privilege boundary under the same macOS UID, not an OS sandbox.

The Virtualization.framework configuration contains these devices:

- one native Linux kernel and one immutable guest image
- one writable state disk
- no host-directory sharing device; workspace bytes live on the encrypted guest state disk
- one virtio-vsock device for control, byte streams, and the dedicated egress packet session
- entropy and memory balloon devices

### `agent-vmmd`

`agent-vmmd` is the only privileged guest orchestration endpoint.
It listens on a fixed vsock port and does not listen on TCP.

It controls these resources:

- containerd namespaces, containers, tasks, images, and snapshots
- cgroups, quotas, networks, and DNS views
- CNI, IPAM, nftables rules, and port forwarding
- named volumes and approved mounts
- BuildKit jobs, build caches, and OCI imports
- exec, PTY, resize, signal, log, and file streams
- idempotent policy reconciliation and runtime events

`agent-vmmd` does not implement an OCI runtime, image format, Dockerfile parser, or identity system.
It accepts requests with an explicit actor, target, and capability from the host.

### containerd and runsc

Containerd owns runtime facts for containers, tasks, image references, and snapshots.
`agent-vmmd` sets the containerd namespace from the authenticated agent identity.

Runsc is the only workload runtime handler.
It uses the systrap platform and one sandbox for each container.

Persistent workloads use the containerd native snapshotter and runsc `overlay2=none`.
This combination avoids nested overlay errors and temporary task-only writes.

A fixed supervisor runs as the sandbox init.
The user process runs as a retained exec process in the same sandbox.
This design avoids the shim wait race for a short-lived init process.

GVisor does not replace authorization.
A bad mount, network rule, or secret transfer can still disclose data.

Agent OCI specifications never contain these interfaces:

- the containerd socket
- runsc control directories
- host filesystem paths or host-directory sharing devices
- the vsock control device

### Agent namespace resources

Each agent namespace has these resources:

- an immutable namespace ID
- a cgroup root with CPU, memory, PID, I/O, and disk limits
- an independent bridge and subnet
- a policy-aware DNS view
- containers, volumes, image references, and build caches
- approved guest-volume grants
- approved incoming or outgoing grants

A workspace is a long-lived container.
Its writable layer can persist while the container exists.
Data that must survive container replacement belongs in a named volume.

Stateful services use Linux volumes on the encrypted state disk.

## Cross-Namespace Collaboration

### `NetworkLink`

A `NetworkLink` records these fields:

- source and destination agent namespaces
- one-way or two-way direction
- all containers or selected container IDs
- allowed protocols and ports
- an explicit DNS alias
- inclusion of future containers
- expiry and revocation state

Each agent namespace keeps its own bridge and subnet.
`agent-vmmd` does not merge bridges or attach a second interface to a live sandbox.

`agent-vmmd` compiles each active link into nftables rules for source and destination IP sets.
A namespace-wide link uses dynamic sets.
A partial link contains only the selected stable container IDs.

Policy DNS returns only local records and records visible through an active link.
It uses a host-approved qualified name or alias.

Revocation removes the DNS record and allow rule first.
It then clears related conntrack state so existing connections stop.

### `VolumeGrant`

Each volume has one owning agent namespace.
A `VolumeGrant` names the grantee, container, mount path, access mode, and expiry.

Knowledge of a volume name gives no access.
The host must approve cross-namespace read or write access.

Shared volumes need the applicable gVisor mount hint for inotify support.
A mount change on a live container requires controlled container replacement.

### `ControlGrant`

A `ControlGrant` permits selected inspect, exec, signal, restart, or log operations.
Its target can contain selected containers or a complete agent namespace.

The grant gives no network or volume access.
It never gives the grantee a runtime socket.

The host authorizes each operation and records the actor, target, and grant ID.
An agreement between agents is only an input to host authorization.

## State Ownership

| Fact | Authoritative source |
| --- | --- |
| Agent identity, namespace, quotas, and guest-volume grants | Host SQLite database |
| `NetworkLink`, `VolumeGrant`, and `ControlGrant` | Host SQLite database |
| Virtual machine configuration and disk identity | Host SQLite database |
| Guest image and state disk bytes | Files in the host environment directory |
| Containers, tasks, image references, and snapshots | The applicable containerd namespace |
| Volume ownership, quotas, and grants | Host SQLite database |
| Volume contents | One directory on the guest state disk |
| Build cache contents | One cache volume for each agent namespace |
| Network, address, DNS, and firewall state | Rebuildable `agent-vmmd` projections |
| Processes, TTY sessions, and active connections | Temporary runtime state |
| Device identity public metadata, trust anchors, mesh snapshots/tombstones, and route deny state | Host SQLite plus platform-protected root private key |
| Endpoint transport key and live Iroh streams | Connector-supervised data worker temporary state |
| Managed registration/grant/route intent | Remote controller; never reconstructed from connector state |
| Personal-mesh registry revision/freshness receipt | Registry projection verified against device-signed operations; never a local signing authority |

After a restart, `agent-vmmd` denies cross-namespace traffic before policy replay.
It rebuilds projections from active host grants.
It never derives authority from old nftables rules.

A Virtualization.framework memory snapshot can only serve as a disposable startup cache.
It is not an authoritative state source.

## Data Flow

```text
agent A/B harness
       |
       | authenticated local session
       v
macOS host controller ---- SQLite: namespace / link / grant
       |
       | authorized RPC and byte streams over virtio-vsock
       v
agent-vmmd
       +---- containerd namespace agent-A ---- runsc sandbox A1/A2/...
       +---- containerd namespace agent-B ---- runsc sandbox B1/B2/...
       +---- CNI / IPAM / nftables / policy DNS
       +---- namespace volume / BuildKit cache
       |
       +---- traffic allowed only by an active NetworkLink
```

An agent container cannot access a control socket in this diagram.

## Provider-neutral trust and service wire

`api/trust/v1` owns device identity, the two fixed authority classes, trust
anchors, managed membership credentials, personal mesh membership/snapshots,
endpoint observations, route capabilities, and the registry surface.
`api/service/v1` owns export/import/route/lease and `OpenServiceForward`.
Canonical signature inputs, fixed limits, error posture, and the three V1 ALPNs
are defined in [trust-service-protocol.md](trust-service-protocol.md).
The once-only managed enrollment response carries the controller anchor and a
device-root-bound membership credential. The connector cannot install either
without the install-scoped trust capability, and a managed service capability
is self-contained with its signed source membership credential.

The complete route class set is `LOCAL`, `DEVICE`, `COMPUTE`, `LAN`, and
`PUBLIC_HTTP`. Reachability and transport selection never grant authority.
Same-host optimization, direct Iroh, relay Iroh, and platform fallback all
validate the same source/destination/container/generation/capability fences.
Operator-created personal service listeners use the same destination-signed
capability: `DEVICE` binds only loopback, while `LAN` requires one explicit
private address currently owned by a local interface. Listener facts persist
for audit/idempotency but are revoked on host restart; a live socket is never
reconstructed without a new authorization exchange.
All control follows one direction from the harness through the host to `agent-vmmd`.
The operator read and restricted connector read each return at most 32 durable
local mesh snapshots. The connector uses its read only to synchronize signed
registry facts and endpoint projections. Create, revoke, and identity repair
remain separate operator mutations; the connector and local MCP cannot call them.

## Public Egress Boundary

`PullImage` and `PUBLIC_INTERNET` permit only public destinations.
Each DNS result and HTTP redirect requires a new address check.

The check rejects private, shared, documentation, loopback, link-local, metadata, guest-host, and agent-namespace addresses.
The current transparent proxy maps public targets to `198.18.0.0/15`.
This range is the only approved RFC 6890 exception.

The connection uses only the validated IP address.
TLS still uses the original hostname for SNI and certificate checks.

Product code ignores `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY`.
The guest keeps namespace bridge, policy DNS, nftables, and `NetworkLink`
ownership. Public IPv4 packets retain their namespace source address and
follow one guest TUN over a dedicated vsock session to the host controller.
One boot-scoped gVisor netstack terminates permitted TCP and UDP flows
through validated macOS sockets; the host never installs a route to
a guest namespace.

Public A and CNAME queries use the namespace gateway source and the fixed
TUN virtual DNS endpoint. The host system resolver supplies results,
and the host filters every returned address before sending it to the guest.
`DENY_ALL`, unknown or stale sources, private addresses, metadata, host
virtual addresses, and namespace ranges fail closed. The guest infrastructure
source is denied unless the host holds a short-lived lease for an authorized
operation such as image pull.

Sleep, wake, and host network changes may terminate existing flows. The guest
pump reconnects with bounded backoff, and new DNS and TCP connections must
recover within 30 seconds. Host-to-container traffic remains a separate authorized
`ForwardTCP` stream and never uses reverse egress routing.

The public E2E runner tests a fixed TLS service from the host and guest.
The target can use a public address or the approved transparent mapping.
An unavailable environment reports `environment_blocked` without changing the security boundary.

## Technology Decisions

### gVisor

GVisor protects the guest host and other agents from direct workload syscalls.
It supports ARM64, containerd, CNI, and the systrap platform.

The cost includes syscall, file-system, network, and performance differences from runc.
V0 accepts this cost and tests image compatibility.

An incompatible image returns a clear error.
The system never falls back to a guest-native runc.

### containerd

Containerd namespaces map directly to persistent agent resource scopes.
Runtime v2 supports the runsc shim for each container.

The content store can deduplicate blobs while image references remain namespace-specific.
`agent-vmmd` uses the Go client and does not parse CLI output.

Containerd does not provide product volumes, service DNS, or cross-namespace grants.
`agent-vmmd` implements this policy layer.

The design does not add Kubernetes or CRI orchestration.
Those systems would add pods, services, controllers, and another state store.

### Docker Engine

Docker Engine provides containers, networks, volumes, exec, and BuildKit.
It does not provide an agent resource namespace.

Name prefixes, labels, and authorization plugins make every Engine method a security review point.
A Docker socket also gives control of the complete guest.

The host CLI can use familiar command forms.
The product does not promise Docker API or Compose compatibility.

### Podman and libpod

Podman can use gVisor and can build Dockerfiles.
It still cannot provide the required multi-agent resource scope.

Podman 6 rejects the old nonempty libpod namespace configuration.
Older libpod namespaces cover only parts of the resource view.
They do not cover all images, volumes, networks, caches, and grants.

A Podman service cannot replace per-request authorization.
Placing it behind `agent-vmmd` adds another API and database without removing policy code.

Embedding libpod adds storage, image, process, network, and Buildah dependencies.
It still does not remove an Agent VMM component.

Podman pods also conflict with one independent runsc sandbox for each normal container.
Cross-agent traffic must use `NetworkLink` policy.

V0 does not install Podman, libpod, or Buildah.
A future move to a general local container engine requires a new product decision.

### Firecracker

Firecracker requires Linux KVM.
Running it inside the VZ guest requires nested virtualization and duplicates the virtual machine lifecycle.

It does not solve dynamic agent-namespace grants.
Hardware isolation between agents would conflict with the shared-machine requirement.

### Apple Containerization

Apple Containerization validates the use of Virtualization.framework, OCI, ext4 volumes, and gRPC over vsock.
Its stable design uses one virtual machine for each container and requires macOS 26.

Its multi-container `LinuxPod` API remains experimental.
It does not provide the required single-guest agent namespace model.

### Dockerfile and BuildKit

V0 supports Dockerfiles, Containerfiles, multi-stage targets, build arguments, secrets, caches, and OCI output.
It uses a locked Dockerfile frontend and a short-lived BuildKit daemon.

The outer runsc sandbox sets the resource boundary through its cgroup and project quota.
Its network has only loopback or filtered egress for the current agent namespace.

The build request accepts these inputs:

- an uploaded context or approved guest-volume snapshot
- a relative Dockerfile path
- a target and platform
- build arguments and secret handles
- a cache policy

The sandbox cannot mount the guest root, containerd socket, another namespace, devices, or broad guest directories.
It cannot request `security.insecure`, guest-host networking, or a cross-namespace cache.

BuildKit receives secrets through its secret interface.
Secrets do not enter build arguments, image layers, or the host database.
Cancellation stops the complete outer sandbox.

## Requirement Mapping

| Requirement | Permanent structure | Failure without it |
| --- | --- | --- |
| Multiple agents in one virtual machine | Shared Virtualization.framework virtual machine | One machine per agent breaks density and collaboration goals |
| Agent resource scope | Containerd namespace and host identity map | An agent can access another agent's resources |
| Execution isolation | One runsc sandbox per container | An escape reaches the shared guest kernel |
| Default network isolation | One subnet per namespace and deny rules | Agent namespaces can connect by default |
| Selected service access | `NetworkLink` and IP sets | Access becomes global or requires container replacement |
| Data collaboration | `VolumeGrant` | The system must copy data or expose all volumes |
| Controlled operations | `ControlGrant` | Collaboration requires a runtime socket |
| Persistent data | State disk and named volumes | Container replacement deletes data |
| Safe persistent data | Host-approved guest-volume grants on the encrypted state disk | A guest can select arbitrary macOS paths |
| Image builds | Isolated BuildKit and Dockerfile frontend | Existing environment definitions cannot run |
| Interactive access | Containerd exec and PTY over vsock | The guest needs an SSH service and keys |

## Structure Budget

V0 permits these permanent production components:

- one macOS host controller and one local SQLite database
- one shared Virtualization.framework virtual machine
- one versioned guest image
- one `agent-vmmd` service
- locked containerd, runsc shim, runsc, CNI, and nftables components
- one locked BuildKit image and Dockerfile frontend
- one host API and one versioned guest protocol
- `NetworkLink`, `VolumeGrant`, and `ControlGrant`

V0 does not permit these components:

- a distributed scheduler, worker leases, or a remote state database
- Docker, Podman, libpod, Buildah, Kubernetes, CRI, or Compose layers
- a custom OCI runtime, image format, Dockerfile parser, or general plugin registry
- agent-selected runc, host networking, privileged mode, devices, or arbitrary annotations
- arbitrary host paths, Firecracker, nested virtualization, checkpoints, migration, or SSH
- authorization based on names, labels, or path prefixes

## Delivery Milestones

### Runtime baseline

Verify containerd, runsc, CNI, storage, vsock, and resource limits in one VZ guest.
Test two isolated agent namespaces and concurrent sandboxes.

Reject host networking, privileged mode, devices, host paths, and arbitrary runtime annotations.
Verify state recovery and default network denial after a forced stop.

### Persistent workspaces

Create the host identity to agent namespace mapping.
Add lifecycle operations for cgroups, networks, volumes, workspaces, exec, PTY, logs, and files.

Test forged identities, concurrent deletion, quota exhaustion, restart reconciliation, and path escape attempts.

### Dynamic collaboration

Add policy DNS, the three grant types, nftables projections, and audit events.
Test link scope, direction, ports, future containers, revocation, and restart replay.

### Isolated image builds

Add short-lived BuildKit jobs, a locked frontend, namespace caches, progress, and cancellation.
Test multi-stage builds, cache reuse, secrets, limits, hostile Dockerfiles, and namespace isolation.

### Hardening

Sign and lock guest, runtime, and build artifacts.
Minimize the kernel and guest user space.

Fuzz RPC inputs, paths, OCI specifications, and grant state changes.
Add storage cleanup, cache cleanup, audit, and failure recovery tests.

Remove raw runtime socket forwarding, native runc fallbacks, stale compatibility paths, and duplicate state stores.

## Local MCP and remote compute boundaries

`agent-vmm-mcp` is the only model-facing entry point. It uses MCP over stdio,
binds one process to one LOCAL environment, and closes its short-lived operator
connection after obtaining a filtered runtime session. It has a fixed tool set;
there is no HTTP transport, operator tool, host path, or implicit shell.

`agent-vmm-connect` is the only controller-facing entry point. It makes an
outbound TLS 1.3 gRPC connection, reads the opaque controller credential from
Keychain, and connects locally only to the mode-0600 RemoteComputeService socket.
Each active lease receives a filtered AgentRuntimeService session over the single
bounded byte transport. The controller supplies an allocation ID, while the host
creates all local environment, namespace, lease, and capability identities.

Every control connection has a nonzero unpredictable epoch and a fresh ordered
command sequence. The initial full inventory carries an inventory watermark and
the complete required feature set. A command binds its ID, sequence, connection
epoch, deadline, target revision, and lease generation. The connector emits an
admission ACK, execution-started/finished evidence, and then a terminal result.
Loss after admission is an `unknown_outcome`; a controller reads authoritative
inventory and must not automatically replay the side effect. Only a completed
duplicate in the same epoch may reuse the connector's bounded result cache.

Controller integrations use `controller/remote.ControlSession` and
`controller/remote.AcceptSession`. These public helpers own conformance checks,
unknown-outcome classification, and the controller-side virtual `net.Conn`;
tests and products must not carry private copies of the stream decoder.

The OperatorService owns registration and pool policy, local enable/drain/revoke,
inventory observation, and destructive cleanup. RemoteComputeService is scoped to
the active registration and cannot see LOCAL environments. This same-UID split is
an application least-privilege boundary, not a separate macOS security principal.

Release retains allocation data. Destruction requires a fresh plan and an execute
request matching its revision and volume set. Disconnect starts a bounded grace
period; expiry or revocation closes the capability and stops remote-owned work.
Host startup converts every persisted `LEASED` allocation to fail-closed
`RETAINED` and advances its policy revision before the RemoteComputeService can
admit a replacement connector. In-memory leases are never reconstructed.
The connector does not implement Salix device/group/task/router concepts: an
external controller adapter maps those concepts to the public provider-neutral
registration/allocation/lease protocol.

## Workspace and service projections

Workspace RPCs are bounded guest operations rooted at
`/state/workspaces/<namespace>`. The guest opens every component with
`openat2(RESOLVE_BENEATH|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_SYMLINKS)`, rejects
links and special archive entries, caps files/archives at 256 MiB, and limits
concurrent transfers. Interrupted uploads retain a request-scoped staging file;
resume requires the exact offset and prefix SHA-256 before the final atomic
rename. No host path or VirtioFS mount participates in this contract.

Service exports persist an exact destination container instance, port, protocol,
owner, and revision. Imports persist the host-injected source environment and
container instance plus a virtual DNS label. For same-guest LOCAL/COMPUTE
optimization the host projects only that label, exact source/destination
containers, port, and expiry into the guest; the guest removes the nftables/DNS
projection at expiry and container replacement removes the address projection.
Restart reconciliation recreates only ACTIVE routes whose route and lease
revision/generation still match.

`OpenServiceForward` is available only on the capability-authenticated connector
socket. It re-reads the import, export, route, lease, device identity, both
current container instances, and the byte budget before opening the destination
stream. Transport selection never changes those facts. The public
`controller/service.MemoryTransport` is the in-memory conformance transport for
LOCAL/DEVICE/COMPUTE/LAN tests and the future data-worker adapter; it owns no
authority or persistent route state.

## Known Limits and Review Triggers

- GVisor does not implement every Linux syscall, file-system operation, or ioctl.
- An incompatible workload gets an error instead of a weaker runtime.
- GVisor reads interface data when a sandbox starts. Dynamic links use guest routing and firewall rules.
- Shared-volume inotify needs a gVisor mount hint. A mount change requires container replacement.
- Native runc inside an agent namespace invalidates the shared-guest isolation claim.
- Stable multi-namespace support in Apple Containerization can trigger a review of the custom guest layer.
- Full Compose support needs a separate permission model and test budget.
- x86_64 workloads need separate Linux Rosetta and binfmt tests.

## References

- [Apple Virtualization.framework](https://developer.apple.com/documentation/virtualization)
- [Apple virtio-vsock](https://developer.apple.com/documentation/virtualization/vzvirtiosocketdeviceconfiguration)
- [Apple Containerization](https://github.com/apple/containerization)
- [containerd namespace API](https://pkg.go.dev/github.com/containerd/containerd/v2/pkg/namespaces)
- [containerd namespace context](https://github.com/containerd/containerd/blob/main/docs/getting-started.md)
- [containerd runtime v2](https://github.com/containerd/containerd/blob/main/docs/PLUGINS.md)
- [gVisor security model](https://gvisor.dev/docs/architecture_guide/security/)
- [gVisor architecture](https://gvisor.dev/docs/architecture_guide/intro/)
- [gVisor platforms](https://gvisor.dev/docs/architecture_guide/platforms/)
- [gVisor containerd configuration](https://gvisor.dev/docs/user_guide/containerd/configuration/)
- [gVisor CNI](https://gvisor.dev/docs/tutorials/cni/)
- [BuildKit](https://github.com/moby/buildkit)
- [Dockerfile frontend](https://docs.docker.com/build/buildkit/frontend/)
- [Podman architecture](https://github.com/podman-container-tools/podman#oci-projects-plans)
- [Podman 4 libpod namespaces](https://docs.podman.io/en/v4.0.0/markdown/podman.1.html#namespace-namespace)
- [Podman 6 namespace removal](https://github.com/podman-container-tools/podman/blob/v6.0.2/libpod/runtime.go#L452-L454)
- [Podman service security](https://docs.podman.io/en/latest/markdown/podman-system-service.1.html#security)
- [Firecracker design](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md)
- [Firecracker KVM requirements](https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md)
