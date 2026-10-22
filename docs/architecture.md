# Agent VMM Architecture

- Status: Accepted
- Date: 2026-07-29
- Scope: Apple Silicon with macOS 15 or later, and Linux arm64 with KVM as an experimental Host

## Decision

Agent VMM runs one persistent Linux virtual machine through Virtualization.framework.
Multiple agents share this virtual machine.

Each agent has an agent namespace inside the guest.
The agent namespace is the primary resource, identity, and policy boundary.

Each agent namespace maps to these resources:

- one VMMD-owned logical resource record; all workload runtime objects use one internal containerd namespace
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

### Controller lifecycle and resource direction

Owner direction, 2026-09-19. These requirements supersede the design prescriptions
below where they conflict. The current implementation has not yet converged.

- Controller loss, upgrade, or an older version must not by itself stop workloads.
  Fence stale commands and capabilities at their authority boundary.
  Keep explicit revocation, user stop, and actual VM shutdown separate.
- System appliances share a measured, bounded resource budget.
  Do not reserve CPU or memory per appliance or choose a fixed percentage split.
  Do not impose a workload or appliance count as a substitute for bounded operations.
  Retain real disk, PID, address-space, and request-size constraints.
- Appliance install, restart, and maintenance must not require sibling readiness
  or stop the shared VM merely to attach that appliance's storage.
  Preserve persistent data and isolation when changing the storage layout.
- File-descriptor checks protect one operation against path replacement.
  They do not define permanent workload, controller, or volume identity.
  Preserve stale-target deletion protection without deriving volume identity from ctime.

The Guest stores appliance data under `/state/appliance-data/<installation>/<volume>` on the pool filesystem.
Each directory has an independent project quota bounded by `data_volume_bytes`.
These rules do not authorize data deletion or an authorization bypass.

### Supported operations

The system must let multiple agents do these tasks in one guest:

1. Change files and install tools in separate Linux workspaces.
2. Keep selected data after a container or virtual machine restart.
3. Start services that can discover approved peers.
4. Mount named volumes from the guest state disk.
5. Build OCI images from Dockerfiles.
6. Start containers and use exec or a TTY.
7. Grant specific access between agent namespaces.
8. Run untrusted code without control of the guest or the macOS host.

The first release excludes distributed scheduling inside Agent VMM, Kubernetes, live migration, GPUs, and arbitrary guest operating systems. Each controller domain has an exact private or external registration and a user-approved capacity pool; multiple domains may be active while scheduling remains controller-owned.
It also excludes full Docker API and Compose compatibility.

The provider-neutral transport extension does not change VM ownership.
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

- The code-signed Host executable embeds the expected Guest manifest digest. VM start verifies that manifest and the path and size of each boot artifact before Virtualization.framework receives their paths. The bundle signature covers artifact content, so VM start does not hash the artifacts again. Guest health reports runtime observations; it does not echo Host-provided artifact identity as evidence.
- The Guest build resolves the current upstream dependencies. CI tests and scans
  that resolved set before publication. The generated SBOM records what entered
  the bundle; it is inventory for dependency and license consumers, not a
  runtime trust root. Moving upstream dependencies can fail the build, tests, or
  scans before publication. The published Guest bundle identity, not an input
  lock, selects the release artifact.
- OCI manifest and blob digests bind image content. The control plane supplies an immutable HTTPS archive URL plus its expected size and SHA-256. The target Host downloads those bytes directly, and the Guest validates the exact Host-to-Guest transfer and idempotency record before activating the image. The URL transport is not a second content authority.
- The packaged release inventory is diagnostic metadata. Lifecycle admission uses the fixed executable, helper, Guest, plist, and launchd-template layout rather than trusting fields declared by that inventory.
- Virtualization.framework isolates macOS from the Linux guest. It does not isolate agents inside one guest.
- A containerd namespace is an object scope. Access to the containerd socket bypasses this scope.
- Linux namespaces, cgroups, capabilities, and seccomp still share the guest kernel.
- Workspaces, services, and image builds can all run hostile code.
- Network, volume, and control access are separate capabilities.
- Persistent data and live process state are separate facts.
- A named volume is a capability from the host controller. A guest path cannot select host filesystem content.
- A Dockerfile defines a portable build input. BuildKit internal state is not a product contract.

## Compatibility Position

The Host uses the current metadata schema.
An older database with appliance rows fails closed before metadata conversion.
An older database without appliance rows can update metadata.
Startup preserves diagnostic disks and keys and performs no appliance-data conversion or deletion.
The signed Host provides an offline conversion from the legacy single-filesystem
state disk to the control and workload layout. That command attaches the source
read-only and writes a separate target. The first target boot converts legacy
namespace quota labels and removes retired receipt labels. It rebuilds disposable
system container root filesystems with the overlayfs snapshotter. The conversion
preserves system private volumes and all workload container state.

The target design starts with one shared virtual machine, one host controller, and one guest controller.
It does not use a temporary Docker or Podman backend.

## Runtime ownership

### Shared virtual machine

The shared virtual machine is the boundary between macOS and Linux.
Each agent namespace defines the scope for resource authorization and policy.

### Privileged runtime control

A workspace with a Docker or containerd socket has guest-root authority.
It can inspect, mount, or delete resources from another agent namespace.

Only `agent-vmmd` can access containerd, CNI, nftables, and BuildKit control interfaces.
Agents use high-level requests with a host-authenticated identity.

### Explicit capabilities

`NetworkLink`, `VolumeGrant`, and `ControlGrant` authorize network, volume, and control access separately.

Each resource records its parties, scope, rights, expiry, and revocation state.

### Resource authorization

Containerd namespaces are management scopes. Host-authenticated ownership checks enforce agent resource isolation.
Workloads have no access to the containerd socket.

`agent-vmmd` uses one private containerd namespace, keys physical image records by manifest digest, and
generates opaque internal container and snapshot keys. Every runtime object carries its logical owner;
list, get, start, exec, stop, and delete verify that owner before returning or mutating it. Per-agent
logical namespace records retain tag-to-digest image authorization,
resource contracts, cgroup, network, volume, and build-cache ownership without duplicating read-only
root files.

`agent-vmmd` calls containerd and the isolated BuildKit worker directly.
It owns authorization and lifecycle across both interfaces.

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
The sandbox runs the CI-tested RootlessKit, BuildKit, and `buildkit-runc`
components as UID 1000.
Nested BuildKit uses overlayfs snapshots on the controller-scoped, VMM-owned
ext4-backed preparation cache. The cache survives allocation release and can be
collected only after its accepted builders settle. Each build starts a fresh worker.
Its sandbox capabilities are SETUID, SETGID, and SETPCAP for RootlessKit user mapping.
Overlay copy-up and whiteouts use the existing gofer filesystem interface.
The readonly base and nodev cache mounts remain enforced.
The builder image contains all fixed mount destinations, including the authorized
base image’s `/base-image` directory, before its read-only snapshot is mounted.
The build leaf cgroup shares CPU and memory with its workload namespace, with
`cpu.max`, `memory.max`, and `memory.high` set to `max`. Its PID limit and
namespace parent cgroup remain enforced.
Namespace deletion first settles internal build tasks, then removes that namespace’s
internal builder image with synchronous GC. Build cleanup does not remove image
records from another namespace.
Build sandbox creation uses a private non-debug runsc error log. On creation
failure, the Guest reads the first error-level fatal message before cleanup and
retains up to 2048 bytes in the creation error. Debug logging and spec dumps stay
disabled. The private startup files are removed with the build workspace.
Build stdout and stderr flow through the caller’s authorized BuildImage progress
stream in small frames, with backpressure. The Guest retains six bytes to match
BuildKit cache markers across writes. It drains containerd stdout and stderr
before reporting the exit code, within the existing cleanup deadline. Output
frames use lossy UTF-8 conversion. Repository output does not enter the shared
Guest log.

The sandbox mounts only the current context, temporary secrets, one cache, and one OCI output.
`agent-vmmd` verifies the output, records the caller's logical image grant, and materializes its rootfs in
the internal runtime namespace.

## Components

### macOS host controller

`agent-vmm-host` is the sole VM owner and host control plane. It stores policy facts in one SQLite database, exposes a private operator Unix socket, and creates one revocable `0600` Unix socket per agent session. The session identity fixes the environment and guest namespace; request messages cannot replace either identity.

The operator can sign a dedicated Comma recovery challenge with the existing device root and exact local registration.
Enrollment health does not gate identity proof. Failed enrollment remains recoverable without creating a new registration.
Revoked or locally disposed registrations cannot sign. The challenge binds the account, Session, Workspace, installation and nonce.
Cloud authorization decides whether to consume the proof. Signing changes neither enrollment nor command authority.

The SwiftUI app is an optional operator client. It consumes snapshots and bounded event, metric, and log streams. Its lifecycle is independent of the sidecar, VM, and agent sessions.

The host controller has these responsibilities:

- create, start, stop, and recover the shared virtual machine
- authenticate an agent session and assign its agent namespace
- store quotas, guest-owned volumes, desired resources, and grants
- authorize each agent request before a vsock call
- provide local TTY, file transfer, and port forwarding
- reconcile desired policy after a virtual machine restart

Without `--cpus` or `--memory`, the Host derives the VM size from the Mac. The
VM gets three quarters of the cores (at least 2) and half of the physical memory
(2 GiB to 32 GiB). An explicit flag overrides each value. Remote capacity
reports the resulting values. The Host refuses a configuration that leaves less
than 4 GiB of physical memory to macOS. The VM has a memory balloon device, but
the Host does not change its target. If Virtualization.framework
unexpectedly stops a running VM, the Host first revokes sessions, capabilities,
and remote connector admission while preserving the
durable registration, allocation, and state-disk identity. The same Host
process owns one recovery attempt against the existing state disk. It publishes
remote admission after a fresh Guest boot ID, policy replay, workload recovery,
and connector reconnection. Appliance reconciliation continues independently.
One failed appliance does not close VM admission or stop a sibling appliance.
A failed VM attempt or a second unexpected stop requires operator action. It
does not replace the disk or restart macOS.

The host controller uses one local SQLite database for control-plane facts.
It does not copy container runtime state or volume contents into this database.

The same host will own the stable random device ID, service-user-protected P-256
root, trust anchors, personal-mesh membership/tombstones, endpoint generations,
service exports/imports, and local route deny state. Root private keys never
enter SQLite or leave the Host key provider. Managed-controller and
personal-mesh authorities use the same `trust.v1`/`service.v1` verifier but have
separate credentials and membership state.

Environment ownership is explicit and persistent. A local environment has no registration or allocation identity. A remote environment belongs to one registration and one controller-scoped allocation ID; its host environment ID and guest namespace ID are generated locally and are never returned to the controller.

The operator socket and the remote-compute socket are separate `0600` Unix sockets. The remote-compute service exposes remote inventory, allocation admission, frontend lease management, cleanup plan/execute, connection state, and allocation-scoped runtime session creation.

Runtime handlers authorize resources through the session namespace and execution
identity. Remote runtime sessions can observe and cancel their exact image
operations. Environment reset and discard belong to the controller's typed
lifecycle API. OperatorService is not registered on runtime sessions. Runtime sessions use the existing concurrent-stream, message-size and Guest resource bounds; serial workspace reads do not consume a calls-per-second budget.

Container create and start operations retain accepted request identities. Successful Guest evidence completes their receipts. Only a typed `admission=not_accepted` rejection stores a failed receipt with the original RPC status. Replay returns that receipt. Uncertain mutations require exact observation or allocation cleanup. Cached container creation uses the existing unpacked image and its namespace quota.

The Host computes an internal configuration fingerprint from the complete `CreateContainerRequest` with `request_id` cleared. The Guest stores this value in its container label. `GetContainer` can include an expected creation declaration; the Host compares both values with its own protobuf encoder and returns optional `configuration_matches`. It does not store the expected declaration or the comparison. List, mutation receipts, and queries without an expected declaration leave the comparison unset. An explicit Install can detect environment, resource, mount, and launch changes with the same image, or recognize a completed Create after a lost response. A missing internal fingerprint compares false. The comparison does not authorize a request or restrict upgrades.

An integration commit stores its revision change and original operation receipt in one transaction. Replaying the request returns that original receipt. Local sessions bind to the accepted integration revision; callers can require an exact revision when opening a session. Mutation admission checks that binding under the integration lock. A new intent cancels sessions of the previous revision and waits for their admitted Host mutation handlers to finish before committing. Guest operations retain their existing cancellation, instance, and receipt boundaries. Long Exec and ForwardTCP streams do not hold the integration lock for their full lifetime. Connection verification does not advance the operator intent revision and cannot verify a newer intent.

Operator Stop can carry the exact integration ID and revision. The Host saves the false running intent before connecting to the Guest, including a component that has not started yet. It does not start the VM or require a runtime session for this command. If the Guest cannot confirm the stop, `SERVICE_STOP_PENDING` reports the saved intent and unknown actual state as a failed RPC. A later intent fences replay of the old Stop.

A new runtime session for an allocation generation replaces its current session. The connector closes the old tunnel and confirms exact capability revocation before it opens the replacement. Revocation failure blocks replacement. Session replacement does not stop the workload.

Closing or revoking an already ended session succeeds and preserves its first terminal reason and timestamp. Cleanup still closes any remaining listener and tunnel. Both closed and revoked sessions reject authorization; an unknown session ID remains an error.

A rejected allocation projection includes the saved `RemoteAllocation` as a typed gRPC status detail.
The connector retains it in the rejected command result with the original capacity dimension.
A capacity rejection without that detail occurs before allocation admission. Transport uncertainty does not prove non-admission.

Update PREPARING retains the source installation state, grant, boot, and instance.
Explicit import-slot or storage-headroom rejection waits under the original operation and deadline.
Other failures retain their failed receipt. They do not withdraw the source controller during preparation.
A single transaction advances the installation revision, records DRAINING, and disables source admission after preparation succeeds.
Stop, revoke, and remove interrupt the original update in their existing mutation transaction.
Post-preparation phases use the existing installation lifecycle lock. Activation retains its existing instance and revision checks.

The remote-compute service cannot manage the VM or local environments. This is an application least-privilege boundary under the same macOS UID, not an OS sandbox.

The Virtualization.framework configuration contains these devices:

- one native Linux kernel and one immutable guest image
- one writable state disk
- one VirtioFS device configured at boot for approved static service directories
- workspace bytes and owned volumes on the guest state disk
- one virtio-vsock device for control, byte streams, and the dedicated egress packet session
- entropy and memory balloon devices

Appliance installations use directories on the shared state disk and add no block devices.

### `agent-vmmd`

`agent-vmmd` is the only privileged guest orchestration endpoint.
It listens on a fixed vsock port and does not listen on TCP.
The Host owns each pending VMMD connection until it transfers the connection to its caller.
If cancellation wins, the Host closes any connection that VZ returns later.

It controls these resources:

- logical agent namespaces plus the private containerd runtime namespace, containers, tasks, images, and snapshots
- cgroups, quotas, networks, and DNS views
- CNI, IPAM, nftables rules, and port forwarding
- named volumes and approved mounts
- BuildKit jobs, build caches, and OCI imports
- exec, PTY, resize, signal, log, and file streams
- idempotent policy reconciliation and runtime events

`agent-vmmd` does not implement an OCI runtime, image format, Dockerfile parser, or identity system.
It accepts requests with an explicit actor, target, and capability from the host.

### containerd and runsc

Containerd owns runtime facts for containers, tasks, physical image materialization, and snapshots in one
private namespace. Per-agent image records are authorization and retention grants. `agent-vmmd` maps the
authenticated agent identity to opaque internal keys and verifies owner labels on every operation.

Runsc is the only workload runtime handler.
It uses the systrap platform and one sandbox for each container.

Persistent workloads use the containerd overlayfs snapshotter and runsc `overlay2=none`. Each image digest
has one immutable committed lower tree; every container gets its own writable upper/work directory and
runsc sandbox. Containerd owns the only persistent overlay layer.

The state disk has two filesystems. The 8 GiB control filesystem
stores containerd metadata, VMMD state, and system private volumes. The 56 GiB workload filesystem stores OCI content, overlay
snapshots, workload volumes, logs, and build temporary files. Fixed mount paths
keep containerd metadata available when the workload pool is full. A workload
project quota includes its overlay upper and work directories and its private
volumes. Shared immutable OCI content counts against the workload filesystem,
but it does not count against every workload quota.

The system and workload parent cgroups share Guest CPU and memory. They use CPU
weight 100 and no CPU or memory quota. The Host does not reserve compute capacity
for an installation. System appliance grants retain PID and disk limits.
System appliance grants share one 4 GiB Guest control-system budget.
The existing short disk-budget lock serializes every namespace grant.
Admission adds the requested quota to the actual usage of each other system namespace. It does not add their unused quota. Each quota still bounds its own namespace.
A rejection reports the requested and previous quota, total commitment, budget, and each existing namespace's quota and usage.
These commitments belong to the control filesystem, independently of workload-pool import headroom.
The other 4 GiB remains available for trusted metadata and recovery work. It also absorbs growth of other system namespaces after an admission.
The internal appliance-build namespace declares no writable control quota. Its control directories contain trusted namespace metadata and use that metadata reserve.
Build context, output, and reusable cache use the existing workload-pool builder growth accounting.
Successful appliance removal cleans up stopped payloads, image references, and private data before deleting its system namespace and clearing its quota.
Failed cleanup retains the namespace commitment for retry. Stop and Revoke retain data and its quota.
Per-environment
grants use the workload pool ceiling: configured state disk bytes minus the
8 GiB control partition and the existing 4 GiB emergency and 1 GiB metadata
reserves. A 64 GiB state disk has a nominal 51 GiB workload quota ceiling.
Quota grants do not preallocate files. Guest admission counts their unused hard-quota commitments as promised physical capacity, then checks filesystem usage, overhead, and import headroom before growth. Namespace growth rejection logs the requested delta, current quota, remaining commitments, available capacity, reserves, and combined import and builder credit from that admission snapshot. Guest RPC maps the storage owner's `ErrCapacityUnavailable` identity to `ResourceExhausted` with `storage_headroom` detail. It preserves the runtime stage and original cause. The same error text from another owner does not prove capacity exhaustion.
Cold OCI import and other materializations reserve all remaining uncommitted bytes after the emergency and metadata reserves. They require at least 2 GiB. OCI manifests declare only compressed sizes, so the reservation does not assume a compression ratio.
A cold import first reserves the available byte budget. It collects discardable caches only when the required reservation cannot fit.
After collection it recalculates usage and commitments before admission. Percentage pressure is diagnostic and does not trigger cache deletion.
Actual filesystem inode exhaustion still fails the write. No fixed percentage predicts the inode count an image requires.
System, builder, and workload images use the shared overlayfs snapshotter.
Immutable layers count against pool project 9000. Writable workload snapshots
retain their namespace quotas. System views mount existing layers read-only.
Creating a view uses the pool metadata allowance, not a cold-import reservation.
The Guest logs pool capacity, available bytes, and shared image project usage.
The Host projects the durable installation's active image and unfinished update target through private `ReconcileSystemImages` and system Import. Guest namespace labels retain those roots across VMMD restart. The installation revision rejects late older projections before GC. The existing Host reconciliation also updates stopped or missing Guest records, so cancellation does not require a later import to release pending roots.
Containerd metadata stays on the control filesystem so pool pressure does not
consume its recovery space.
Permanent appliance Remove releases its exact installation namespace's stopped
containers and image retention roots. Synchronous GC preserves images referenced
by other namespaces. Stop and Revoke retain their separate lifecycle contracts.

Host removal deletes the installation's incoming frontend approvals and outgoing
service imports, including revoked approvals, with their routes and leases. This
cleanup and the final removal receipt commit together. A pending-destroy operation
resumes under its original request after an interrupted commit.

The lifecycle owner serializes shared-VM startup and target data cleanup. Remove
leaves that VM running so other services retain their runtime and caches; it does
not own a whole-VM shutdown. Removal admission releases its installation lock
before waiting for the lifecycle owner. Cleanup takes the owner before the
installation lock and reads the original pending operation again.
Host construction classifies interrupted commands from local receipts. The
existing appliance reconciliation loop retries pending removal effects with a
bounded context for each target. Each attempt uses the existing VM execution
budget, shortened by a future original operation deadline. Background cleanup
checks the VM state after it takes the lifecycle owner and runs only while the
VM runs. Only an explicit Remove can start the VM for cleanup. An expired deadline
does not discard an accepted pending deletion. A failed cleanup retains its
pending receipt, records its error, and leaves the Operator available for
inspection and retry. An accepted ExecuteRemove returns its operation when cleanup
must wait. The operation remains RUNNING/PENDING_DESTROY and records the cleanup
error. The CLI watches that operation. Store transition failures still return errors. An ordinary VM Start does not repeat command recovery or consume a pending Remove;
the original Remove request remains the explicit retry path.

Normal Guest shutdown closes admission to filesystem operations, cancels active streams, and waits for their handlers and cleanup.
It freezes all appliance records before waiting for an in-flight restart, including records in startup or restart backoff.
Build and Copy use their existing writer cancellation and settlement records.
Archive-only appliance builds share the in-memory run lifecycle without a persistent image receipt or retention root.
Successful cleanup removes that run. Failed cleanup keeps its exact workspace cleanup action available for the next shutdown attempt.
The Guest then stops system and ordinary workload tasks and waits for task monitors to close logs and process handles.
Ordinary container configurations and Host desired-running intent remain available for the next VM start.
Unresolved writer teardown prevents host preparation and filesystem unmount. The same prepare request can retry cleanup.
Startup cleanup has a separate responsibility and preserves running workloads after a control-daemon restart.

Before sending ExecuteRemove, the operator CLI saves its confirmed command and
original timeout under `~/Library/Application Support/AgentVMM/appliance-requests/`,
indexed by the canonical operator socket and request ID. Retries reuse the saved
revision and deadline and require the same confirmation and timeout. The command
remains after success so a repeated invocation can observe the same receipt.
This file holds no runtime state. Its consumer is the same command's retry:
after a lost response, the public API cannot recover its original deadline by
request ID.

System containers use read-only snapshot views. They do not copy the image
root or change its project quota. System images must contain `/run/agent-vmm`
and `/var/lib/agent-vmm` directories and regular `/etc/resolv.conf` and `/.agent-vmm-supervisor` files.
Each parent component must be a directory, not a symbolic link. The Guest
checks these mountpoints before task start and reports `system_mountpoints`
if a target is missing or has the wrong type. It does not create targets in
the shared image. The trusted supervisor has a read-only,
executable file mount at `/.agent-vmm-supervisor`. The runtime
and data mounts remain non-executable.

Before a system image import reserves space, the Guest removes confirmed
stopped containers and unused old image records from that installation's
namespace. It retains the selected image and every image still referenced by
a container. The shared image collector checks references from all namespaces
before it deletes physical content and snapshots. A successful Stop removes
the exact stopped container and its view. It retains appliance data and the
selected image record.

The remaining control capacity covers containerd metadata, VMMD state, and
filesystem overhead. A full workload filesystem cannot consume this control
reserve.

Each appliance data directory has mode `0700` and owner UID/GID 1000.
The appliance receives only its projected directory, without a block device or Host capability.
Its project quota is independent of the workload namespace quota.
`data_volume_bytes` is a hard limit, not reserved capacity. The shared pool accounts
for actual allocated bytes and can run out of space before a directory reaches its limit.
Stop and restart remove runtime projections and preserve persistent data and its quota.
They do not unmount the backing filesystem.

Normal appliance Stop keeps the installation's managed trust and private registration.
A failed publication, such as a bootstrap that fails while an adopted instance starts, fences only that instance. It stops the instance's frontends, private controller route, and egress, and keeps managed trust and its service imports and routes. Those routes stay bound to the boot, container instance, and configuration revision fences. Only explicit Revoke, removal, or disabled reconciliation retires the authority.
A Host restart preserves pending private registration preparation.
Private activation can resume pending or incomplete preparation under the same installation, registration, and revision fences.
External enrollment interrupted before credential completion still requires its operator retry.
Activation can reuse managed trust and private registration with a new boot and container fence. Disabled reconciliation
rechecks the installation fence and checks the durable revocation operation before it
revokes authority. Explicit Revoke still retires the private controller domain and
revokes its managed trust. A later stop does not cancel revocation. Activation cannot restore that authority.

Replacement waits until the exact old writer has stopped.
Typed deletion rejects active references and removes only the exact installation and volume.

The ordinary `state.raw` file and its directories do not encrypt themselves.
Offline device confidentiality requires FileVault or equivalent full-disk encryption on the actual backing disk.
A logical raw-file copy contains plaintext. Encrypt backups independently.
Host identity, bootstrap authority, and device trust retain their separate cryptographic controls.
`SEALED` records integrity failures unrelated to data encryption.
A sealed installation cannot start or restart. Stop and revoke preserve its sealed state.
Removal requires the current revision and an approved impact plan.

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
- a cgroup root with a PID limit and shared CPU and memory
- a project quota for writable disk usage
- an independent bridge and subnet
- a policy-aware DNS view
- containers, volumes, logical image grants, and build caches
- approved guest-volume grants
- approved incoming or outgoing grants

VMMD creates sibling `system` and `workload` cgroups under `/agent-vmm` before it accepts requests.
System appliance namespaces use `/agent-vmm/system/<namespace>`.
Ordinary namespaces use `/agent-vmm/workload/<namespace>`, including their build sandboxes.
Container and build processes inherit the namespace boundary.
Usage reads and namespace cleanup use the same paths.

The workload and system parents can use the Guest memory ceiling. Each parent
retains a 2,048 PID limit.
Equal CPU weights apply between workload namespaces. Containers within a namespace share its allocation.
Ordinary namespaces and containers have unlimited `cpu.max`, `memory.max`, and
`memory.high`, with CPU weight 100. PID and writable disk limits remain.
System appliance namespaces also share CPU and memory. Their PID and disk limits
remain. The Host does not cap the total stored workload count. CPU and memory
samples do not prevent starts. The same workload still serializes lifecycle
commands. Conflicting start, stop, or quiesce returns `LIFECYCLE_CONFLICT`.

Every five seconds, the Host samples parent memory and up to 16 active
instances, with four concurrent RPC workers and a five-second total deadline.
A stable cursor advances past failed samples. Coverage time grows with the
number of active containers, which can exceed the environment count.
The current reclaim candidate receives priority. Unknown or stale samples
cannot authorize reclaim.

Three samples at 90% parent memory or 10% full PSI enter pressure.
A parent-local OOM increment enters pressure immediately. Child OOM events do
not trigger parent reclaim. Three samples below 80% memory and 1% full PSI
leave pressure. Memory includes file and shmem charges. The Guest logs their
breakdown at high memory usage and never subtracts them from charged memory.

Under pressure, the Host selects one oldest idle instance, idle for at least
60 seconds. Only its registration receives the allocation and instance identity.
Demand, instance changes, unavailable samples, or a 30-second deadline cancel
the candidate. Expiry prevents replacement in the same sample round.
The controller must settle its business obligations before quiesce and stop.
A positive minimum idle period requires a current reclaim candidate. Zero-idle
quiesce remains available for bootstrap repair and other explicit lifecycle stops.
No eligible idle instance means no voluntary reclaim. Kernel OOM remains possible.

Guest request IDs are nonempty opaque business keys. They have no resource-name
character or length constraint.
Namespace, container, and volume names have separate resource validation.
The Guest retains the original request ID and checks the complete input
fingerprint for replay.
A repeated key with different input fails.
The Guest keeps at most 1024 completed request identities in memory for five minutes. It caches only successful results.
Failed calls retain only their fingerprint. The same input can run again, including after cancellation.
A Guest restart clears this cache.

Exec replay uses the original execution record while that record remains retained.
Only the first admitted execution creates an independent containerd process ID.
Stream input, signals, and resize target that process handle.
Host execution authority binds the original execution ID and exact container instance.

The Host keeps at most 64 execution activities for an active sandbox. Each
activity binds an execution ID, trusted owner, operation kind, allocation
authority, and exact container instance. Main execution activities have no
deadline. Short operations keep the owner deadline. An expired short operation
becomes unknown and continues to block quiesce until its owner settles it.

An acquire of the same activity is idempotent. A release settles only the exact
activity on the exact target. It cannot change another activity or a replacement
instance. `ListExecutions` returns one atomic view with explicit recovering,
stale, stopped, empty, and present states. Host restart recovery stops running
instances before clearing their activities. For a stopped container, Guest retains
its successful Start fingerprint, exact instance, and start time in container metadata.
Host rebuilds bounded stopped-instance evidence from these fields when current instance
is empty. Created containers and missing Start evidence do not prove an instance stopped.
A replacement never supplies evidence for the old instance. Transport reconnect preserves activities.
Safe sleep first closes new execution admission with a quiesce command. The
matching instance and command
must then be present on the stop request. Cancellation can reopen admission only
for that same quiesce command before a stop starts. Repeating the cancellation
when no quiesce is current is a successful no-op. A different command or a stop
in progress remains fenced. The quiesce request can require a bounded minimum
idle duration. The Host rejects the request until the active instance has
remained idle for that exact duration.

A workspace is a long-lived container.
Its writable layer can persist while the container exists.
Data that must survive container replacement belongs in a named volume.

Stateful databases and SQLite WAL use owned volumes on the Guest state disk.

### Approved Host directories

Operator `CreateHostShare`, `RebindHostShare`, `RevokeHostShare`, and `ListHostShares`
manage directory grants for local service environments. Controller-domain allocations cannot use them.
A grant trusts the service environment, including its sessions and container replacements.
A container ID or image hash does not narrow this trust boundary.

`VolumeMount.source` selects `owned_volume` or `host_share`. A runtime session receives only the grant ID.
Operator responses include the source path for diagnostics. The Host records its filesystem UUID, directory file ID,
access mode, revision, and state in `host_shares`. The existing operation stores its request identity and receipt.
Accepted share operations retain their candidate source identity and access mode across Host restart until they settle.
Recovery and replays use that frozen candidate under the current share operation ID and revision.
A lost operation handler does not revoke that authority.
An old successful request cannot restore a later revoked grant.
An explicit operator command with the current revision can supersede an unresolved rebind.
The same transaction interrupts the old operation and fences its completion.

Host shares are static VM configuration. New Create, Rebind, and Revoke commands require the VM to be stopped with no attached machine. A successful original receipt can replay while the VM runs. The inventory transition lock serializes mutations with all VM start paths. Original pending commands can settle when the VM is stopped or failed without an attachment; this does not authorize a new command in a failed VM.

Before each boot, the Host checks canonical paths, filesystem UUIDs, directory file IDs, and volume properties. Sources must be on internal local volumes that are neither removable nor ejectable. External, ejectable, and network volumes are not supported. A missing or changed source remains unavailable and does not prevent unrelated workloads from starting. The Host builds the complete directory map with upstream `vz.NewSharedDirectory` and sets it before VM Start. Guest `ApplyHostShares` observes the configured private directories and publishes only the complete valid environment-scoped authority array.

The upstream API uses paths. The operator must not move or replace configured sources while the VM runs; this is not an FD-bound projection or a guarantee against every path replacement race. An RW grant permits changes to the authorized tree, including changes made through another share of its parent. The Host does not claim additional isolation between those paths.

Source loss or loss of environment approval rejects new mounts, stops affected consumers, and withdraws Guest authority.
A failed stop remains unresolved. The VZ device mapping stays attached until VM shutdown.
Restoring a path does not restore its writer during that boot.
Stop the VM, repair or explicitly rebind the source, then start the VM.

Environment removal withdraws Guest authority and closes sessions and listeners while the VM runs.
Static grants retain REVOKING until the VM stops and their physical configuration can be removed.
The original pending operation continues Guest deletion after the operator starts the VM.
Environments without active shares retain their normal removal path.
Removing a share never deletes its source contents or applies owned-volume quota claims.

A missing Guest directory makes only that share unavailable.
Guest projection observes directories once per reconcile call and reports applied share IDs.
Background reconciliation retries unavailable projections.

Create and Start validate the grant and access mode. The Guest binds only the approved private source. The container label and request identity include the typed mount source and access mode. Existing owned-volume labels convert once during the normal inventory rebuild.

The Host does not recursively change directory ownership or permissions. Numeric container users can receive permission errors. On the tested platform, Host 501:20 directories appeared as Guest 0:0. UID 1000 could not use a 0700 directory. It could read and write a test-only 0777 directory. Read-only projection still rejected writes. These results do not guarantee write access for every image user, filesystem, or ACL configuration. Host share contents are outside controller backups.


Provider-native credential stores use the same provider-neutral volume
primitive. The product controller allocates and retains one opaque volume ID,
creates it with the workload image's numeric UID/GID and mode `0700`, and mounts
it read-write at the image's declared workspace. Agent VMM does not receive an
OAuth token, API key, device code, provider name, workload generation, or
runtime connection epoch for this operation. The provider process writes its
native credential store inside the container, so the host observes only the
volume ID, ownership, mode, and mount policy. Container replacement and VM
restart preserve the volume; another namespace cannot name it and another UID
cannot read it. Workload-generation and connection-epoch fencing remain the
controller/runtime-carrier authority and do not create a second Agent VMM
volume lifecycle.

Volume creation accepts permission bits and the sticky bit (`01777` maximum).
It rejects setuid, setgid, and other mode bits. Guest applies numeric ownership
before the final directory mode. A root-owned sticky job directory lets the job
UID create files while protecting root-owned registration files from deletion.
Create receipts include the requested mode in their input fingerprint. Replaying
a settled create returns its original result without changing directory permissions.

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
| Container/task records and shared immutable image/snapshot materialization | The private containerd runtime namespace |
| Per-agent image authorization and retention | The logical agent namespace record |
| Volume ownership, quotas, and grants | Host SQLite database |
| Volume contents | One directory on the guest state disk |
| Build cache contents | VMM-owned preparation directory scoped to the controller registration, or to the namespace for operator builds |
| Network, address, DNS, and firewall state | Rebuildable `agent-vmmd` projections |
| Processes, TTY sessions, and active connections | Temporary runtime state |
| Device identity public metadata, trust anchors, mesh snapshots/tombstones, and route deny state | Host SQLite plus service-user-protected root private key |
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
       +---- private containerd runtime namespace
       |       +---- shared immutable image lower snapshots
       |       +---- owner-fenced upper A1/A2 ---- runsc sandbox A1/A2/...
       |       +---- owner-fenced upper B1/B2 ---- runsc sandbox B1/B2/...
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

Operator-approved appliance imports reuse the service import and route owners.
The import freezes an installation and an exact destination export. Its active
route binds the current source grant revision, boot, and container instance.
Stop withdraws the route and preserves the intent. Activate and same-product
Update validate the same export and project the new source instance. Revoke
and Remove terminate the intent. A revoked import cannot be restored by a new
grant. An export keeps the approved environment, container ID, and port.
Container replacement updates its observed generation and instance without changing that authorization.

VM network recovery observes ordinary source and destination containers before restoring service projections.
Imports and exports follow their approved container IDs across generation and instance changes.
The Host closes old projections before it publishes current instances.
A missing container leaves the intent unavailable. Recovery does not create a replacement container. Appliance source projections are restored by the appliance lifecycle
owner after the current boot and running instance are committed. Transport
observation failures remain recovery errors.

Service DNS resolves `<alias>.services.agent` within the caller's approved
imports. The alias does not grant access: Guest projection enforcement checks
both exact container instances and the destination port. Gitea uses
`http://gitea.services.agent:3000/` for its root and clone URLs.

The complete route class set is `LOCAL`, `DEVICE`, `COMPUTE`, `LAN`, and
`PUBLIC_HTTP`. Reachability and transport selection never grant authority.
Same-host optimization, direct Iroh, relay Iroh, and platform-specific transport all
validate the same source/destination/container/generation/capability fences.
### Personal mesh listeners

Operator-created personal service listeners use the same destination-signed
capability: `DEVICE` binds only loopback, while `LAN` requires one explicit
private address currently owned by a local interface. Listener facts persist
for audit/idempotency but are revoked on host restart; a live socket is never
reconstructed without a new authorization exchange.

### Local service intent and listeners

The Host stores service running intent by environment and container ID, independently of exports and ports.
Only operator-approved integration environments use this supervisor. Controllers manage their own allocation containers.
Start and Stop set running intent. Recovery starts only existing containers with the recorded generation.
Removal clears running intent. A missing container requires explicit Create before it can run again.
Agent and Operator container deletion clear Host running intent and service projections only after the Guest confirms absence.
An old successful Delete replay preserves a replacement container. An unavailable observation preserves Host facts for retry.
Stop retries preserve the original request ID and timeout, including an omitted timeout.
Guest mutations use the same retry rule: failures can run again with the original request ID and complete fingerprint.
Successful results replay from the in-memory cache. Each Delete retry checks the namespace and container generation.
Container Get and List return optional `desired_running` from this running intent separately from Guest state.
Unmanaged containers omit that field. A missing container still returns NotFound.

Operator-created local service listeners target an environment, container ID, and port.
Loopback is the default. LAN exposure requires one explicit private address on a current local interface.
The Host persists the selected bind address and port and restores that endpoint after restart.
Generation or instance replacement closes old connections and publishes the current target.
Stop closes the endpoint while preserving its address. Listener revocation and owner removal terminate the publication intent.
Create, same-request replay, and List return current `available` and `unavailable_reason` observations.
A missing approved interface or failed socket bind makes only that listener unavailable.
These observations do not fail completed container mutations or unrelated appliance publication.
These fields describe the bound socket, not application readiness, and are never stored in authorization or mutation receipts.
List retries each local publication independently. An occupied port, absent interface, or stopped target reports unavailable;
a failed publication does not hide other rows. Once the original address is available and the target is running,
the Host restores the same listener identity and port. It does not select a different port or interface.
The CLI can exit after creation. Forwarding goes directly to Guest TCP and has no mesh capability expiry.
Local service listeners have no total byte or connection limit. Backpressure does not bound aggregate concurrent resource use.
All control follows one direction from the harness through the host to `agent-vmmd`.
The operator read and restricted connector read each return at most 32 durable
local mesh snapshots. The connector uses its read only to synchronize signed
registry facts and endpoint projections. Create, revoke, and identity repair
remain separate operator mutations; the connector and local MCP cannot call them.

## Public Egress Boundary

`PullImage`, runtime OCI archive downloads, and `PUBLIC_INTERNET` permit only public destinations.
For an image absent from the namespace, Host `PullImage` downloads the linux/arm64 image anonymously and commits it through the same immutable `ImportImage` path, with a request ID derived from the pull request. The download uses Host infrastructure egress, not the allocation egress mode, and ignores proxy settings. It is bounded by the 8 GiB import limit, a 30-second read idle timeout, and the 10-minute import budget. Its temporary archive is removed after the import.
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

Logical namespace records map to persistent agent resource scopes. Workload
containers, tasks, and physical image records live in one private containerd
namespace; runtime v2 supports the runsc shim for each container.

Physical image records are keyed by manifest digest, while logical tag-to-digest
grants remain namespace-specific.
`agent-vmmd` uses the Go client and does not parse CLI output.

An image is available only when its logical grant, exact content graph, and
shared overlay lower snapshot are materialized. Image listing does not project
a grant whose physical rootfs is missing, and container creation rejects it
before creating a container record. Re-importing the same digest-bound OCI
archive repairs that materialization in place; a different digest or import
fingerprint remains a conflict and cannot replace the logical grant.

Before containerd starts, VMMD establishes project 9000 and directory
inheritance on the pool containerd and import roots. New CAS content and immutable
overlay layers inherit that project. Startup does not traverse layer payloads.
Containerd GC removes entries without retention references.
System image retirement preserves retained environments and BuildKit caches.
Disk-pressure GC can reclaim these caches after their accepted writers settle.
Appliance content staging checks the published archive and its receipt before
acquiring an import slot. A complete cache hit needs no writer capacity.
Missing or incomplete archives use the import slot and disk reservation, with
the cache checked again after admission.
The pool project quota opens for admitted image materialization and then returns
to its metadata allowance. The import archive owner restores inheritance when
startup cleanup has removed the import root.
Cached workload creation does not take the cold import slot or change its quota allowance.
Overlay snapshot preparation writes directory metadata before the writable project assignment.
The namespace lock preserves the source grant through snapshot creation.
Cached image copy also bypasses the cold import slot.
Copy records its exact source digest before it releases the source namespace lock.
Unsettled copy receipts retain that shared image until the target grant exists or the writer settles.
Retained-cache GC and shared-image GC use these existing receipt references.
Cold materialization still uses the import slot and physical growth reservation.
A request waits for the import slot in FIFO order until its deadline. Only an
expired deadline returns `import_slot` capacity exhaustion. Background GC and
retention work do not wait; they skip the round when the slot is busy.
The writable-snapshot owner assigns the workload project to both overlay upper
and work directories before mounting. Copy-up can then move temporary inodes
into upper without crossing the project boundary. Startup never recursively
rewrites OCI content or changes that assignment.
Quota operations pin a path without following symlinks and accept only regular
files and directories. Image links and special inodes are not opened for quota
inspection. Startup metadata allowance, stale-task recovery, the existing-state
GC barrier, and subsequent idle-quota tightening retain their existing order.

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
It uses the Dockerfile frontend from the resolved BuildKit image and a
short-lived BuildKit daemon.

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
| Agent resource scope | Logical namespace records, owner-labelled objects in one private runtime namespace, and the host identity map | An agent can access another agent's resources |
| Execution isolation | One runsc sandbox per container | An escape reaches the shared guest kernel |
| Default network isolation | One subnet per namespace and deny rules | Agent namespaces can connect by default |
| Selected service access | `NetworkLink` and IP sets | Access becomes global or requires container replacement |
| Data collaboration | `VolumeGrant` | The system must copy data or expose all volumes |
| Controlled operations | `ControlGrant` | Collaboration requires a runtime socket |
| Persistent data | State disk and named volumes | Container replacement deletes data |
| Safe persistent data | Host-approved guest-volume grants on the state disk | A guest can select arbitrary macOS paths |
| Image builds | Isolated BuildKit and Dockerfile frontend | Existing environment definitions cannot run |
| Interactive access | Containerd exec and PTY over vsock | The guest needs an SSH service and keys |

## Structure Budget

V0 permits these permanent production components:

- one macOS host controller and one local SQLite database
- one shared Virtualization.framework virtual machine
- one versioned guest image
- one `agent-vmmd` service
- CI-tested containerd, runsc shim, runsc, CNI, and nftables components
- one CI-tested BuildKit image and Dockerfile frontend
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

Add short-lived BuildKit jobs, a tested frontend, namespace caches, progress,
and cancellation.
Test multi-stage builds, cache reuse, secrets, limits, hostile Dockerfiles, and namespace isolation.

### Hardening

Publish content-addressed guest, runtime, and build artifacts.
Minimize the kernel and guest user space.

Fuzz RPC inputs, paths, OCI specifications, and grant state changes.
Add storage cleanup, cache cleanup, audit, and failure recovery tests.

The implementation has no raw runtime socket forwarding, native runc path, or duplicate state store.

## Runtime and standalone distribution

The Cue/BFT Host bundle uses `build/guest/build.sh OUTPUT release runtime`.
The Guest contains the VM runtime, supervisor, and BuildKit.
`build/guest/build.sh OUTPUT [PROFILE] [FLAVOR]` is the common build entry point.
It requires Docker with Buildx. It exports the Linux arm64 bundle from the
multi-stage Dockerfile into `OUTPUT`. The default profile is `development`.
The default flavor is `standalone`.

The builder runs on Linux arm64 with a case-sensitive filesystem.
The dependency stage resolves current Alpine stable packages and runtime releases.
It downloads and verifies each APK before installation, then installs packages without network access.
APK downloads have a 15-minute total budget and a six-minute limit per package,
with at most two retries. Required packages remain explicit APK world entries.
Transitive dependencies do not become world entries.

BuildKit cache mounts retain Go modules, Go compilation results, and downloaded artifacts within the builder.
Guest builds reuse Docker layers and cache mounts by default.
Set `AGENT_VMM_REFRESH_DEPENDENCIES=1` when dependency refresh is required.
This pulls the current base images and rebuilds the tools and dependency stages;
the existing download and Go cache mounts remain available.
The positional output directory, profile, and flavor arguments are unchanged.
A persistent local builder can reuse these mounts after successful or failed builds.
CI uses the [Blacksmith persistent builder](https://docs.blacksmith.sh/blacksmith-caching/docker-builds) with the same Guest cache key across both workflows.
Its persistent disk contains layers and cache mounts. Blacksmith saves the disk only after the complete job succeeds.
The action can fall back to a local builder. A fallback build has no cache from previous jobs and still builds normally.
Standard layer cache exports do not contain cache-mount data.
The runner's host Go cache does not supply the Docker build's Go cache.

The Host package adds the managed CLI, lifecycle helper, data worker, and fixed
service executor. The managed CLI exposes Host observation, maintenance, VM,
cleanup, and Appliance lifecycle commands.
Host packaging retains the Rust compilation cache at `target/host-rust`, or the
operator's `CARGO_TARGET_DIR`, independently of the package output directory.
The package contains no MCP executable, OCI archive, grant, or bootstrap seed.
Lifecycle status uses `GetSystemHealthSummary`, a constant-fan-out read with
one Host observation and at most two Guest health calls. It does not load the
inventory. Operator VM mutations require whole-VM acknowledgement and the current VM state and boot identity.
Workload observation revisions do not constrain that whole-VM authorization.
`GetVMImpactSummary` provides a bounded diagnostic projection. Rich UI and diagnostic observation continues
to use `GetSystemSnapshot`.
An update reads `GetVM` before graceful stop and again before force stop. A replacement boot rejects force stop.
The installer waits for the stopped VM, Host process exit, and state-disk release before replacing the bundle.
When launchd has no Host PID and the state disk has no open owner, update proceeds without a VM stop request.
The default `standalone` Guest build includes the optional MCP appliance.
The macOS standalone app installs that appliance through `ApplianceAdminService`.

External-controller enrollment and Appliance installation are separate operator workflows.
An external controller uses registration health for readiness.
Appliance readiness requires a healthy installed instance.
On a Host with enabled appliances, the bounded background reconciler restores missing
instances from their installed artifact after the VM starts. An unfinished Update
owns its installation and excludes automatic recovery. Verified bundled upgrade
material is staged; the explicit durable Update operation freezes new writers,
settles existing executions, stops the exact controller, switches artifact and
grant, and validates target bootstrap and health. A runtime bundle omits bundled upgrade material,
so it does not install or upgrade the standalone MCP appliance.

### Linux Host release

The Linux Host has no app bundle. One `agent-vmm` executable contains the Host,
the managed CLI, the VMM child, and the lifecycle (`agent-vmm lifecycle`).
`build/host-linux/package.sh` makes a tar with `agent-vmm`, `agent-vmm-data`,
the Guest payload, and `release.json`. `release.json` lists the size and SHA-256
of each file. Its SHA-256 is the content ID.

The lifecycle uses the same operation journal, managed enrollment handoff, and
coordinator as macOS. Only a small service adapter is different:

| Concern | macOS | Linux |
| --- | --- | --- |
| Release layout | `Agent VMM Host.app` | `/opt/agent-vmm/<content-id>` and the `current` link |
| State | `~/Library/Application Support/AgentVMM` | `/var/lib/agent-vmm` |
| Service | launchd job | `agent-vmm-host.service`, user `agent-vmm` in group `kvm` |
| Version replacement | atomic exchange of the app | atomic rename of the `current` link |
| Mutation user | non-root service user | root |

The unit uses `KillMode=process`, so a Host restart does not stop the VMM child.
`Restart=on-failure` restarts a failed Host. A stop sends `SIGTERM` to the Host,
and the Host stops the VM first. An update stops the VM and the unit. It copies
the release with hash checks, switches `current`, and starts the unit and the
VM. It never changes `/var/lib/agent-vmm`.

## Local MCP and remote compute boundaries

The bundled MCP appliance is the only model-facing entry point. It serves
Streamable HTTP on a host-loopback frontend and owns the MCP schema, opaque
client bindings, allocations, runtime sessions, result bounds, and
cleanup policy. The host app only publishes the current endpoint and never
exposes an OperatorService or AgentRuntime credential to a client.

Controller-owned dynamic loopback frontends are a separate public contract for
long-lived raw TCP forwarding. A ready controller can acquire, renew, or release
one only for its exact accepted allocation authority. The host owns the random loopback
listener, consumes a 256-bit capability preface, and relays bounded bytes over a
typed controller stream; the appliance alone maps the immutable assignment to a
container instance and `ForwardTCP` target. These listeners are memory-only and
close immediately on connection-epoch replacement, allocation/session
revoke, controller disconnect, appliance stop, VM shutdown, or generation change.
They do not use fixed frontend endpoint files and the host never receives a
container ID or target port.

Controller-facing management runs inside the appliance over its authenticated
controller channel. The host keeps only the local lifecycle and transport
supervision needed to publish that channel; no separate connector or stdio proxy
is installed.

Every control connection has a nonzero unpredictable epoch and a fresh ordered
command sequence. The initial bounded inventory carries an inventory watermark
and the required feature set. A command binds its ID, sequence, connection
epoch, deadline, and target revision. The connector emits an
admission ACK, execution-started/finished evidence, and then a terminal result.
Receiving the exact next sequence consumes that issued slot even when deadline
or operation-fence validation returns a terminal rejection; stale epochs and
sequence gaps do not consume a slot. This maps to `AgentVMMCommandSequence.tla`,
where `ClaimForDelivery` advances the controller-owned cursor independently of the
Host admission outcome.
`controller/remote.ControlSession` validates the durable command's exact epoch
and next sequence and sends them unchanged; it does not allocate a second wire
identity after the controller has claimed the command.
The connector reports `REJECTED` for local refusals before dispatch and recognized Host `ErrorInfo` reasons.
VM admission closure and registration drain report `ERROR_REASON_ADMISSION_PAUSED`.
Deadline, cancellation, transport loss, and unrecognized Host errors report `UNKNOWN`.
An allocation projection rejection includes the exact allocation that the Host already accepted.
The controller must clean that allocation before it treats the attempt as settled.
A same-identity Ensure with unchanged parameters reuses the allocation before a new pool policy revision check.
Loss after admission is an `unknown_outcome`. A controller must not automatically replay an external side effect. Only a completed
duplicate in the same epoch may reuse the connector's bounded result cache.

PrepareAgentIntegration uses the caller's ResourceLimits when it creates a local
environment. It returns an existing environment without changing its policy.
Policy changes use UpdateEnvironmentPolicy with the current revision.

An operator can commit a new configuration fingerprint for a ready integration with its current revision. This atomic update retains the integration owner and environment. Removed or revoked integrations cannot be reactivated by a configuration commit.

Controller integrations use `controller/remote.ControlSession` and
`controller/remote.AcceptSession`. These public helpers own conformance checks,
unknown-outcome classification, and the controller-side virtual `net.Conn`;
tests and products must not carry private copies of the stream decoder.

The OperatorService owns registration and pool policy, local enable/drain/revoke,
inventory observation, and destructive cleanup. Each RemoteComputeService transport
is scoped to its exact registration and cannot see local or other-domain environments. This same-UID split is
an application least-privilege boundary, not a separate macOS security principal.

The remote observation stream contains bounded health, active-workload usage,
and an inventory watermark. Usage is the sum of fresh cached samples for the
registration's active namespaces. A missing or stale sample makes the complete
usage result unavailable. The stream does not publish stored-workload disk use
or the Guest disk budget. Guest disk accounting remains local admission
authority and runs only when an operation needs it. A capacity rejection
returns a typed error with safe fields; it does not create a periodic management
projection.

Release retains allocation data. Destruction requires a fresh plan and an execute
request matching its revision and volume set. Remote cleanup keeps planning read-only.
Both ordinary and missing-namespace plan revisions include the exact namespace ID.
A replacement allocation receives a new namespace, so an old plan cannot authorize its cleanup.
After durable acceptance, the Host closes allocation capabilities, confirms workload stop and enters `RETAINED` before deletion.
Reset preserves named volumes and the retained allocation. Discard removes the confirmed resources and then the Host allocation.
A missing container, volume directory, or image does not prove that its namespace
is absent. A discard plan can remove retained Host policy without Guest resource
cleanup only after `GetNamespace` confirms that exact namespace is absent.
An unavailable namespace query preserves the Host owner.
Release also confirms the exact namespace with `GetNamespace` when container
inventory returns `NotFound`. An absent namespace has no workloads to stop;
the existing allocation owner, generation, and revision checks still apply.
Retirement failure prevents resource deletion and produces a failed receipt.
A control request expiry does not
remove an accepted allocation or prevent runtime-session recovery. Runtime
sessions and loopback frontends keep their own finite capability periods.
Explicit release or revocation closes the affected capabilities. A connection
replacement fences the old transport without stopping accepted work.
Automatic empty-allocation retirement stops after 30 seconds. The allocation
then remains `RETAINED` with its durable identity and current revision. A
controller can send `ReleaseAllocation` again with that revision to start a new
30-second attempt window. The Host does not retry an expired window in the
background.
Host startup converts a legacy `LEASED` allocation to `READY`. The durable
allocation generation remains the control fence. The current connection remains
the command fence.
The connector does not implement Salix device/group/task/router concepts: an
external controller adapter maps those concepts to the public provider-neutral
registration/allocation/session protocol.

## Workspace and service projections

Workspace RPCs are bounded guest operations rooted at
`/state/workspaces/<namespace>`. The guest opens every component with
`openat2(RESOLVE_BENEATH|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_SYMLINKS)`, rejects
links and special archive entries, and limits concurrent transfers.
Reads, digest queries, and archives retain their 256 MiB limit.
WriteWorkspace limits each chunk to 64 KiB. The namespace disk quota bounds the stored bytes.
The Guest computes the write receipt digest during transfer. Interrupted uploads retain a request-scoped staging file;
resume requires the exact offset and prefix SHA-256 before the final atomic
rename. The upload staging filename encodes the complete request ID with
SHA-256, so retries use the same file.
WriteWorkspace creates missing parent directories beneath the selected root without following links.
New directories inherit the root owner. Existing directories retain their ownership.
Write failures include the volume, path, request ID, phase, and original error in Guest logs.
Extraction and OCI import use independent temporary names.
Appliance builds encode the request ID with SHA-256 for their internal OCI tag.
These physical names do not replace the original business key or restrict
its contents.
No host path or VirtioFS mount participates in this contract.

Service exports persist an exact destination container instance, port, protocol,
owner, and revision. Imports persist the host-injected source environment and
container instance plus a virtual DNS label. For same-guest LOCAL/COMPUTE
optimization the host projects only that label, exact source/destination
containers, port, and expiry into the guest; the guest removes the nftables/DNS
projection at expiry and container replacement removes the address projection.
Restart reconciliation recreates only ACTIVE routes whose route and lease
revision/generation still match.

Guest namespace DNS logs listener start, close, and unexpected receive or accept exits.
Request failures identify the UDP or TCP read, answer, and send phase with namespace, gateway, peer, and original error.
A receive or accept loop exit removes only its exact listener instance and closes both sockets.
Network reconciliation rebuilds missing listeners for namespaces with retained endpoints.
Old workers cannot remove a replacement listener. This fixes a reproduced listener failure, but does not identify the historical runner timeout.

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


### Installation LAN access

An appliance manifest can request `requested_lan_egress` and a static `LAN_HTTP` frontend.
The operator approves each capability in `ApprovedGrant` during installation.
A request does not authorize access.
The Host stores the LAN approval with the installation grant revision.
Ordinary allocations retain their separate `DENY_ALL` or `PUBLIC_INTERNET` policy.
They do not inherit appliance LAN access.

LAN egress permits IPv4 RFC 1918 destinations.
The Host rejects unapproved Host interface addresses, the namespace pool `10.64.0.0/10`, and current namespace subnets.
Loopback, link-local, metadata, multicast, and IPv6 destinations remain blocked.
The Host applies this policy to TCP, UDP, and virtual DNS answers.
A redirect must open another connection, which receives the same destination check.
The public policy continues to reject private destinations.
An installation that needs public access must also receive the public egress grant.

Host interface addresses require a separate operator-approved `host_tcp_endpoints` grant.
The grant contains exact IPv4 addresses and TCP ports for one installation.
The Host publishes source policy after it persists the exact controller boot and container instance, before it admits the private controller route. Restart recovery uses the same publication path. Stopped and replaced instances lose their source authority.
Environment and session reconciliation publish the complete source set with an atomic policy replacement.
Flows retain authorization when their source facts remain equal, including after another allocation appears.
Source removal, revocation, instance replacement, or boot replacement cancels the affected flow.
The Host logs the cancellation reason and each TCP direction's transferred bytes and original error.
The CLI accepts `--host-tcp-endpoints IP:PORT,IP:PORT` during install or update.
These endpoints require a Host-reachable IPv4 listener. Loopback addresses remain blocked.
An update preserves the current grant unless the operator supplies `--approve-grant-changes`.
An explicit endpoint parameter changes only that list. An empty endpoint parameter clears the list.
The CLI shows the package request, current grant, and proposed grant before update.
The Host rejects a retained grant that exceeds the new package permission limits.
The operator must explicitly adjust the grant before that update can proceed.
These addresses belong to installation configuration and do not enter the product manifest.
The Host binds access to the current appliance instance and grant revision.
Workload allocations do not inherit these endpoints.

The Host checks infrastructure exclusions before endpoint grants.
Appliance frontend listeners, dynamic Host loopback frontends, and personal service listeners remain blocked.
Other namespaces, transport, loopback, and metadata remain blocked.
A wildcard frontend listener blocks its port on every Host interface.
DNS can return an approved Host address. Each TCP connection must match an approved port.
UDP does not receive TCP endpoint access. Redirects and DNS changes require another destination check.
The grant controls Layer 4 access. It does not restrict HTTP paths.
Revocation cancels existing installation flows through the source policy fence.
Unrelated installations and workload flows remain active.

A `LAN_HTTP` frontend requires HTTP transport and explicit approval for the same manifest frontend.
The Host selects an active private IPv4 interface and a TCP port.
The manifest cannot select an interface or an address.
The Host returns the actual endpoint through appliance status.
The Host preserves the selected port when it can bind that port after restart.
The frontend does not promise public reachability or TLS termination.
The appliance must restrict HTTP routes and authenticate webhook requests.

System streams use a fixed Bootstrap, Controller, or Frontend purpose.
Bootstrap requires a positive total byte limit.
Controller rejects a lifetime byte limit and does not count accumulated connection traffic.
Controller retains frame, concurrency, idle, and synchronous backpressure limits.
OpenSystemStream preserves caller deadlines and cancellation but adds no fixed connection lifetime.
Runtime operations retain their own execution deadlines; a long-running Build does not expire the controller transport.
The Guest uses one sender per stream and permits one in-flight frame without a queued frame buffer.
The declared idle timeout bounds socket I/O and blocked gRPC sends.
After output half-close, successful input writes refresh the bounded wait for input closure.
The RPC deadline and cancellation also end these waits and release the endpoint connection.
Each OCI import retains its independent archive size, chunk size, hash, and deadline checks.

Static LAN and loopback frontends use the same bounded system stream.
The Host enforces frame size, concurrent streams, idle timeout, and connection deadline.
A static frontend with `max_total_bytes=0` has no cumulative byte limit.
Nonzero totals bound each direction. An operator cannot approve an unlimited total for a finite request.
Bootstrap retains its finite total. Controller streams require an unlimited total.
Dynamic Host loopback grants retain their lease use count and independent connection byte limit.
Ordinary frontend and controller streams retain nonces only while the connection is active.
Bootstrap retains committed nonces for the current launch and rejects their replay.

`ApplianceStatus.runtime_stopped` reports a current Guest lifecycle observation.
The Host sets it only after a successful Guest lookup confirms a stopped appliance or its absence.
The field stays absent when the VM is not running or the Guest query fails.
Stored installation `STOPPED` and historical health reports cannot prove that the writer has stopped.
A restore source fence also requires the same private registration to be `REVOKED`.
Stop, revoke, instance replacement, and Host shutdown close listener connections and remove the endpoint file.
Installation revocation cancels existing egress connections and rejects new connections.
A new VM boot starts without source grants.
The Host restores source policies only from the current installation grant and Guest identity.


Runtime `ImportImage` accepts one header, then EOF for a public HTTPS `archive_url`.
An empty `archive_url` selects local upload: the header, archive chunks, then EOF.
The Host accepts no local file path. Both modes use the current namespace authorization and Guest disk quota.
Local chunks must contain 1 byte to 3 MiB. The archive must match its declared size and SHA256, within 8 GiB.
The complete request has a ten-minute limit. Each wait for an incoming message has a thirty-second limit.
The Host sends chunks synchronously to the Guest to bound memory and apply backpressure.
The Guest verifies the manifest. A reused Guest image still requires a complete, valid local upload before Host success.

### Controller environment preparation

BuildImage can select an owned base image by reference and manifest digest.
The Guest exports that immutable content into namespace scratch and exposes a read-only OCI layout to BuildKit.
The namespace quota accounts for this scratch. Export does not reserve OCI content capacity or hold the materializer slot.
It checks the complete uploaded context hash before extraction.
A successful image carries a receipt for the original build input.
Image operation receipts retain `google.rpc.Status`, including typed error details.
Their complete containerd label, including its key and encoding, fits within 4 KiB.
Build failure RPC status carries a short summary and typed details. Build progress carries the output; the receipt retains the UTF-8 end of the failure message within its label budget.

Each Build records its authenticated controller cache scope, root path, existing state, and worker ID before execution.
Cache GC records its capacity reason, deleted root, observed filesystem free-byte change, duration, and original errors.
The free-byte change includes concurrent filesystem activity. It is not an exact per-directory storage measurement.
These observations do not change cache ownership or the exact admission budget.

CopyImage accepts exact source and target allocation IDs and generations.
Both allocations must belong to the current enabled controller registration.
The Host derives their namespaces and checks their current state.
The Guest uses containerd content storage, checks target quota, and verifies the digest.
Cached copy publishes metadata without the cold materializer slot. Copy that needs
physical growth holds that slot during capacity admission and physical materialization.
The growth budget uses the held slot.
The copied image retains its build receipt and records the original copy request hash.
A receipt is valid only for the image digest stored with that receipt.

The controller can use a separate PUBLIC allocation for dependency preparation.
Its task allocation retains DENY_ALL.
Preparation cleanup must confirm all build descendants stopped before it reports success.
An unknown build or copy result requires observation of the original receipt.
A disconnected caller does not cancel an accepted build. Its execution deadline
and explicit Host session stop can cancel the Guest RPC. Host session stop waits
for its own handlers to exit. Cancellation alone does not clear the build execution
right or prove that Guest cleanup has stopped every writer.
These APIs do not permit raw storage paths or access to another owner.
