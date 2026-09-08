## Development Guidelines

- Do not update this section unless the user explicitly requests a change.
- Optimize the complete design, not the size of the diff.
- Refactor or delete code when this reduces maintenance cost and structural complexity.
- Avoid excessive design, defensive code, and patch-based fixes.
- Do not run `git stage`, `git reset`, `git add`, or `git commit` unless the user requests it.
- Do not inspect the Git stage unless the user requests it.
- Spawn review agents with an empty inherited context. Put the read-only role, review scope, and decisions to evaluate in the review task instead of forking the implementation conversation.
- During implementation, run the smallest focused test set that covers the current change or failure. Run broader phase gates only after focused tests are stable, and run the full test suite only for final convergence or when a broad regression check is specifically justified.
- Apply the following cleanup continuously as responsibilities settle, and repeat the audit after all automated TODO items pass:
  - Compare the branch with `main` and remove low-quality generated code.
  - Remove comments that a maintainer would not add or that conflict with the local style.
  - Remove excessive checks from trusted or previously validated call paths. A build, publication, deployment, or runtime check must protect an independent boundary and change a real user, operator, or runner-deployer decision; CI output, a test, a TLA+ model, or metadata presence is not sufficient justification.
  - Reuse or extend an existing type when a new type does not improve maintenance.
  - Extend or parameterize an existing test when a separate test does not improve its scope.
  - Rename tests when the file name does not match the test scope.
  - Make all changed code follow the style of its surrounding code.
  - Review any production source file that exceeds 500 lines after the change.
  - Split that file when separate responsibilities justify the split.
  - Delete any component or implementation that is no longer necessary.
  - Review any component that has only one direct consumer.
  - Merge or refactor that component when separation has no clear value.
- Use only one to three sentences in the final summary.

## Output Guidelines

Remove any text that does not affect a decision.

- Give the result or solution without an introduction.
- Omit known facts and evident background information.
- Give an example only when it explains essential logic.
- Ask a question when a wrong assumption costs more than the question.
- Otherwise, use the best supported assumption and state it clearly.

## Documentation style

Use the project `ste-writing` skill for documentation, READMEs, runbooks, release
notes, and pull request text. Use STE-flavored mode by default. Use strict mode
for procedures and safety text. Do not use this default when the user requests
detailed writing or a different style.

## Unexpected Situations

- `build/host/install.sh` selects its payload exclusively through `OUTPUT_DIRECTORY`; a positional app path is ignored. Always set `OUTPUT_DIRECTORY` when installing a non-default package, then verify the installed helper set before reporting the local version.
- `InstallApplianceDomain` must insert the private `remote_registrations` row before the installation row because `appliance_installations.registration_id` is an immediate foreign key. Keep both writes in the same transaction so a fresh installation cannot fail with a foreign-key error after planning succeeds.
- The MCP `ForwardTCP` bridge has one sender goroutine; do not call `CloseSend` until both copy directions have exited, or gRPC's stream state races with an in-flight `Send`.
- The loopback forward helper can report success after an invalid capability if the host closes the TCP connection gracefully after the request is buffered; rejected frontend authentication must use a reset/observable failure so stale-token tests cannot pass spuriously.
- `go test -json` can end with a package-level `FAIL`/interrupt before a child subtest emits a terminal event; the E2E report renderer must retain that child as `INTERRUPTED` instead of omitting the failed case.
- Guest registry pulls collapse the containerd fetch error into `IMAGE_BLOB_FETCH_FAILED`, while the current VZ serial log path can remain empty; preserve a bounded diagnostic for the failed descriptor and egress counters when investigating large-image fetches.
- The guest TUN can deliver a non-IPv4 packet to the egress pump; validate and count it at the TUN boundary, because letting `WriteFrame` reject it tears down the whole vsock session and surfaces as a misleading registry download failure.
- On an existing Guest state disk, containerd needs bounded project-quota headroom for startup metadata before VMMD can listen on vsock 4050. Do not restore the exact OCI idle ceiling until containerd startup and stale-task recovery finish; a reset-by-peer loop with a running VZ VM can otherwise be a pre-listener quota failure, not disk corruption.
- OCI startup prepares fixed owner roots; it does not traverse imported snapshot contents. Quota access uses no-follow descriptors for directories and regular files. Preserve creation-time project inheritance and each writable snapshot's upper/work project pairing. A snapshot created by older code can retain an incorrect workdir project; bound any offline audit and repair to the affected snapshot instead of restoring a startup tree scan or discarding the disk.
- E2E code that opens the host WAL database directly must use one connection with a busy timeout; otherwise forced-stop recovery can race a test-side write and surface `SQLITE_BUSY` from an unrelated RPC.
- Keep controller allocation, lease, capability, and reconnect fencing coupled in one lifecycle model, and keep one appliance installation's activation and operation fencing coupled in one lifecycle model. Model aggregate admission, authority isolation, system inventory projection, and cross-installation ownership separately; a large state-space jump means an orthogonal product was probably reintroduced. Preserve the independent negative-config runs.
- macOS 27 App Attest is supported only by a fully signed Mac app running in a user context, not by a daemon or bare LaunchAgent helper. If Patchouli is linked into `agent-vmm-host`, package the host as the attested app's `Contents/MacOS` executable with the required provisioning profile and entitlements; the current `Contents/Helpers/agent-vmm-host` LaunchAgent layout cannot be assumed to satisfy `DCAppAttestService.isSupported`.
- The development branch of `build/host/package.sh` intentionally requires an ad-hoc host and rejects `_SecItem`/`SecKey` symbols, so the default development bundle cannot exercise Patchouli's Keychain, Secure Enclave, or App Attest path. Use Patchouli's mock/synthetic tests there and a separately provisioned attestation build for real-device validation.

Record a surprising project issue when another agent can encounter it again.
Tell the developer about the issue and add it to this file.
Update or delete an entry when the issue changes or no longer applies.
Merge entries that describe the same condition.
Do not record temporary failures or narrow implementation mistakes.

### General Project Assumptions

- This project is new and has no production release. Report code that does not follow current best practices.
- `.context` contains planning documents only. Production and test code must not read from or write to it.
- Do not preserve compatibility with previously installed MCP, connector, data-worker binaries, launchd jobs, configuration, or local state. Remove them with explicit manual cleanup instructions; production code must not detect, migrate, dual-run, or read their legacy state.
- Build guest artifacts on Linux arm64 and a case-sensitive file system.
- Build the Guest from the current Alpine stable repository. Do not pin Alpine,
  APK, kernel, containerd, gVisor, or BuildKit versions or input hashes. CI must
  test the resolved dependency set before it publishes a Guest bundle.
- Install the complete gVisor release directory.
- Select `linux/arm64` when an image index contains multiple platforms.
- Record the content identity of the completed Guest bundle and its published
  artifacts. Do not treat downloaded dependency hashes as release identity.
- The Alpine `containerd` package installs a guest-native runc. Remove runc and
  `containerd-shim-runc-v2` from the assembled Guest.
- Verify that the guest artifact contains no runc or shim-runc-v2.
- Resolve runsc, its shim, and containerd during the Guest build. Test the
  resolved set with a real sandbox.
- Do not hide a compatibility failure with a wrapper or a runc fallback.
- Run the nested rootless BuildKit worker as UID 1000 inside runsc.
- Give it only sandbox-visible `CAP_SETUID`, `CAP_SETGID`, and `CAP_SETPCAP` for sub-ID mapping.
- Do not depend on setuid or file capabilities for `newuidmap` and `newgidmap` in this sandbox.
- The containerd 2.2.6 namespace store can return empty labels for a missing namespace.
- Use `List` to test namespace existence. Do not treat a successful `Labels` call as proof of existence.
- Containerd image records are namespace-scoped, but their CAS content is shared across the guest. Remove only a fenced namespace record and never report its logical size as reclaimed disk space.
- The containerd 2.2.6 Docker authorizer caches failed anonymous token results without an expiry. Recreate the authorizer and resolver for each bounded registry retry.
- The guest root uses read-only SquashFS. Do not store the build host resolver file in it.
- The current network can map public names to `198.18.0.0/15`. Treat this range as an approved transparent proxy exception.
- Route guest and namespace public IPv4 traffic through the fixed TUN and dedicated egress vsock session.
- Resolve public names through the virtual DNS endpoint; do not restore DHCP or a VZ resolver dependency.
- Neither network exception gives a workload access to the LAN.
- Mark each independent gVisor container spec with `io.kubernetes.cri.container-type=sandbox`.
- Code-Hex/vz v3.7.1 does not wake a blocked virtio-socket `Accept` on listener close; keep the pinned post-v3.7.1 revision or a later release that preserves the close wakeup.
- Use one VMMD-owned internal containerd namespace and the containerd overlayfs snapshotter for persistent workloads. Share immutable lower snapshots by digest; keep each workload's writable upper, volume, runsc sandbox, cgroup, and network independent. Containerd namespaces are not a security boundary.
- Use a build-only runsc `root:memory` overlay for the read-only BuildKit root.
- Do not restore nested persistent overlays.
- Use the fixed supervisor as the sandbox init. Run the user process as a retained exec process.
- Do not replace the supervisor with a delay, a CRI pause container, or exit-code substitution.
- gVisor blocks sandbox-created host-visible Unix sockets by default. Keep runsc `host-uds=create` so system appliances can bind only their projected private sockets; do not grant `open` or `all`.
- Reject new public relay connections after the VM enters `STOPPING`.
- Permit the internal `PrepareShutdown` connection during `STOPPING` so the guest can finish cleanup.
- Link the Host executable with the canonical Guest manifest digest. Keep a Guest SBOM only as CI inventory for a concrete dependency or license consumer, never as runtime bundle authority.
- Treat a new remote connection instance after the stored state became `DISCONNECTED` as grace-period recovery. Revoke remote leases for a replacement instance only when the previous instance is still `CONNECTED`.
- A valid remote lease may open a new runtime session after connector recovery. Revoke the previous session capability before issuing its replacement; do not reject the lease as already open.
- Keep remote admission state separate from connector transport state. Draining rejects new allocations and leases but preserves existing leases and runtime sessions; only revocation tears them down.
- Remote leases are in-memory while allocation state is persisted. On host restart, reconcile every persisted `LEASED` allocation to a fail-closed non-leased state before admitting a replacement connector; an empty in-memory lease map cannot revoke or recover that allocation.
- The supervised container process runs with an empty capability set. A root entrypoint cannot initialize a bind-mounted 0700 volume owned by a non-root UID; tests and workloads that require that ownership must start with the matching numeric UID/GID instead of relying on root capability bypass.
- Containerd can retain the durable `running` label for a short interval after its task has been reaped; use `inspectContainer`'s task/status reconciliation as the state authority, treating a missing task as `stopped` and never exposing raw `NOT_FOUND` or stale `running` results.
- The guest system namespace survives an appliance stop/removal lifecycle. A streamed system OCI import derives its request ID from the immutable import header; the bundled read-only image derives it from its digest-bound canonical manifest reference. Reusing a fixed ID such as `bundled-mcp` makes a later setup fail with an idempotency conflict when content identity changes.
- Appliance reconciliation runs frequently; publication must preserve an already-current private controller route and frontend runtime. Recreating either on every tick churns loopback ports and drops the MCP appliance's controller session.
- `build/guest/build.sh` pipes `cpio` into `xz`; without an explicit `cpio` prerequisite, the redirected pipeline can emit a valid 32-byte empty initramfs and the host VM remains stuck in `STARTING`. Keep the prerequisite check and install `cpio` in every Linux arm64 build environment.
- The development macOS VM is configured with two vCPUs; the bundled MCP grant currently reserves 1000 CPU millis for the system appliance and each client allocation, so one stale MCP client allocation can exhaust the host ledger and make a fresh Streamable HTTP handshake return 503. Clean up the stale allocation before diagnosing the endpoint as unhealthy, and keep the advertised client capacity aligned with the actual resource grant.
- A current same-environment `service_import` can return `INTERNAL` (`runtime request failed`) with `destination_device_id: "local"` even when the source container, destination container instance, and export are all current and running; no import row or virtual-service DNS record is created. A stale export ID returns the more useful `SERVICE_EXPORT_NOT_FOUND`, so verify the export before attributing the failure to local routing.
