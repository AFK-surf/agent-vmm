## Development Guidelines

- Do not update this section unless the user explicitly requests a change.
- Optimize the complete design, not the size of the diff.
- Refactor or delete code when this reduces maintenance cost and structural complexity.
- Avoid excessive design, defensive code, and patch-based fixes.
- Do not run `git stage`, `git reset`, `git add`, or `git commit` unless the user requests it.
- Do not inspect the Git stage unless the user requests it.
- Complete the following cleanup after all automated TODO items pass:
  - Compare the branch with `main` and remove low-quality generated code.
  - Remove comments that a maintainer would not add or that conflict with the local style.
  - Remove excessive checks from trusted or previously validated call paths.
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

## Unexpected Situations

Record a surprising project issue when another agent can encounter it again.
Tell the developer about the issue and add it to this file.
Update or delete an entry when the issue changes or no longer applies.
Merge entries that describe the same condition.
Do not record temporary failures or narrow implementation mistakes.

### General Project Assumptions

- This project is new and has no production release. Report code that does not follow current best practices.
- Build guest artifacts on Linux arm64 and a case-sensitive file system.
- Alpine stable repositories remove superseded kernel package revisions; keep the locked kernel package and digest current or retain it in `AGENT_VMM_DOWNLOAD_CACHE`.
- Install the complete gVisor release directory.
- Select `linux/arm64` when an image index contains multiple platforms.
- Verify the digest of each final guest component.
- The Alpine `containerd` package installs a guest-native runc. Extract only the locked containerd payload.
- Verify that the guest artifact contains no runc or shim-runc-v2.
- Lock runsc, its shim, and containerd separately. Test the selected versions with a real sandbox.
- Do not hide a compatibility failure with a wrapper or a runc fallback.
- Run the nested rootless BuildKit worker as UID 1000 inside runsc.
- Give it only sandbox-visible `CAP_SETUID`, `CAP_SETGID`, and `CAP_SETPCAP` for sub-ID mapping.
- Do not depend on setuid or file capabilities for `newuidmap` and `newgidmap` in this sandbox.
- The containerd 2.2.6 namespace store can return empty labels for a missing namespace.
- Use `List` to test namespace existence. Do not treat a successful `Labels` call as proof of existence.
- The guest root uses read-only SquashFS. Do not store the build host resolver file in it.
- The current network can map public names to `198.18.0.0/15`. Treat this range as an approved transparent proxy exception.
- Route guest and namespace public IPv4 traffic through the fixed TUN and dedicated egress vsock session.
- Resolve public names through the virtual DNS endpoint; do not restore DHCP or a VZ resolver dependency.
- Neither network exception gives a workload access to the LAN.
- Mark each independent gVisor container spec with `io.kubernetes.cri.container-type=sandbox`.
- Code-Hex/vz v3.7.1 does not wake a blocked virtio-socket `Accept` on listener close; keep the pinned post-v3.7.1 revision or a later release that preserves the close wakeup.
- Use the containerd native snapshotter and runsc `overlay2=none` for persistent workloads.
- Use a build-only runsc `root:memory` overlay for the read-only BuildKit root.
- Do not restore nested persistent overlays.
- Use the fixed supervisor as the sandbox init. Run the user process as a retained exec process.
- Do not replace the supervisor with a delay, a CRI pause container, or exit-code substitution.
- Reject new public relay connections after the VM enters `STOPPING`.
- Permit the internal `PrepareShutdown` connection during `STOPPING` so the guest can finish cleanup.
- Package development-signed guest bundles only with an ad-hoc-signed helper built with the development trust root. A release-signed helper requires a release-signed guest bundle.
- Treat a new remote connection instance after the stored state became `DISCONNECTED` as grace-period recovery. Revoke remote leases for a replacement instance only when the previous instance is still `CONNECTED`.
- A valid remote lease may open a new runtime session after connector recovery. Revoke the previous session capability before issuing its replacement; do not reject the lease as already open.
