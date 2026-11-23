# Alternatives

These decisions describe the current design. They are not compatibility promises for other runtimes.

## Selected design

**One l4h VM for each workload**: Selected. Each workload has its own kernel. seL4 and l4h isolate that kernel from other workloads.
l4h runs its VMs without hardware virtualization, so the same design runs inside Virtualization.framework.
The cost is one kernel and one PID 1 for each workload. Demand memory hotplug and shared image disks limit that cost.

## Rejected runtimes

**Shared-kernel runc**: Rejected for workload execution. It exposes one shared kernel directly to untrusted syscalls.

**gVisor in one shared VM**: Not used. Its workloads share one Guest kernel below the sandbox.
It adds syscall, filesystem, and network compatibility costs.

**Docker Engine API**: Rejected. Its socket controls the complete runtime and does not supply the required resource authority.

**Podman or embedded libpod**: Rejected. Another service and state store would retain the same per-request authorization work.

**Kubernetes or CRI orchestration**: Rejected. Pods, controllers, and a second policy store do not remove a current component.

**Firecracker or nested KVM**: Rejected. It needs nested hardware virtualization in the Carrier VM.
It adds another VM lifecycle without solving resource authorization.

**Apple Containerization**: Not used. It runs on macOS only. The same dom0 code serves macOS and Linux arm64.

**Shared BuildKit daemon**: Rejected. It mixes untrusted build control, contexts, secrets, and cache authority.
Each build uses a disposable build VM.

## No fallback

An incompatible image returns an error. The runtime does not select a less isolated fallback.
