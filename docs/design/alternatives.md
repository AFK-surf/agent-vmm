# Alternatives

These decisions describe the current design. They are not compatibility promises for other runtimes.

| Alternative | Decision and cost |
| --- | --- |
| Guest-native runc | Rejected for workload execution. It exposes the shared Guest kernel directly to untrusted syscalls. |
| Docker Engine API | Rejected. Its socket controls the complete Guest and does not supply the required resource authority. |
| Podman or embedded libpod | Rejected. Another service and state store would retain the same per-request authorization work. |
| Kubernetes or CRI orchestration | Rejected. Pods, controllers, and a second policy store do not remove a current component. |
| One VM per workload | Rejected for this product. It changes the shared VM capacity and lifecycle contract. |
| Firecracker inside the Guest | Rejected. Nested KVM adds another VM lifecycle without solving resource authorization. |
| Apple Containerization | Not the runtime owner. Its per-container VM model does not supply this shared Guest design. |
| Shared BuildKit daemon | Rejected. It mixes untrusted build control, contexts, secrets, and cache authority. |

GVisor adds syscall, filesystem, and network compatibility costs.
An incompatible image returns an error. The runtime does not select a less isolated fallback.
