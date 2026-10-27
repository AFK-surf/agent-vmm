# Security model

The VM separates the Host operating system from Linux workloads.
Each workload runs in its own runsc sandbox and shares the Guest kernel.
VMMD alone controls containerd, CNI, quotas, and Guest network policy.
A containerd namespace is a management scope. It is not a security boundary.

## Resource authority

The Host fixes the caller's environment before it forwards runtime requests.
A request cannot select another owner's namespace.
VMMD checks logical ownership before it reads or changes containers, images, snapshots, or volumes.
Shared immutable image content does not share writable container state.
Host shares and service exports/imports grant separate, explicit rights.
An approved mount or route does not grant general Host access.

Workloads receive no Linux capabilities, raw devices, runtime sockets, or arbitrary Host paths.
A fixed supervisor owns sandbox init. The workload runs as a retained exec process.
An image that requires privileged initialization must use an unprivileged configuration.

## Local users

Signed macOS release Hosts in an agent session can use Keychain.
Secure Enclave keeps the device root private key non-exportable when available.
Development builds, daemon mode, and Linux use mode-0600 secret files.
Read [SECURITY.md](../../SECURITY.md) for backend selection, token authority, and residual risks.

The service executor handles fixed administrator service operations.
The non-root lifecycle process owns business state and receives only the result of those fixed operations.
Untrusted request parameters cannot select an administrator command or filesystem target.

## Remote trust

A registration authorizes one controller domain. It cannot enumerate another domain's allocations.
Runtime sessions have independent revocation and replacement fences.
Personal routes require current membership and a destination-issued capability.
Endpoint observations establish reachability. They do not establish authority.

See [trust and service](../protocols/trust-service.md) for signatures, pairing, and route verification.
See [networking](networking.md) for egress and LAN grants.

## Persistent data

Read [the data protection rules](../../SECURITY.md#other-trust-boundaries) before copying or deleting persistent state.
