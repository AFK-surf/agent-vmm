# Security model

seL4 and l4h separate each workload VM from the other VMs and from the Host operating system.
Each workload has its own Linux kernel. A kernel compromise stays in that workload VM.
On macOS, Virtualization.framework also separates the complete l4h computer from macOS.

## Isolation layers

The seL4 kernel gives each l4h component and each VM only its own capabilities.
`l4h-memd`, `l4h-blkd`, and `l4h-netd` own memory, block devices, and virtual switches.
Dom0 can stop a VM or change its memory size. It cannot read the memory of a VM.

The dom0 service controls what each workload VM receives.
It selects the disks, the disk keys, the switch address, the network policy, and the DNS answers.
A compromise of the dom0 service exposes the disks and network of all workloads.
The Host and the dom0 service are in the trusted computing base.

`vmm-guest-init` is PID 1 in each workload VM.
It serves the Guest RPC only to the gateway address, with the RPC token of that boot.
A workload that controls its VM can change only that VM and its own disks.

## Resource authority

The Host selects the environment for each caller before it sends runtime requests to dom0.
A request cannot select another environment.
The Host checks logical ownership before it reads or changes containers, images, or volumes.
Shared immutable image content does not share writable container state.
Host shares and service exports and imports grant separate, explicit rights.
An approved mount or route does not grant general Host access.

A workload VM receives no raw Host device, runtime socket, or arbitrary Host path.
It receives its own block devices and one interface on the isolated switch.

## Workload privileges

The workload starts with the user of the request or of the image.
A process that runs as a non-root user has no Linux capabilities.
A process that runs as root has root authority in its own VM only.
Host share mounts use `nosuid` and `nodev`.

A build VM runs BuildKit as UID 1000.
That process keeps only `SETUID`, `SETGID`, and `SETPCAP` for sub-ID mapping.

## Local users

Signed macOS release Hosts in an agent session can use Keychain.
Secure Enclave keeps the device root private key non-exportable when available.
Development builds, daemon mode, and Linux use mode-0600 secret files.
Read [SECURITY.md](../../SECURITY.md) for backend selection, token authority, and residual risks.

The service executor handles fixed administrator service operations.
The non-root lifecycle process owns business state and receives only the result of those fixed operations.
Untrusted request parameters cannot select an administrator command or filesystem target.
On macOS, only the Carrier has the virtualization entitlement.

## Remote trust

A registration authorizes one controller domain. It cannot enumerate the allocations of another domain.
Runtime sessions have independent revocation and replacement fences.
Personal routes require current membership and a destination-issued capability.
Endpoint observations establish reachability. They do not establish authority.

See [trust and service](../protocols/trust-service.md) for signatures, pairing, and route verification.
See [networking](networking.md) for egress and service routes.

## Persistent data

Root overlays and named volumes use dm-crypt with one key for each disk.
Dom0 seals each key with its master key. The pool file does not protect that master key.
Read [the data protection rules](../../SECURITY.md#other-trust-boundaries) before copying or deleting persistent state.
