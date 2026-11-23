# Glossary

## Components

**Host**: The `agent-vmm-host` process. It owns the policy database, credentials, and operator socket.

**Carrier**: The macOS `agent-vmm-carrier` process. It owns the Virtualization.framework VM and the pool file.

**l4h**: The hypervisor that runs Linux VMs on the seL4 microkernel.

**Dom0**: The first l4h VM. Its Linux system runs the dom0 service.

**Dom0 service**: The owner of disk extents, disk keys, switch policy, and the workload VM lifecycle.
On macOS, it is the `l4h-dom0d` daemon. On Linux, the Host includes the same `vmm-dom0` code.

**`l4h-memd`**: The l4h component that owns free memory and the memory blocks of each VM.

**Console**: The development tool in `apps/console`. It reads one Host through the operator socket.

**Controller**: An external service that owns registrations, allocations, and command policy.

## Runtime objects

**Workload VM**: One l4h VM that runs one workload. The public API calls a workload a container.

**Guest**: The Linux system in a workload VM. `vmm-guest-init` is its PID 1.

**Guest bundle**: The build output that contains seL4, l4h, the dom0 system, and the workload initramfs.

**Extent pool**: The disk region where dom0 allocates image, root overlay, and volume disks.

**Environment**: The owner of workloads, volumes, image grants, limits, and network policy.

**Namespace**: The API identifier of the private scope of one environment.

**Host share**: One operator-approved host directory projected into one environment.

**Host socket**: One operator-approved Host Unix socket at a port of the Host virtual address.
The grant applies to one environment or to every environment.

**Service export**: Authorization to expose one container port through a Host-owned route.

**Service import**: Authorization to connect an importer to one approved service export.

## Remote compute and appliances

**Registration**: A Host record that binds one external controller to its authorized compute scope.

**Allocation**: A controller-owned compute environment under one registration.

**Binding**: The allocation that one MCP token selects. The Bearer token controls local access.

**PersonalMesh**: A signed personal device membership descriptor and its membership generation.

**Appliance**: A system service with an approved manifest, private registration, and private data volume.

**Installation**: One installed appliance with its approved grant, lifecycle, and private data volume.

**Frontend**: A manifest-declared appliance endpoint, represented by `ControllerFrontend`.

## Authority and receipts

**Generation**: The identity that an owner gives to one resource incarnation.

**Epoch**: An unpredictable nonzero controller connection identity.

**Fence**: An authority check that rejects a stale generation, epoch, revision, or command.

**Receipt**: The stored result of one request ID. It includes an unknown or terminal outcome.

**Retained**: An environment state that stops workloads and preserves private files for owner action.
