# Glossary

| Term | Meaning |
| --- | --- |
| Host | The `agent-vmm-host` controller that owns the VM, policy database, and operator socket. |
| Guest | The Linux VM that runs containerd, gVisor, and workload storage. |
| VMMD | The `agent-vmmd` Guest daemon that owns runtime operations. |
| Environment / namespace | An environment owns a private Guest namespace and its workloads, volumes, and image grants. |
| Allocation | A controller-owned compute environment under one registration. |
| Registration | A Host record that binds one external controller to its authorized compute scope. |
| Installation | One installed appliance with its approved grant, lifecycle, and private data volume. |
| Generation | An owner's identity for one resource incarnation. |
| Epoch | An unpredictable nonzero controller connection identity. |
| Fence | An authority check that rejects a stale generation, epoch, revision, or command. |
| Receipt | The stored result of one request ID, including an accepted operation's unknown or terminal outcome. |
| Retained | An environment state that stops workloads and preserves private files for owner action. |
| PersonalMesh | A signed personal device membership descriptor and its membership generation. |
| Appliance | A system service that runs in gVisor with an approved manifest and private data volume. |
| Controller | An external service that owns registrations, allocations, and command policy. |
| Binding | The MCP client's allocation label. The private Unix socket controls local access. |
| Frontend | A manifest-declared appliance endpoint, represented by `ControllerFrontend`. |
| Host share | One operator-approved host directory projected into one environment. |
| Service export | Authorization to expose one container port through a Host-owned route. |
| Service import | Authorization to connect an importer to one approved service export. |
