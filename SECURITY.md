# Security

## Report a vulnerability

Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/AFK-surf/agent-vmm/security/advisories/new).
Do not include credentials or private workload data in a public issue.

## Supported versions

The project has no release. Security fixes target `main` only.

## Local threat model

This section covers local operating system users on the Host computer.
Other trust boundaries do not change. These boundaries include workload VMs,
appliances, remote controllers, mesh peers, and the network.

### In scope

- Other local users without administrator rights. We treat them as attackers.

- Their access to Agent VMM through local network ports on 127.0.0.1.

- Their access to Agent VMM files, sockets, and process arguments.

- Their access to shared temporary directories, such as /tmp.

- An unprivileged service account that tries to control a root component.
  Examples are the macOS service executor, launchd, and systemd.

### Out of scope

- Processes that run as the same user as the Host.
  These processes can read all Host state and use all Host sockets.

- The root user and administrators.

- Physical access to an unlocked computer.

### Assumptions

- The Host keeps all state in a private directory.
  Only the Host user can open this directory (mode 0700).
  The Host does not start if this directory has a different owner or mode.

- Signed macOS release Hosts in a logged-in user session can store secrets in Keychain.
  The device root uses Secure Enclave when available. Secure Enclave does not export its private key.

- Development builds, macOS daemons, and Linux use mode-0600 credential files.
  File confidentiality depends on permissions and full-disk encryption.

- The Host preserves its selected secret backend. An unavailable selected Keychain backend stops Host startup.

- MCP listens on loopback HTTP. Any local user can connect.
  Requests without an issued token return 401.

- A token authorizes its environment within the MCP installation grant.
  Only the Host owner can issue tokens or rotate the signing key.

- Tokens are secrets. Store them in mode-0600 files and send them only through `Authorization` headers.

- The Host does not put secrets in command arguments.
  Other users can read the command arguments of all processes.

- Any local user can connect to a TCP port on 127.0.0.1.
  A loopback port is not private.

- Build and test scripts do not use fixed names in shared temporary
  directories.

- A host share source must not have a parent directory that another
  user can write. The Host rejects such a source.

- On Linux, the Host runs as the agent-vmm system user.
  Only root and agent-vmm can use the operator socket.

### Same-user separation

Agent VMM separates some functions that run as the same user.
Examples are agent sessions and remote compute registrations.
This separation limits mistakes. It is not a security boundary.

### Residual risks

- A local user can open many connections to a loopback port.
  This can delay or stop other connections to that port.

- A device service listener with TCP loopback exposure accepts
  connections from all local users. The default Unix socket listener
  accepts connections only from your user.

- An appliance frontend with loopback exposure accepts connections
  from all local users. The appliance protocol must authenticate
  the client.

- A LAN frontend or LAN service listener accepts connections from
  the local network. Its own protocol must authenticate the client.

- Another local user can occupy the MCP port before the Host binds it.
  That user can receive tokens from clients that use the saved URL.
  The Host does not start on another port.

## Other trust boundaries

- Each workload runs in its own l4h VM with its own Linux kernel.
  seL4 and l4h separate the VMs. Dom0 cannot read the memory of a workload VM.

- A process that runs as root in a workload VM has root authority in that VM only.

- The dom0 service controls the disks, the switch policy, and the DNS answers of each workload VM.

- An appliance must authenticate its LAN clients.

- A remote controller owns only allocations under its authorized registration.

- Root overlays and named volumes use dm-crypt with one key for each disk.
  Dom0 seals these keys with its master key.

- On macOS, the pool file `pool.raw` contains that master key and all image content.
  The pool file does not protect its contents from a reader of the file.

Use FileVault or equivalent full-disk encryption for offline device confidentiality.
A copy of the pool file exposes its contents. Encrypt backups independently.
Keep diagnostic disks and keys until their owner approves exact deletion.

Read [the security model](docs/design/security-model.md) for resource and remote authority.
Read [backup](docs/operator/backup.md) before copying persistent state.
