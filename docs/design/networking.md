# Networking

## Switch and addresses

Each workload VM has one network interface on an isolated l4h switch.
The switch network is `10.77.0.0/16`. Dom0 owns `10.77.0.1` as the gateway.
Each boot gets one slot address. The dom0 service keeps the slot until all grants of that boot are gone.

The dom0 service applies one nftables table at the gateway interface.
A source address selects the policy of one workload VM.
The table drops traffic between workload VMs.
Cross-workload traffic requires a service route through a dom0 proxy.

Service addresses use `100.127.254.0/24`. The Host virtual address is `100.127.255.1`.
Dom0 routes both ranges to its loopback interface.
Neither range is a public destination or a route to the Host LAN.

## Gateway access

A workload VM can reach only these gateway endpoints:

- The boot configuration port, for its own slot.

- The DNS service on port 53, through UDP and TCP.

- The ports of its own Host shares.

- The Host socket ports that its environment has.

- The service addresses and ports that its imports have.

The table drops all other traffic from a workload VM to dom0.

## Public egress

`DENY_ALL` permits no public egress. `PUBLIC_INTERNET` permits public IPv4 destinations.
Dom0 forwards permitted packets and applies source NAT at its uplink.
On macOS, the uplink is the NAT interface of the Carrier VM.

The table drops private, shared, loopback, link-local, documentation, multicast, and reserved destinations.
These drops come before the `PUBLIC_INTERNET` rule. Internet access does not grant LAN access.
The policy has IPv4 rules only. A workload VM has no IPv6 address.

Transparent proxies on the Host network can map public names into `198.18.0.0/15`.
The table drops that range by default.
The dom0 DNS service records such an address from an upstream answer, with the answer lifetime.
A `PUBLIC_INTERNET` workload can then reach that recorded address.
This exception gives no LAN access.

## DNS

PID 1 writes the gateway address to `/etc/resolv.conf` in each workload VM.
The dom0 DNS service identifies the workload VM by its source address.
It refuses a query from an address with no running boot.

A virtual service name resolves to its service address only for the importing workload VM.
The names are `<name>`, `<name>.services.agent`, and `<name>.<environment>.agent`.
Other single-label names and other `.agent` names do not exist.

For a `PUBLIC_INTERNET` environment, the service sends other names to an upstream resolver.
It selects a public IPv4 resolver from the dom0 resolver file, or `1.1.1.1`.
For a `DENY_ALL` environment, the service refuses other names.

## Service routes

A service export names one container and port.
An import binds its current source instance to a current export and authorization.
The Host gives each import a service address and the port of the export.

The dom0 service listens at that address and port.
It accepts a connection only from the address of the importing workload VM.
It then connects to the exporting workload VM and copies the TCP stream.
A revoked or expired route closes its connections.
Resolve a service name again after container instance replacement.

For a local import, `destination_device_id: "local"` selects the current device.
The Host still enforces the same environment or controller domain.
The Host now rejects an import for another device and an import with a capability.
Read [the trust protocol](../protocols/trust-service.md#personal-route-authority-and-transport) for cross-device route authority.

## Host shares and Host sockets

A Host share is a 9p2000.L export at a gateway port.
The export accepts only the workload VM of that boot and its attach name.

An operator can approve one Unix socket at one TCP port of the Host virtual address:
`agent-vmm host-socket set [--environment-id ID] --port PORT --path PATH --request-id ID`.
Without an environment ID, the grant applies to all environments on this Host.
A grant of an environment at the same port takes its place there.

The workload connects to `100.127.255.1:PORT`. The dom0 service connects each flow to the socket path.
The grant gives no other Host loopback, LAN, or socket access. Port 53 stays the DNS service.
The approval belongs to the Host owner, so it also applies to controller-owned environments.
A controller policy update does not change it.

The path must be absolute and clean. The socket can come later.
Each flow connects to the path as it is then. A flow to a missing socket fails.

Removal with `host-socket remove` or a new path ends the current flows.
The removal of an environment removes its own grants.
The program behind the socket sees the dom0 service process as its peer, not the workload.

On Linux, the Host and the dom0 service use one filesystem.
On macOS, the dom0 service resolves the socket path in the Carrier VM.

## Appliance grants

A manifest can request LAN egress, Host TCP endpoints, and a static `LAN_HTTP` frontend.
The operator must approve each right. Workload allocations do not inherit these approvals.
The Host checks the approved grant against the manifest before it accepts an installation.
`host_tcp_endpoints` holds at most 64 exact IPv4 address and port pairs.

The dom0 switch policy has only the `DENY_ALL` and `PUBLIC_INTERNET` modes.
It has no LAN egress rule. Private destinations stay blocked for every workload VM.

## Host listeners

MCP uses loopback HTTP with issued Bearer tokens. The default port is 8788.
Personal listener rules are in [the trust protocol](../protocols/trust-service.md#personal-route-authority-and-transport).
See [the appliance protocol](../protocols/appliance.md) for frontends.
