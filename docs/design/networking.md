# Networking

## Namespace and service traffic

Each environment has a separate network namespace, bridge policy, and DNS view.
Cross-environment traffic requires service export/import authorization.
A service export names one container and port. It follows the approved container ID after instance replacement.
An import binds its current source instance to a current export and authorization.
The virtual service name resolves only within that source namespace.

Service addresses use `100.126.0.0/16`.
The egress TUN uses its separate `100.127.255.0/30` range.
Neither range is a public destination or a route to the Host LAN.

For a local import, `destination_device_id: "local"` selects the current device.
The Host still enforces the same environment or controller domain.
Cross-device imports require current capability authority.
Personal mesh imports ask the destination to issue a capability after it verifies current source membership.
A stale source instance or capability requires fresh facts before another request.

## Public egress

`DENY_ALL` permits no public egress. `PUBLIC_INTERNET` permits checked public IPv4 destinations.
Guest traffic uses the fixed TUN and a dedicated egress vsock connection.
A boot-scoped Host netstack terminates permitted TCP and UDP flows through checked Host sockets.
The Host installs no route to workload subnets.
Public names use the virtual DNS endpoint. DHCP and a VZ resolver are not required.

The Host checks DNS answers, selected addresses, and every new connection after a redirect.
It rejects private, shared, loopback, link-local, metadata, namespace, and Host virtual destinations.
Public egress always admits `198.18.0.0/15` because transparent proxies map public names into that range.
This exception gives no LAN access.
The connection uses the checked address while TLS retains the original hostname.
Product egress ignores `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY`.

A Host network change can end existing flows.
The TUN pump reconnects with bounded backoff. Clients must establish new DNS and transport connections.
A service name must be resolved again after container instance replacement.

## Appliance LAN grants

A manifest can request LAN egress and a static `LAN_HTTP` frontend.
The operator must approve each right. Workload allocations do not inherit these approvals.
LAN egress permits RFC 1918 IPv4 destinations under the installation's current grant and instance fences.
Loopback, metadata, multicast, IPv6, and other namespaces remain blocked.
Public egress requires its own approval.

Host interface TCP access requires exact operator-approved IPv4 address and port pairs in `host_tcp_endpoints`.
Infrastructure exclusions apply first. A grant cannot expose runtime listeners or another namespace.
UDP does not inherit a TCP grant. The grant authorizes a connection, not an HTTP path.
Changing a grant or instance cancels affected flows and preserves unrelated installations.

The Host chooses the actual LAN interface and port for a `LAN_HTTP` frontend.
Appliance status reports that endpoint. The frontend supplies neither public reachability nor TLS termination.
The appliance authenticates clients and restricts its application routes.

## Host sockets

An operator can approve one Host Unix socket at one TCP port of the Host virtual address (`100.127.255.1`):
`agent-vmm host-socket set [--environment-id ID] --port PORT --path PATH --request-id ID`.
Without an environment ID, the grant applies to every environment of this Host. An environment's own grant at the same port takes its place there.
The containers connect to `100.127.255.1:PORT`. The Host egress connects each such flow to the socket.
The grant gives no other Host loopback, LAN, or socket access. Port 53 stays the virtual DNS.
The approval is the Host owner's, so it also applies to controller-owned environments. A controller policy update does not change it.
The path must be absolute and clean. The socket can come later: each flow connects to the path as it is then, and a flow to a missing socket is refused.
Removal (`host-socket remove`) or a new path ends the current flows. The environment's removal removes its own sockets.
The program behind the socket sees the Host process as its peer, not the container.
An installer can approve a known local service once, before the service or the environments exist.

## Listeners and restart

MCP uses fixed loopback HTTP with issued Bearer tokens.
Personal listener rules are in [the trust protocol](../protocols/trust-service.md#personal-route-authority-and-transport).

Static appliance frontends retain frame, concurrency, idle, deadline, and declared byte bounds.
Controller streams retain backpressure and idle bounds without a cumulative lifetime byte limit.
Bootstrap streams retain a finite total byte limit.
An installation stop, revoke, or instance replacement closes its affected frontend connections.
See [the appliance protocol](../protocols/appliance.md).
