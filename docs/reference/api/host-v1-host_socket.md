# `agentvmm.host.v1`

Source: `host/v1/host_socket.proto`.

## Message `HostSocket`

HostSocket records an operator-approved Host Unix socket that an environment's containers reach
over TCP at a port of the Host virtual address. The Host connects each such flow to the socket.
An empty environment ID means every environment of this Host; an environment's own socket at the
same port takes its place there.


## Message `SetHostSocketRequest`

SetHostSocketRequest carries inputs for an operation that approves one Host Unix socket path at
one port for the selected environment, or for every environment when no environment is selected.
The socket need not exist yet: each flow connects to the path as it is then. A port that has a
socket already gets the new path.


## Message `RemoveHostSocketRequest`

RemoveHostSocketRequest carries inputs for an operation that removes the socket at one port of
the selected environment, or of every environment when none is selected, and closes its current
flows.


## Message `ListHostSocketsRequest`

ListHostSocketsRequest carries inputs for an operation that reads operator-approved Host
sockets: all of them, or those the selected environment reaches.


## Message `ListHostSocketsResponse`

ListHostSocketsResponse carries results from an operation that reads operator-approved Host
sockets.
