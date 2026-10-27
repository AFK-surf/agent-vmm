# `agentvmm.host.v1`

Source: `host/v1/host_share.proto`.

## Message `HostShare`

HostShare records an operator-approved directory identity and its environment-scoped projection.


## Message `CreateHostShareRequest`

CreateHostShareRequest carries inputs for an operation that approves one checked Host
directory for the selected environment.


## Message `RebindHostShareRequest`

RebindHostShareRequest carries inputs for an operation that changes the checked source of an
existing share under its revision fence.


## Message `RevokeHostShareRequest`

RevokeHostShareRequest carries inputs for an operation that revokes one share rather than
granting general Host filesystem access.


## Message `ListHostSharesRequest`

ListHostSharesRequest carries inputs for an operation that reads operator-approved directory
shares and their current references.


## Message `ListHostSharesResponse`

ListHostSharesResponse carries results from an operation that reads operator-approved
directory shares and their current references.


## Enum `HostShareState`

### `HOST_SHARE_STATE_UNSPECIFIED = 0`

### `HOST_SHARE_STATE_AVAILABLE = 1`

### `HOST_SHARE_STATE_UNAVAILABLE = 2`

### `HOST_SHARE_STATE_REVOKING = 3`

### `HOST_SHARE_STATE_REVOKED = 4`
