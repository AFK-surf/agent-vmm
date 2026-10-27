# `agentvmm.host.v1`

Source: `host/v1/mesh.proto`.

## Message `ServiceListener`

ServiceListener reports one operator-approved listener and its route authority.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `available` | 16 | `optional bool` | Current socket observation. These fields are not persisted authorization. |

## Message `PersonalMeshResponse`

PersonalMeshResponse returns the local verified mesh projection after an operator membership
action.


## Message `PersonalMeshPairingInvitation`

PersonalMeshPairingInvitation contains the bounded out-of-band material needed to join one
pairing exchange.


## Enum `DeviceIdentityState`

### `DEVICE_IDENTITY_STATE_UNSPECIFIED = 0`

### `DEVICE_IDENTITY_STATE_READY = 1`

### `DEVICE_IDENTITY_STATE_NEEDS_REPAIR = 2`

## Enum `ServiceListenerExposure`

### `SERVICE_LISTENER_EXPOSURE_UNSPECIFIED = 0`

### `SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK = 1`

Any local user can connect using the owner's route authority.

### `SERVICE_LISTENER_EXPOSURE_LAN = 2`

### `SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET = 3`

## Enum `PersonalMeshPairingState`

### `PERSONAL_MESH_PAIRING_STATE_UNSPECIFIED = 0`

### `PERSONAL_MESH_PAIRING_STATE_INVITED = 1`

### `PERSONAL_MESH_PAIRING_STATE_HANDSHAKING = 2`

### `PERSONAL_MESH_PAIRING_STATE_AWAITING_CONFIRM = 3`

### `PERSONAL_MESH_PAIRING_STATE_READY_TO_COMMIT = 4`

### `PERSONAL_MESH_PAIRING_STATE_COMMITTED = 5`

### `PERSONAL_MESH_PAIRING_STATE_REJECTED = 6`

### `PERSONAL_MESH_PAIRING_STATE_EXPIRED = 7`

## Enum `PairingFailure`

### `PAIRING_FAILURE_UNSPECIFIED = 0`

### `PAIRING_FAILURE_CONTEXT_MISMATCH = 1`

### `PAIRING_FAILURE_TIMEOUT = 2`

### `PAIRING_FAILURE_AUTHENTICATION_FAILED = 3`

### `PAIRING_FAILURE_PROTOCOL_VIOLATION = 4`

### `PAIRING_FAILURE_CONNECTION_LOST = 5`
