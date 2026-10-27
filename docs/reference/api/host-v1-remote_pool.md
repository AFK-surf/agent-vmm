# `agentvmm.host.v1`

Source: `host/v1/remote_pool.proto`.

## Message `ConnectorStatus`

ConnectorStatus observes transport recovery separately from registration admission state.


## Message `ControllerDomainMetrics`

ControllerDomainMetrics reports sampled traffic and session diagnostics for one registration
domain.


## Message `RemotePoolPolicy`

RemotePoolPolicy bounds each requested environment contract. It does not reserve aggregate
allocation capacity.


## Message `RemoteRegistration`

RemoteRegistration combines durable controller authority, admission policy, and separate
connector observations.


## Message `BeginRemoteEnrollmentRequest`

BeginRemoteEnrollmentRequest carries inputs for an operation that records one controller
registration and its approved scope before credential enrollment.


## Message `ApplyManagedRemoteEnrollmentRequest`

ApplyManagedRemoteEnrollmentRequest carries inputs for an operation that applies a managed
registration and one-time token within the local enrollment owner.


## Message `CompleteRemoteEnrollmentRequest`

CompleteRemoteEnrollmentRequest carries inputs for an operation that commits credential
completion for the current registration revision.


## Message `RetryRemoteEnrollmentRequest`

RetryRemoteEnrollmentRequest carries inputs for an operation that retries the existing
incomplete registration under its current revision.


## Message `BeginRemoteRevocationRequest`

BeginRemoteRevocationRequest carries inputs for an operation that records revocation intent
before stopping the registration's owned workload authority.


## Message `CompleteRemoteRevocationRequest`

CompleteRemoteRevocationRequest carries inputs for an operation that completes the selected
registration's durable revocation.


## Message `UpdateRemotePoolRequest`

UpdateRemotePoolRequest carries inputs for an operation that changes one registration's
allocation policy without reallocating existing workloads.


## Message `SetRemoteRegistrationStateRequest`

SetRemoteRegistrationStateRequest carries inputs for an operation that changes admission state
independently of connector transport connectivity.


## Message `GetRemoteRegistrationRequest`

GetRemoteRegistrationRequest carries inputs for an operation that reads the selected
registration, connector observation, and owned allocations.


## Message `RemoteAllocation`

RemoteAllocation identifies durable allocation ownership, generation, policy, and retained
state.


## Message `LocalRegistrationMetadata`

LocalRegistrationMetadata reports bounded local registration facts without contacting its
controller.


## Enum `RemoteRegistrationState`

### `REMOTE_REGISTRATION_STATE_UNSPECIFIED = 0`

### `REMOTE_REGISTRATION_STATE_ENABLED = 1`

### `REMOTE_REGISTRATION_STATE_DRAINING = 2`

### `REMOTE_REGISTRATION_STATE_REVOKED = 3`

## Enum `RemoteRegistrationOwnerKind`

### `REMOTE_REGISTRATION_OWNER_KIND_UNSPECIFIED = 0`

### `REMOTE_REGISTRATION_OWNER_KIND_EXTERNAL = 1`

### `REMOTE_REGISTRATION_OWNER_KIND_APPLIANCE = 2`

## Enum `RemoteConnectionState`

### `REMOTE_CONNECTION_STATE_UNSPECIFIED = 0`

### `REMOTE_CONNECTION_STATE_DISCONNECTED = 1`

### `REMOTE_CONNECTION_STATE_CONNECTED = 2`

## Enum `RemoteEnrollmentState`

### `REMOTE_ENROLLMENT_STATE_UNSPECIFIED = 0`

### `REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL = 1`

### `REMOTE_ENROLLMENT_STATE_READY = 2`

### `REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE = 3`

### `REMOTE_ENROLLMENT_STATE_REVOKING = 4`

## Enum `ConnectorPhase`

### `CONNECTOR_PHASE_UNSPECIFIED = 0`

### `CONNECTOR_PHASE_STOPPED = 1`

### `CONNECTOR_PHASE_ENROLLING = 2`

### `CONNECTOR_PHASE_CONNECTING = 3`

### `CONNECTOR_PHASE_CONNECTED = 4`

### `CONNECTOR_PHASE_RETRY_WAIT = 5`

### `CONNECTOR_PHASE_AUTH_FAILED = 6`

### `CONNECTOR_PHASE_CONFIG_ERROR = 7`

## Enum `AllocationState`

### `ALLOCATION_STATE_UNSPECIFIED = 0`

### `ALLOCATION_STATE_READY = 1`

### `ALLOCATION_STATE_DRAINING = 3`

### `ALLOCATION_STATE_RETAINED = 4`

### `ALLOCATION_STATE_DISCARDED = 5`
