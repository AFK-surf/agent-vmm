# `agentvmm.remote.v1`

Source: `remote/v1/remote.proto`.

## Service `RemoteControllerService`

RemoteControllerService is the provider-neutral controller endpoint for registration, command
delivery, and runtime tunnels.

### `Connect`

Connect exchanges current inventory, command authority, ordered commands, and terminal
delivery evidence.

`stream .agentvmm.remote.v1.ConnectRequest` → `stream .agentvmm.remote.v1.ConnectResponse`

### `ServeSession`

ServeSession carries a current allocation's framed runtime connection under its issued session
identity.

`stream .agentvmm.remote.v1.ServeSessionRequest` → `stream .agentvmm.remote.v1.ServeSessionResponse`

## Message `EndpointObservationRenewal`

EndpointObservationRenewal updates reachability under the current controller connection epoch.


## Message `RemoteObservationRenewal`

RemoteObservationRenewal updates a sampled Host observation without changing command authority.


## Message `HostHello`

HostHello offers current registration inventory, connection epoch, capacity, features, and
observation watermark.


## Message `RemoteObservationSnapshot`

RemoteObservationSnapshot reports bounded current health, usage, and inventory freshness rather
than stored-workload reservations.


## Message `RemoteObservationHealth`

RemoteObservationHealth reports the connector-visible health projection without granting
lifecycle authority.


## Message `RemoteObservationComponent`

RemoteObservationComponent names one observed component and its current health classification.


## Message `RemoteObservationUsage`

RemoteObservationUsage reports sampled active-workload usage and explicit stale or unavailable
state.


## Message `Capacity`

Capacity bounds one environment request and maximum egress policy without an aggregate
reservation ledger.


## Message `Allocation`

Allocation identifies a durable controller-owned environment and its generation, revision,
policy, and state.


## Message `ControllerReady`

ControllerReady acknowledges the exact Hello epoch and inventory watermark before command
admission.


## Message `ControllerCommand`

ControllerCommand binds one ordered command to its epoch, relative timeout, and target revision.


## Message `EnsureAllocation`

EnsureAllocation requests idempotent allocation identity with an exact policy contract and
generation.


## Message `ReleaseAllocation`

ReleaseAllocation retires the exact current allocation authority while preserving retained
private data.


## Message `DisposeAllocation`

DisposeAllocation requests disposal of an exact allocation under stable request and generation
fences.


## Message `PlanCleanupAllocation`

PlanCleanupAllocation reads reset or discard impacts without revoking an allocation's current
authority.


## Message `ExecuteCleanupAllocation`

ExecuteCleanupAllocation confirms a current cleanup revision and exact volume set before
deletion.


## Message `DrainRegistration`

DrainRegistration closes new allocation admission while preserving existing allocations and
sessions.


## Message `OpenSession`

OpenSession requests replacement runtime authority for the current allocation generation.


## Message `CommandResult`

CommandResult reports the command's terminal outcome with its original sequence, epoch, and
accepted resource facts.


## Message `CommandAck`

CommandAck acknowledges command admission separately from execution or terminal success.


## Message `CommandEvidence`

CommandEvidence reports command execution progress without replacing its final outcome.


## Message `CleanupPlan`

CleanupPlan binds remote cleanup impacts to the current allocation generation and plan revision.


## Message `SessionReady`

SessionReady issues the current runtime tunnel identity after previous session revocation
succeeds.


## Message `SessionHeader`

SessionHeader binds a tunnel to its current registration, allocation, session, and connection
authority.


## Message `ResourceLimits`

ResourceLimits contains remote PID and disk limits independent of the Host protobuf types.


## Message `CleanupImpact`

CleanupImpact describes one remote-visible cleanup effect without exposing Host environment
identifiers.


## Message `WorkloadReclaimCandidate`

WorkloadReclaimCandidate identifies a bounded reclaim suggestion without authorizing deletion.


## Message `ResourceReference`

ResourceReference identifies only the kind and resource ID forwarded to the remote controller.


## Message `ConnectRequest`

ConnectRequest carries Host handshake, observations, and command evidence for the
authenticated registration connection.


## Message `ConnectResponse`

ConnectResponse carries controller handshake, ordered commands, and current connection control
frames.


## Message `ServeSessionRequest`

ServeSessionRequest carries the exact session header before bounded runtime tunnel frames.


## Message `ServeSessionResponse`

ServeSessionResponse carries framed runtime tunnel responses under the established session
authority.


## Enum `CapacityDimension`

### `CAPACITY_DIMENSION_UNSPECIFIED = 0`

### `CAPACITY_DIMENSION_PIDS = 4`

### `CAPACITY_DIMENSION_WRITABLE = 5`

### `CAPACITY_DIMENSION_STORAGE_HEADROOM = 6`

### `CAPACITY_DIMENSION_IMPORT_SLOT = 7`

## Enum `CommandAckDisposition`

### `COMMAND_ACK_DISPOSITION_UNSPECIFIED = 0`

### `COMMAND_ACK_DISPOSITION_ADMITTED = 1`

### `COMMAND_ACK_DISPOSITION_REJECTED = 2`

### `COMMAND_ACK_DISPOSITION_DUPLICATE = 3`

## Enum `ExecutionStage`

### `EXECUTION_STAGE_UNSPECIFIED = 0`

### `EXECUTION_STAGE_STARTED = 1`

### `EXECUTION_STAGE_FINISHED = 2`

## Enum `CommandOutcome`

### `COMMAND_OUTCOME_UNSPECIFIED = 0`

### `COMMAND_OUTCOME_SUCCEEDED = 1`

### `COMMAND_OUTCOME_REJECTED = 2`

### `COMMAND_OUTCOME_UNKNOWN = 3`

## Enum `ErrorReason`

### `ERROR_REASON_UNSPECIFIED = 0`

### `ERROR_REASON_INVALID_ARGUMENT = 1`

### `ERROR_REASON_UNAUTHENTICATED = 2`

### `ERROR_REASON_VERSION_MISMATCH = 3`

### `ERROR_REASON_REGISTRATION_MISMATCH = 4`

### `ERROR_REASON_POLICY_CONFLICT = 5`

### `ERROR_REASON_CAPACITY_EXHAUSTED = 6`

### `ERROR_REASON_ALLOCATION_NOT_FOUND = 7`

### `ERROR_REASON_STALE_REVISION = 10`

### `ERROR_REASON_UNAVAILABLE = 11`

### `ERROR_REASON_INTERNAL = 12`

### `ERROR_REASON_STALE_CONNECTION_EPOCH = 13`

### `ERROR_REASON_SEQUENCE_CONFLICT = 14`

### `ERROR_REASON_DEADLINE_EXCEEDED = 15`

### `ERROR_REASON_FEATURE_MISMATCH = 16`

### `ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED = 17`

### `ERROR_REASON_ADMISSION_PAUSED = 18`

## Enum `EgressMode`

### `EGRESS_MODE_UNSPECIFIED = 0`

### `EGRESS_MODE_DENY_ALL = 1`

### `EGRESS_MODE_PUBLIC_INTERNET = 2`

## Enum `AllocationState`

### `ALLOCATION_STATE_UNSPECIFIED = 0`

### `ALLOCATION_STATE_READY = 1`

### `ALLOCATION_STATE_DRAINING = 3`

### `ALLOCATION_STATE_RETAINED = 4`

### `ALLOCATION_STATE_DISCARDED = 5`

## Enum `CleanupKind`

### `CLEANUP_KIND_UNSPECIFIED = 0`

### `CLEANUP_KIND_RESET = 1`

### `CLEANUP_KIND_DISCARD = 2`
