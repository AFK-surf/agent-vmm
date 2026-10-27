# `agentvmm.host.v1`

Source: `host/v1/resources.proto`.

## Message `WorkspaceEntry`

WorkspaceEntry describes one path relative to an authorized workspace root.


## Message `WriteWorkspaceHeader`

WriteWorkspaceHeader binds a write request to its target, offset, prefix digest, and owned
volume.


## Message `ExtractWorkspaceHeader`

ExtractWorkspaceHeader binds an archive extraction request to its owned destination.


## Message `VM`

VM reports VM lifecycle state and boot identity, not individual application readiness.


## Message `VMMutationFence`

VMMutationFence captures the observed whole-VM authority before an explicit start or stop.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `observed_workloads` | 3 | `repeated .agentvmm.host.v1.ResourceReference` | observed_workloads is bounded evidence, not an exhaustive authorization list. The whole-VM acknowledgement includes workloads added after this observation and workloads that are not individually enumerable. |

## Message `ResourceLimits`

ResourceLimits grants PID and writable disk limits. CPU and memory remain shared within Guest
capacity.


## Message `EnvironmentOwner`

EnvironmentOwner binds an environment to local ownership or an exact remote registration and
allocation.


## Message `AgentEnvironment`

AgentEnvironment combines durable environment policy with explicitly fresh or unavailable
runtime inventory.


## Message `AgentIntegration`

AgentIntegration records local client configuration intent independently of a runtime session's
lifetime.


## Message `AgentSession`

AgentSession reports a revocable environment-scoped runtime capability rather than workload
lifetime.


## Message `VolumeMount`

VolumeMount selects an owned volume or approved Host share without accepting a raw Host path.


## Message `Container`

Container combines durable configuration with reconciled process state and current instance
fences.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `start_request_instance_id` | 18 | `string` | Guest instance recorded by the most recent successful Start request. Retained after stop; this does not prove that the instance is running. |

## Message `StringList`

StringList preserves the distinction between an omitted command override and an explicitly empty
argument list.


## Message `Execution`

Execution identifies one instance-bound accepted execution whose settlement can outlive its
caller connection.


## Message `ImportImageHeader`

ImportImageHeader commits the immutable archive identity before URL download or local chunk
upload.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `archive_url` | 7 | `string` | Non-empty: public HTTPS download, followed by EOF. Empty: archive_chunk messages followed by EOF, with exact size and SHA256. Chunks are non-empty and at most 3 MiB. Archives are at most 8 GiB. |

## Message `ImageOperationReceipt`

ImageOperationReceipt keeps the business result separate from proof that all operation writers
settled.


## Message `Image`

Image describes a logical image grant and any content-bound build or copy receipt.


## Message `ExecStart`

ExecStart binds process arguments and execution identity to the expected container instance.


## Message `ExecResize`

ExecResize changes the terminal dimensions of an already accepted PTY execution.


## Message `Volume`

Volume reports an owned named volume and the environment disk limit that contains it.


## Message `ForwardTCPHeader`

ForwardTCPHeader selects the exact current container instance and TCP port before stream data.


## Message `BuildArgument`

BuildArgument supplies a Dockerfile argument. Secret values belong in BuildSecret instead.


## Message `BuildSecret`

BuildSecret names a secret whose bytes arrive through the separate bounded secret stream.


## Message `BuildInputFingerprint`

BuildInputFingerprint records the content and build options that determine idempotent build
identity.


## Message `BuildImageHeader`

BuildImageHeader binds one build to its request, source context, base image, and execution
fence.


## Message `BuildContextChunk`

BuildContextChunk carries context bytes and an explicit final marker before build execution.


## Message `BuildSecretChunk`

BuildSecretChunk carries one named secret with an explicit final marker.


## Message `AllocationImageSource`

AllocationImageSource selects an image source by allocation identity and generation.


## Message `RetainedImageSource`

RetainedImageSource selects retained controller image content through its original build
request.


## Message `ImageSource`

ImageSource selects either a current allocation source or authorized retained build content.


## Message `BuildImageResult`

BuildImageResult commits the resulting image reference and manifest digest after
materialization.


## Message `ComponentHealth`

ComponentHealth reports one component observation with its reason and observation time.


## Message `HealthStatus`

HealthStatus combines component observations without treating missing facts as healthy.


## Message `Usage`

Usage reports sampled actual usage and explicit stale or unavailable state.


## Message `DiskAccounting`

DiskAccounting keeps physical usage, commitments, and independent import and reserve accounting
distinguishable.


## Message `LayerResources`

LayerResources reports sampled resource facts for one implementation layer and preserves
unavailable observations.


## Message `ResourceReference`

ResourceReference identifies an owned object and its generation for impact or error reporting.


## Message `ContainerIDList`

ContainerIDList selects an explicit container set rather than all containers.


## Message `ContainerSelector`

ContainerSelector selects all current containers or one explicit set for an internal network
projection.


## Message `NetworkLink`

NetworkLink describes the Guest policy projection retained beneath service authorization. It has
no public mutation RPC.


## Message `CleanupImpact`

CleanupImpact states the exact resource effect and whether it blocks or irreversibly changes
cleanup.


## Message `CleanupPlan`

CleanupPlan binds one environment's reset or discard impacts to a current revision.


## Message `OperationError`

OperationError records a safe machine reason, message, and affected resource for a stored
operation.


## Message `Operation`

Operation retains accepted request identity and observed progress through a terminal or
interrupted result.


## Message `SupportBundleItem`

SupportBundleItem declares diagnostic sensitivity and default inclusion before export approval.


## Message `LocalDisposalPlan`

LocalDisposalPlan freezes one operator-selected local environment or complete registration
disposal scope.


## Message `LocalEnvironmentSummary`

LocalEnvironmentSummary reports bounded local owner facts while preserving partial or unknown
disk observations.


## Enum `WorkspaceEntryKind`

### `WORKSPACE_ENTRY_KIND_UNSPECIFIED = 0`

### `WORKSPACE_ENTRY_KIND_FILE = 1`

### `WORKSPACE_ENTRY_KIND_DIRECTORY = 2`

## Enum `EgressMode`

### `EGRESS_MODE_UNSPECIFIED = 0`

### `EGRESS_MODE_DENY_ALL = 1`

### `EGRESS_MODE_PUBLIC_INTERNET = 2`

## Enum `EnvironmentOwnerKind`

### `ENVIRONMENT_OWNER_KIND_UNSPECIFIED = 0`

### `ENVIRONMENT_OWNER_KIND_LOCAL = 1`

### `ENVIRONMENT_OWNER_KIND_REMOTE_REGISTRATION = 2`

## Enum `EnvironmentOrigin`

### `ENVIRONMENT_ORIGIN_UNSPECIFIED = 0`

### `ENVIRONMENT_ORIGIN_LOCAL = 1`

### `ENVIRONMENT_ORIGIN_REMOTE = 2`

## Enum `InventoryState`

### `INVENTORY_STATE_UNSPECIFIED = 0`

### `INVENTORY_STATE_AVAILABLE = 1`

### `INVENTORY_STATE_VM_STOPPED = 2`

### `INVENTORY_STATE_UNAVAILABLE = 3`

### `INVENTORY_STATE_STALE = 4`

## Enum `IntegrationScope`

### `INTEGRATION_SCOPE_UNSPECIFIED = 0`

### `INTEGRATION_SCOPE_USER = 1`

### `INTEGRATION_SCOPE_WORKSPACE = 2`

### `INTEGRATION_SCOPE_MANUAL = 3`

## Enum `IntegrationDesiredState`

### `INTEGRATION_DESIRED_STATE_UNSPECIFIED = 0`

### `INTEGRATION_DESIRED_STATE_ALLOWED = 1`

### `INTEGRATION_DESIRED_STATE_REMOVED = 2`

## Enum `IntegrationConfigState`

### `INTEGRATION_CONFIG_STATE_UNSPECIFIED = 0`

### `INTEGRATION_CONFIG_STATE_PENDING = 1`

### `INTEGRATION_CONFIG_STATE_READY = 2`

### `INTEGRATION_CONFIG_STATE_NEEDS_RESTART = 3`

### `INTEGRATION_CONFIG_STATE_CONFLICT = 4`

### `INTEGRATION_CONFIG_STATE_ERROR = 5`

### `INTEGRATION_CONFIG_STATE_REMOVING = 6`

### `INTEGRATION_CONFIG_STATE_REMOVED = 7`

### `INTEGRATION_CONFIG_STATE_CONFIGURING = 8`

## Enum `AgentSessionOrigin`

### `AGENT_SESSION_ORIGIN_UNSPECIFIED = 0`

### `AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION = 1`

### `AGENT_SESSION_ORIGIN_REMOTE_ALLOCATION = 2`

## Enum `AgentSessionState`

### `AGENT_SESSION_STATE_UNSPECIFIED = 0`

### `AGENT_SESSION_STATE_LISTENING = 1`

### `AGENT_SESSION_STATE_CONNECTED = 2`

### `AGENT_SESSION_STATE_REVOKED = 3`

### `AGENT_SESSION_STATE_EXPIRED = 4`

### `AGENT_SESSION_STATE_CLOSED = 5`

## Enum `ContainerState`

### `CONTAINER_STATE_UNSPECIFIED = 0`

### `CONTAINER_STATE_CREATED = 1`

### `CONTAINER_STATE_RUNNING = 2`

### `CONTAINER_STATE_STOPPED = 3`

### `CONTAINER_STATE_UNKNOWN = 4`

## Enum `ExecutionKind`

### `EXECUTION_KIND_UNSPECIFIED = 0`

### `EXECUTION_KIND_MAIN_EXECUTION = 1`

### `EXECUTION_KIND_AUTH_OPERATION = 2`

### `EXECUTION_KIND_MIGRATION_EXPORT = 3`

### `EXECUTION_KIND_MIGRATION_IMPORT = 4`

### `EXECUTION_KIND_EXEC = 5`

### `EXECUTION_KIND_PROCESS = 6`

### `EXECUTION_KIND_BUILD = 7`

## Enum `ExecutionState`

### `EXECUTION_STATE_UNSPECIFIED = 0`

### `EXECUTION_STATE_ACTIVE = 1`

### `EXECUTION_STATE_UNKNOWN = 2`

## Enum `ExecutionListStatus`

### `EXECUTION_LIST_STATUS_UNSPECIFIED = 0`

### `EXECUTION_LIST_STATUS_RECOVERING = 1`

### `EXECUTION_LIST_STATUS_NOT_FOUND = 2`

### `EXECUTION_LIST_STATUS_STALE = 3`

### `EXECUTION_LIST_STATUS_STOPPED = 4`

### `EXECUTION_LIST_STATUS_EMPTY = 5`

### `EXECUTION_LIST_STATUS_PRESENT = 6`

## Enum `ImageOrigin`

### `IMAGE_ORIGIN_CURRENT_ALLOCATION = 0`

Zero selects the caller's current allocation.
buf:lint:ignore ENUM_ZERO_VALUE_SUFFIX

### `IMAGE_ORIGIN_CONTROLLER_RETAINED = 1`

## Enum `LogStream`

### `LOG_STREAM_UNSPECIFIED = 0`

### `LOG_STREAM_STDOUT = 1`

### `LOG_STREAM_STDERR = 2`

## Enum `ProcessSignal`

### `PROCESS_SIGNAL_UNSPECIFIED = 0`

### `PROCESS_SIGNAL_TERM = 1`

### `PROCESS_SIGNAL_KILL = 2`

### `PROCESS_SIGNAL_INT = 3`

## Enum `BuildNetworkMode`

### `BUILD_NETWORK_MODE_UNSPECIFIED = 0`

### `BUILD_NETWORK_MODE_NONE = 1`

### `BUILD_NETWORK_MODE_NAMESPACE_EGRESS = 2`

## Enum `HealthState`

### `HEALTH_STATE_UNSPECIFIED = 0`

### `HEALTH_STATE_HEALTHY = 1`

### `HEALTH_STATE_DEGRADED = 2`

### `HEALTH_STATE_UNAVAILABLE = 3`

## Enum `CleanupKind`

### `CLEANUP_KIND_UNSPECIFIED = 0`

### `CLEANUP_KIND_RESET = 1`

### `CLEANUP_KIND_DISCARD = 2`

## Enum `OperationState`

### `OPERATION_STATE_UNSPECIFIED = 0`

### `OPERATION_STATE_RUNNING = 1`

### `OPERATION_STATE_SUCCEEDED = 2`

### `OPERATION_STATE_FAILED = 3`

### `OPERATION_STATE_INTERRUPTED = 4`

## Enum `LocalDisposalTargetKind`

### `LOCAL_DISPOSAL_TARGET_KIND_UNSPECIFIED = 0`

### `LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT = 1`

### `LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION = 2`

## Enum `ImageOperationKind`

### `IMAGE_OPERATION_KIND_UNSPECIFIED = 0`

### `IMAGE_OPERATION_KIND_BUILD = 1`

### `IMAGE_OPERATION_KIND_COPY = 2`

## Enum `ImageOperationState`

### `IMAGE_OPERATION_STATE_UNSPECIFIED = 0`

### `IMAGE_OPERATION_STATE_RUNNING = 1`

### `IMAGE_OPERATION_STATE_SUCCEEDED = 2`

### `IMAGE_OPERATION_STATE_FAILED = 3`

### `IMAGE_OPERATION_STATE_STOPPED = 4`

### `IMAGE_OPERATION_STATE_UNKNOWN = 5`

## Enum `VMState`

### `VM_STATE_UNSPECIFIED = 0`

### `VM_STATE_STOPPED = 1`

### `VM_STATE_STARTING = 2`

### `VM_STATE_RUNNING = 3`

### `VM_STATE_STOPPING = 4`

### `VM_STATE_FAILED = 5`
