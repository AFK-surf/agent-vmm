# `agentvmm.host.v1`

Source: `host/v1/errors.proto`.

## Enum `ErrorReason`

ErrorReason identifies a failure that changes caller handling.
Host RPCs attach RetryInfo to Unavailable and ResourceExhausted errors.
RetryInfo can also identify a transient pre-admission refusal.
Callers use RetryInfo or Unavailable to decide whether to retry.

### `ERROR_REASON_UNSPECIFIED = 0`

No ErrorInfo detail is emitted for this value.

### `ERROR_REASON_STALE_CONTAINER_INSTANCE = 1`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_STALE_EXECUTION = 2`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_WORKLOAD_EXECUTION_BUSY = 3`

Code: ResourceExhausted.
Metadata: none.

### `ERROR_REASON_LIFECYCLE_CONFLICT = 4`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_WORKLOAD_STOP_UNRESOLVED = 5`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_CONTAINER_MUTATION_REJECTED = 6`

Code: underlying failure code.
Metadata: operation_id.

### `ERROR_REASON_CONTAINER_CREATE_NOT_ADMITTED = 7`

Code: underlying failure code.
Metadata: admission.

### `ERROR_REASON_CONTAINER_START_NOT_ADMITTED = 8`

Code: underlying failure code.
Metadata: admission.

### `ERROR_REASON_CONTAINER_NOT_RUNNING = 9`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_CONTAINER_RUNNING = 10`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_CONTAINER_GENERATION_CONFLICT = 11`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_EXEC_START_FAILED = 12`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_PID_LIMIT_EXCEEDED = 13`

Code: ResourceExhausted.
Metadata: none.

### `ERROR_REASON_LOG_CURSOR_STALE = 14`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_ENVIRONMENT_INVALID = 15`

Code: InvalidArgument.
Metadata: none.

### `ERROR_REASON_RUNTIME_PRECONDITION_FAILED = 16`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_SOURCE_CONTAINER_INSTANCE_STALE = 17`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_DESTINATION_CONTAINER_INSTANCE_STALE = 18`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_PLAN_STALE = 19`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED = 20`

Code: ResourceExhausted.
Metadata: resource_dimension, stage; optional available_bytes, required_bytes.

### `ERROR_REASON_IMAGE_REFERENCE_INVALID = 21`

Code: InvalidArgument.
Metadata: none.

### `ERROR_REASON_IMAGE_IMPORT_REQUIRED = 22`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_IMAGE_NOT_FOUND = 23`

Code: NotFound.
Metadata: none.

### `ERROR_REASON_IMAGE_PLATFORM_UNAVAILABLE = 24`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_IMAGE_REGISTRY_UNAVAILABLE = 25`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_REGISTRY_RATE_LIMITED = 26`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_AUTH_UNAVAILABLE = 27`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_DOWNLOAD_TIMEOUT = 28`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_DOWNLOAD_INTERRUPTED = 29`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_MANIFEST_FETCH_FAILED = 30`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_IMAGE_UNPACK_FAILED = 31`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_IMAGE_IN_USE = 32`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_IMAGE_GENERATION_CONFLICT = 33`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_IMAGE_DIGEST_CONFLICT = 34`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_IMAGE_CACHE_NOT_ADMITTED = 35`

Code: NotFound.
Metadata: admission.

### `ERROR_REASON_SERVICE_EXPORT_NOT_FOUND = 36`

Code: NotFound.
Metadata: none.

### `ERROR_REASON_SERVICE_EXPORT_ENVIRONMENT_NOT_FOUND = 37`

Code: NotFound.
Metadata: none.

### `ERROR_REASON_SERVICE_EXPORT_REVISION_CONFLICT = 38`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_SERVICE_IMPORT_NOT_FOUND = 39`

Code: NotFound.
Metadata: none.

### `ERROR_REASON_SERVICE_IMPORT_REVISION_CONFLICT = 40`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_SERVICE_PROJECTION_APPLY_FAILED = 41`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_SERVICE_PROJECTION_DELETE_FAILED = 42`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_SERVICE_ROUTE_APPLY_FAILED = 43`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_CROSS_DOMAIN_SERVICE_ROUTE = 44`

Code: PermissionDenied.
Metadata: none.

### `ERROR_REASON_REMOTE_SERVICE_TRANSPORT_UNAVAILABLE = 45`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_DESTINATION_ENDPOINT_STALE = 46`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_ROUTE_CAPABILITY_DENIED = 47`

Code: PermissionDenied.
Metadata: none.

### `ERROR_REASON_PERSONAL_ROUTE_DESTINATION_INVALID = 48`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_PERSONAL_ROUTE_REQUEST_DENIED = 49`

Code: PermissionDenied.
Metadata: none.

### `ERROR_REASON_PERSONAL_ROUTE_RESULT_UNKNOWN = 50`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_DEVICE_IDENTITY_UNAVAILABLE = 51`

Code: FailedPrecondition, Unavailable.
Metadata: none.

### `ERROR_REASON_TRUST_UNAVAILABLE = 52`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_APPLIANCE_SERVICE_SOURCE_CHANGED = 53`

Code: Aborted.
Metadata: admission.

### `ERROR_REASON_APPLIANCE_SERVICE_DESTINATION_CHANGED = 54`

Code: Aborted.
Metadata: admission.

### `ERROR_REASON_CONTENT_STAGE_NOT_ADMITTED = 55`

Code: DeadlineExceeded.
Metadata: admission.

### `ERROR_REASON_APPLIANCE_OPERATION_NOT_ADMITTED = 56`

Code: DeadlineExceeded.
Metadata: admission.

### `ERROR_REASON_SERVICE_STOP_PENDING = 57`

Code: Unavailable.
Metadata: desired_running, actual_state.

### `ERROR_REASON_POLICY_PROJECTION_FAILED = 58`

Code: Unavailable.
Metadata: none.

### `ERROR_REASON_SYSTEM_IMAGE_COLLECTION_DEFERRED = 59`

Code: Unavailable.
Metadata: projection_applied.

### `ERROR_REASON_WORKSPACE_GUEST_RELAY_FAILURE = 60`

Code: underlying failure code.
Metadata: hop, phase.

### `ERROR_REASON_CLEANUP_PLAN_STALE = 61`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_CLEANUP_ADMISSION_TIMEOUT = 62`

Code: DeadlineExceeded.
Metadata: none.

### `ERROR_REASON_CLEANUP_ADMISSION_CANCELED = 63`

Code: Canceled.
Metadata: none.

### `ERROR_REASON_CLEANUP_ADMISSION_OUTCOME_UNKNOWN = 64`

Code: Unavailable.
Metadata: request_id.

### `ERROR_REASON_CLEANUP_TERMINAL_PERSIST_FAILED = 65`

Code: Unavailable.
Metadata: operation_id, request_id.

### `ERROR_REASON_CLEANUP_INVENTORY_LIMIT = 66`

Code: ResourceExhausted.
Metadata: none.

### `ERROR_REASON_VOLUME_CONFIRMATION_REQUIRED = 67`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_AGENT_ACCESS_ACTIVE = 68`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_REMOTE_ALLOCATION_ACTIVE = 69`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_REMOTE_ALLOCATION_CONTROLLER_OWNED = 70`

Code: FailedPrecondition.
Metadata: none.

### `ERROR_REASON_VM_ADMISSION_CLOSED = 71`

Code: FailedPrecondition. RetryInfo identifies the pre-admission retry.
Metadata: none.

### `ERROR_REASON_VM_ADMISSION_OUTCOME_UNKNOWN = 72`

Code: Unavailable.
Metadata: request_id.

### `ERROR_REASON_VM_TERMINAL_PERSIST_FAILED = 73`

Code: Unavailable.
Metadata: operation_id, request_id.

### `ERROR_REASON_LOCAL_DISPOSED = 74`

Code: FailedPrecondition, NotFound.
Metadata: none.

### `ERROR_REASON_REGISTRATION_REVISION_CONFLICT = 75`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_REGISTRATION_DRAINING = 76`

Code: FailedPrecondition. RetryInfo identifies the pre-admission retry.
Metadata: none.

### `ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED = 77`

Code: Aborted.
Metadata: allocation_id, generation, revision.

### `ERROR_REASON_STALE_CONNECTION_EPOCH = 78`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_ALLOCATION_CONFLICT = 79`

Code: AlreadyExists.
Metadata: none.

### `ERROR_REASON_POOL_EGRESS_LIMIT = 80`

Code: PermissionDenied.
Metadata: none.

### `ERROR_REASON_STALE_RUNTIME_SESSION = 81`

Code: Aborted.
Metadata: none.

### `ERROR_REASON_REQUEST_ID_CONFLICT = 82`

Code: Aborted.
Metadata: none.
