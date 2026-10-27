# Errors

Host and Guest errors use gRPC status codes and `google.rpc.ErrorInfo` with domain `agent-vmm`.
The `reason` string is the `ErrorReason` name without the `ERROR_REASON_` prefix.
The [generated catalogue](api/host-v1-errors.md) records each reason and its metadata keys.

## Retry and unknown outcomes

Read `RetryInfo` when present. Wait for its delay before a retry.
`Unavailable` can also permit a retry, but does not prove that a mutation failed before commit.
Keep the original request ID when you query or retry an operation with a stored receipt.
Do not create a new request ID merely because a connection closed.

`Aborted` requires the caller to inspect the conflict and refresh the relevant revision or instance.
`REQUEST_ID_CONFLICT` means that an existing ID belongs to a different request. Do not reuse that ID for changed input.
`RESOURCE_CAPACITY_EXHAUSTED` describes one request that exceeds its allowed capacity. Other allocations do not reserve that capacity.
Its `stage` metadata distinguishes `vm_capacity` from `pool_limit`.
Do not delete another allocation to satisfy that request.

A RemoteCompute mutation can report an unknown outcome after commit.
The connector preserves unknown outcomes for `Unavailable`, `DeadlineExceeded`, `Canceled`, `Unknown`, and `Internal`.
An attached reason does not turn those outcomes into a definitive rejection.
Pre-admission `VM_ADMISSION_CLOSED` and `REGISTRATION_DRAINING` use `FailedPrecondition` with `RetryInfo`.

The Host returns `admission=not_accepted` when the named mutation did not cross admission.
`operation_id` identifies an operation whose durable receipt can resolve the result.
`projection_applied` distinguishes a completed projection from a failure before that stage.
Resource details identify the affected object. Remote responses omit local environment details.
The MCP adapter does not infer reasons from message text. Unknown internal errors use a fixed public message.

## Reason handling

Use the next action below when no `RetryInfo` supplies a retry instruction.
Mutation retries must preserve receipt identity.

- Read current state and resolve the revision, instance, or request conflict. Reasons: `STALE_CONTAINER_INSTANCE`, `STALE_EXECUTION`, `CONTAINER_GENERATION_CONFLICT`, `LOG_CURSOR_STALE`, `PLAN_STALE`, `IMAGE_GENERATION_CONFLICT`, `IMAGE_DIGEST_CONFLICT`, `SERVICE_EXPORT_REVISION_CONFLICT`, `SERVICE_IMPORT_REVISION_CONFLICT`, `APPLIANCE_SERVICE_SOURCE_CHANGED`, `APPLIANCE_SERVICE_DESTINATION_CHANGED`, `CLEANUP_PLAN_STALE`, `REGISTRATION_REVISION_CONFLICT`, `DISPOSAL_REVISION_NOT_ACCEPTED`, `STALE_CONNECTION_EPOCH`, `STALE_RUNTIME_SESSION`.
- Resolve the stated limit or pressure. Follow RetryInfo when present. Reasons: `WORKLOAD_EXECUTION_BUSY`, `PID_LIMIT_EXCEEDED`, `RESOURCE_CAPACITY_EXHAUSTED`, `CLEANUP_INVENTORY_LIMIT`.
- Resolve the named state or configuration before retrying. Reasons: `LIFECYCLE_CONFLICT`, `WORKLOAD_STOP_UNRESOLVED`, `CONTAINER_NOT_RUNNING`, `CONTAINER_RUNNING`, `EXEC_START_FAILED`, `RUNTIME_PRECONDITION_FAILED`, `SOURCE_CONTAINER_INSTANCE_STALE`, `DESTINATION_CONTAINER_INSTANCE_STALE`, `IMAGE_IMPORT_REQUIRED`, `IMAGE_PLATFORM_UNAVAILABLE`, `IMAGE_UNPACK_FAILED`, `IMAGE_IN_USE`, `REMOTE_SERVICE_TRANSPORT_UNAVAILABLE`, `DESTINATION_ENDPOINT_STALE`, `PERSONAL_ROUTE_DESTINATION_INVALID`, `TRUST_UNAVAILABLE`, `VOLUME_CONFIRMATION_REQUIRED`, `AGENT_ACCESS_ACTIVE`, `REMOTE_ALLOCATION_ACTIVE`, `REMOTE_ALLOCATION_CONTROLLER_OWNED`.
- Inspect the gRPC code and durable operation receipt before retrying. Reasons: `CONTAINER_MUTATION_REJECTED`, `CONTAINER_CREATE_NOT_ADMITTED`, `CONTAINER_START_NOT_ADMITTED`, `DEVICE_IDENTITY_UNAVAILABLE`, `CONTENT_STAGE_NOT_ADMITTED`, `APPLIANCE_OPERATION_NOT_ADMITTED`, `WORKSPACE_GUEST_RELAY_FAILURE`, `CLEANUP_ADMISSION_TIMEOUT`, `CLEANUP_ADMISSION_CANCELED`, `LOCAL_DISPOSED`, `ALLOCATION_CONFLICT`.
- Correct the request. Reasons: `ENVIRONMENT_INVALID`, `IMAGE_REFERENCE_INVALID`.
- Read current inventory and select an existing resource. Reasons: `IMAGE_NOT_FOUND`, `IMAGE_CACHE_NOT_ADMITTED`, `SERVICE_EXPORT_NOT_FOUND`, `SERVICE_EXPORT_ENVIRONMENT_NOT_FOUND`, `SERVICE_IMPORT_NOT_FOUND`.
- Resolve availability, then retry with the same receipt identity. Reasons: `IMAGE_REGISTRY_UNAVAILABLE`, `IMAGE_REGISTRY_RATE_LIMITED`, `IMAGE_AUTH_UNAVAILABLE`, `IMAGE_DOWNLOAD_TIMEOUT`, `IMAGE_DOWNLOAD_INTERRUPTED`, `IMAGE_MANIFEST_FETCH_FAILED`, `SERVICE_PROJECTION_APPLY_FAILED`, `SERVICE_PROJECTION_DELETE_FAILED`, `SERVICE_ROUTE_APPLY_FAILED`, `PERSONAL_ROUTE_RESULT_UNKNOWN`, `SERVICE_STOP_PENDING`, `POLICY_PROJECTION_FAILED`, `SYSTEM_IMAGE_COLLECTION_DEFERRED`, `CLEANUP_ADMISSION_OUTCOME_UNKNOWN`, `CLEANUP_TERMINAL_PERSIST_FAILED`, `VM_ADMISSION_OUTCOME_UNKNOWN`, `VM_TERMINAL_PERSIST_FAILED`.
- Resolve authorization before another request. Reasons: `CROSS_DOMAIN_SERVICE_ROUTE`, `ROUTE_CAPABILITY_DENIED`, `PERSONAL_ROUTE_REQUEST_DENIED`, `POOL_EGRESS_LIMIT`.
- Wait for the RetryInfo delay. Preserve the request ID. Reasons: `VM_ADMISSION_CLOSED`, `REGISTRATION_DRAINING`.
- Use a new ID only for the changed request. Reasons: `REQUEST_ID_CONFLICT`.
