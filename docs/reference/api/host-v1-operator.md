# `agentvmm.host.v1`

Source: `host/v1/operator.proto`.

## Service `OperatorService`

OperatorService is the trusted local operator surface for Host policy, observation, and explicit
destructive actions.

### `CreateMCPToken`

CreateMCPToken issues a token for the built-in MCP appliance.

`.agentvmm.host.v1.CreateMCPTokenRequest` → `.agentvmm.host.v1.CreateMCPTokenResponse`

### `RotateMCPTokenKey`

RotateMCPTokenKey starts an appliance update that replaces the MCP token signing key.

`.agentvmm.host.v1.RotateMCPTokenKeyRequest` → `.agentvmm.host.v1.RotateMCPTokenKeyResponse`

### `StartVM`

StartVM starts the shared VM with a whole-VM acknowledgement and the observed mutation fence.

`.agentvmm.host.v1.StartVMRequest` → `.agentvmm.host.v1.StartVMResponse`

### `GetVM`

GetVM reads current VM state without inferring individual workload readiness.

`.agentvmm.host.v1.GetVMRequest` → `.agentvmm.host.v1.GetVMResponse`

### `StopVM`

StopVM stops every workload in the shared VM under the supplied mutation fence.

`.agentvmm.host.v1.StopVMRequest` → `.agentvmm.host.v1.StopVMResponse`

### `CreateAgentSession`

CreateAgentSession issues a revocable runtime session fixed to the selected environment.

`.agentvmm.host.v1.CreateAgentSessionRequest` → `.agentvmm.host.v1.CreateAgentSessionResponse`

### `RevokeAgentSession`

RevokeAgentSession revokes one runtime capability without deleting its environment or private
data.

`.agentvmm.host.v1.RevokeAgentSessionRequest` → `.agentvmm.host.v1.RevokeAgentSessionResponse`

### `ListAgentIntegrations`

ListAgentIntegrations reads local integration intent and configuration state for operator
review.

`.agentvmm.host.v1.ListAgentIntegrationsRequest` → `.agentvmm.host.v1.ListAgentIntegrationsResponse`

### `PrepareAgentIntegration`

PrepareAgentIntegration creates pending integration intent or reuses its existing environment
without changing that environment's policy.

`.agentvmm.host.v1.PrepareAgentIntegrationRequest` → `.agentvmm.host.v1.PrepareAgentIntegrationResponse`

### `CommitAgentIntegration`

CommitAgentIntegration commits the configuration fingerprint at the current integration
revision.

`.agentvmm.host.v1.CommitAgentIntegrationRequest` → `.agentvmm.host.v1.CommitAgentIntegrationResponse`

### `BeginAgentIntegrationRemoval`

BeginAgentIntegrationRemoval revokes access and records removal intent for the exact
integration.

`.agentvmm.host.v1.BeginAgentIntegrationRemovalRequest` → `.agentvmm.host.v1.BeginAgentIntegrationRemovalResponse`

### `CompleteAgentIntegrationRemoval`

CompleteAgentIntegrationRemoval completes the recorded integration removal after its owned
cleanup succeeds.

`.agentvmm.host.v1.CompleteAgentIntegrationRemovalRequest` → `.agentvmm.host.v1.CompleteAgentIntegrationRemovalResponse`

### `ListAgentSessions`

ListAgentSessions reads session capability state separately from workload process state.

`.agentvmm.host.v1.ListAgentSessionsRequest` → `.agentvmm.host.v1.ListAgentSessionsResponse`

### `RevokeAgentSessions`

RevokeAgentSessions revokes the selected integration's runtime sessions without discarding its
environment.

`.agentvmm.host.v1.RevokeAgentSessionsRequest` → `.agentvmm.host.v1.RevokeAgentSessionsResponse`

### `GetSystemSnapshot`

GetSystemSnapshot reads operator inventory across Host-owned environments and registrations.

`.agentvmm.host.v1.GetSystemSnapshotRequest` → `.agentvmm.host.v1.GetSystemSnapshotResponse`

### `GetSystemHealthSummary`

GetSystemHealthSummary reads bounded Host and Guest health without enumerating workload
contents.

`.agentvmm.host.v1.GetSystemHealthSummaryRequest` → `.agentvmm.host.v1.GetSystemHealthSummaryResponse`

### `GetVMImpactSummary`

GetVMImpactSummary captures bounded owner references for a whole-VM decision, including
partial or overflowing observation state.

`.agentvmm.host.v1.GetVMImpactSummaryRequest` → `.agentvmm.host.v1.GetVMImpactSummaryResponse`

### `ListEnvironments`

ListEnvironments lists operator-visible environments with their logical ownership and
inventory freshness.

`.agentvmm.host.v1.ListEnvironmentsRequest` → `.agentvmm.host.v1.ListEnvironmentsResponse`

### `GetEnvironment`

GetEnvironment reads environment policy and observed runtime facts under the caller's
authority.

`.agentvmm.host.v1.OperatorServiceGetEnvironmentRequest` → `.agentvmm.host.v1.OperatorServiceGetEnvironmentResponse`

### `ListEnvironmentImages`

ListEnvironmentImages reads logical image grants in the selected operator-visible environment.

`.agentvmm.host.v1.ListEnvironmentImagesRequest` → `.agentvmm.host.v1.ListEnvironmentImagesResponse`

### `DeleteEnvironmentImage`

DeleteEnvironmentImage deletes one environment's image grant while preserving references owned
elsewhere.

`.agentvmm.host.v1.DeleteEnvironmentImageRequest` → `.agentvmm.host.v1.DeleteEnvironmentImageResponse`

### `WatchEvents`

WatchEvents streams Host events for observation without granting mutation authority.

`.agentvmm.host.v1.WatchEventsRequest` → `stream .agentvmm.host.v1.WatchEventsResponse`

### `WatchMetrics`

WatchMetrics streams sampled usage and freshness rather than reserved capacity.

`.agentvmm.host.v1.WatchMetricsRequest` → `stream .agentvmm.host.v1.WatchMetricsResponse`

### `Logs`

Logs reads bounded container output under the caller's environment authority.

`.agentvmm.host.v1.OperatorServiceLogsRequest` → `stream .agentvmm.host.v1.OperatorServiceLogsResponse`

### `StopContainer`

StopContainer stops the exact owned workload process under its current container fence.

`.agentvmm.host.v1.OperatorServiceStopContainerRequest` → `.agentvmm.host.v1.OperatorServiceStopContainerResponse`

### `ContinueContainer`

ContinueContainer resumes Host desired-running intent for the selected operator-visible
container.

`.agentvmm.host.v1.ContinueContainerRequest` → `.agentvmm.host.v1.ContinueContainerResponse`

### `SignalContainer`

SignalContainer signals the retained workload process, whose natural exit can race the
request.

`.agentvmm.host.v1.OperatorServiceSignalContainerRequest` → `.agentvmm.host.v1.OperatorServiceSignalContainerResponse`

### `DeleteContainer`

DeleteContainer deletes an owned stopped container without deleting its named volumes.

`.agentvmm.host.v1.OperatorServiceDeleteContainerRequest` → `.agentvmm.host.v1.OperatorServiceDeleteContainerResponse`

### `UpdateEnvironmentPolicy`

UpdateEnvironmentPolicy changes the environment's approved PID, disk, and egress policy at its
current revision.

`.agentvmm.host.v1.UpdateEnvironmentPolicyRequest` → `.agentvmm.host.v1.UpdateEnvironmentPolicyResponse`

### `PlanResetEnvironment`

PlanResetEnvironment reads a revision-bound cleanup plan that preserves named volumes.

`.agentvmm.host.v1.PlanResetEnvironmentRequest` → `.agentvmm.host.v1.PlanResetEnvironmentResponse`

### `PlanDiscardEnvironment`

PlanDiscardEnvironment reads a revision-bound plan for deleting the environment's exact
private resources.

`.agentvmm.host.v1.PlanDiscardEnvironmentRequest` → `.agentvmm.host.v1.PlanDiscardEnvironmentResponse`

### `ExecuteCleanup`

ExecuteCleanup accepts the reviewed cleanup revision and exact volume confirmations before
Guest side effects.

`.agentvmm.host.v1.ExecuteCleanupRequest` → `.agentvmm.host.v1.ExecuteCleanupResponse`

### `PlanLocalDisposal`

PlanLocalDisposal freezes one local environment or exact registration for trusted operator
disposal.

`.agentvmm.host.v1.PlanLocalDisposalRequest` → `.agentvmm.host.v1.PlanLocalDisposalResponse`

### `ExecuteLocalDisposal`

ExecuteLocalDisposal accepts confirmed local disposal and persists its replay fence before
resource deletion.

`.agentvmm.host.v1.ExecuteLocalDisposalRequest` → `.agentvmm.host.v1.ExecuteLocalDisposalResponse`

### `GetOperation`

GetOperation reads the stored result of the original Host operation.

`.agentvmm.host.v1.GetOperationRequest` → `.agentvmm.host.v1.GetOperationResponse`

### `RunDiagnostics`

RunDiagnostics collects bounded operator diagnostics with explicit sensitivity options.

`.agentvmm.host.v1.RunDiagnosticsRequest` → `.agentvmm.host.v1.RunDiagnosticsResponse`

### `PreviewSupportBundle`

PreviewSupportBundle lists diagnostic content and sensitivity before support bundle export.

`.agentvmm.host.v1.PreviewSupportBundleRequest` → `.agentvmm.host.v1.PreviewSupportBundleResponse`

### `ExportSupportBundle`

ExportSupportBundle exports only the operator-approved diagnostic scope.

`.agentvmm.host.v1.ExportSupportBundleRequest` → `.agentvmm.host.v1.ExportSupportBundleResponse`

### `BeginRemoteEnrollment`

BeginRemoteEnrollment records one controller registration and its approved scope before
credential enrollment.

`.agentvmm.host.v1.BeginRemoteEnrollmentRequest` → `.agentvmm.host.v1.BeginRemoteEnrollmentResponse`

### `ApplyManagedRemoteEnrollment`

ApplyManagedRemoteEnrollment applies a managed registration and one-time token within the
local enrollment owner.

`.agentvmm.host.v1.ApplyManagedRemoteEnrollmentRequest` → `.agentvmm.host.v1.ApplyManagedRemoteEnrollmentResponse`

### `CompleteRemoteEnrollment`

CompleteRemoteEnrollment commits credential completion for the current registration revision.

`.agentvmm.host.v1.CompleteRemoteEnrollmentRequest` → `.agentvmm.host.v1.CompleteRemoteEnrollmentResponse`

### `RetryRemoteEnrollment`

RetryRemoteEnrollment retries the existing incomplete registration under its current revision.

`.agentvmm.host.v1.RetryRemoteEnrollmentRequest` → `.agentvmm.host.v1.RetryRemoteEnrollmentResponse`

### `BeginRemoteRevocation`

BeginRemoteRevocation records revocation intent before stopping the registration's owned
workload authority.

`.agentvmm.host.v1.BeginRemoteRevocationRequest` → `.agentvmm.host.v1.BeginRemoteRevocationResponse`

### `CompleteRemoteRevocation`

CompleteRemoteRevocation completes the selected registration's durable revocation.

`.agentvmm.host.v1.CompleteRemoteRevocationRequest` → `.agentvmm.host.v1.CompleteRemoteRevocationResponse`

### `UpdateRemotePool`

UpdateRemotePool changes one registration's allocation policy without reallocating existing
workloads.

`.agentvmm.host.v1.UpdateRemotePoolRequest` → `.agentvmm.host.v1.UpdateRemotePoolResponse`

### `SetRemoteRegistrationState`

SetRemoteRegistrationState changes admission state independently of connector transport
connectivity.

`.agentvmm.host.v1.SetRemoteRegistrationStateRequest` → `.agentvmm.host.v1.SetRemoteRegistrationStateResponse`

### `GetRemoteRegistration`

GetRemoteRegistration reads the selected registration, connector observation, and owned
allocations.

`.agentvmm.host.v1.GetRemoteRegistrationRequest` → `.agentvmm.host.v1.GetRemoteRegistrationResponse`

### `SignRegistrationRecovery`

SignRegistrationRecovery signs only the bounded single-use registration-recovery challenge
using the current device identity.

`.agentvmm.host.v1.SignRegistrationRecoveryRequest` → `.agentvmm.host.v1.SignRegistrationRecoveryResponse`

### `GetLocalDiskSummary`

GetLocalDiskSummary reads Guest filesystem accounting without scanning every environment or
collecting compute metrics.

`.agentvmm.host.v1.GetLocalDiskSummaryRequest` → `.agentvmm.host.v1.GetLocalDiskSummaryResponse`

### `ListLocalEnvironmentPage`

ListLocalEnvironmentPage reads an indexed bounded page of local environment summaries.

`.agentvmm.host.v1.ListLocalEnvironmentPageRequest` → `.agentvmm.host.v1.ListLocalEnvironmentPageResponse`

### `ListLocalRegistrationPage`

ListLocalRegistrationPage reads an indexed bounded page of local registration metadata.

`.agentvmm.host.v1.ListLocalRegistrationPageRequest` → `.agentvmm.host.v1.ListLocalRegistrationPageResponse`

### `GetDeviceIdentity`

GetDeviceIdentity reads public device identity and custody state without exposing private key
bytes.

`.agentvmm.host.v1.GetDeviceIdentityRequest` → `.agentvmm.host.v1.GetDeviceIdentityResponse`

### `ListPersonalMeshes`

ListPersonalMeshes reads the local projection of verified personal mesh state.

`.agentvmm.host.v1.ListPersonalMeshesRequest` → `.agentvmm.host.v1.ListPersonalMeshesResponse`

### `CreatePersonalMesh`

CreatePersonalMesh creates personal mesh intent rooted in the current device identity.

`.agentvmm.host.v1.CreatePersonalMeshRequest` → `.agentvmm.host.v1.CreatePersonalMeshResponse`

### `RevokePersonalMeshMember`

RevokePersonalMeshMember queues the signed removal of one member under the current mesh
revision.

`.agentvmm.host.v1.RevokePersonalMeshMemberRequest` → `.agentvmm.host.v1.RevokePersonalMeshMemberResponse`

### `BeginPersonalMeshPairing`

BeginPersonalMeshPairing creates a bounded pairing invitation without granting membership
before confirmation.

`.agentvmm.host.v1.BeginPersonalMeshPairingRequest` → `.agentvmm.host.v1.BeginPersonalMeshPairingResponse`

### `JoinPersonalMeshPairing`

JoinPersonalMeshPairing joins the selected pairing exchange using its separate PAKE secret.

`.agentvmm.host.v1.JoinPersonalMeshPairingRequest` → `.agentvmm.host.v1.JoinPersonalMeshPairingResponse`

### `GetPersonalMeshPairing`

GetPersonalMeshPairing reads pairing progress and its confirmation state.

`.agentvmm.host.v1.GetPersonalMeshPairingRequest` → `.agentvmm.host.v1.GetPersonalMeshPairingResponse`

### `ConfirmPersonalMeshPairing`

ConfirmPersonalMeshPairing confirms the observed pairing identity before membership
publication.

`.agentvmm.host.v1.ConfirmPersonalMeshPairingRequest` → `.agentvmm.host.v1.ConfirmPersonalMeshPairingResponse`

### `RejectPersonalMeshPairing`

RejectPersonalMeshPairing rejects the exact pairing exchange and its pending authority.

`.agentvmm.host.v1.RejectPersonalMeshPairingRequest` → `.agentvmm.host.v1.RejectPersonalMeshPairingResponse`

### `RepairDeviceIdentity`

RepairDeviceIdentity replaces unavailable identity through explicit membership reset while
preserving the committed winner of concurrent repair.

`.agentvmm.host.v1.RepairDeviceIdentityRequest` → `.agentvmm.host.v1.RepairDeviceIdentityResponse`

### `CreateLocalServiceListener`

CreateLocalServiceListener creates an operator-approved local listener for a current service
route.

`.agentvmm.host.v1.CreateLocalServiceListenerRequest` → `.agentvmm.host.v1.CreateLocalServiceListenerResponse`

### `ListServiceListeners`

ListServiceListeners reads active and revoked listener facts without reconstructing a live
listener after restart.

`.agentvmm.host.v1.ListServiceListenersRequest` → `.agentvmm.host.v1.ListServiceListenersResponse`

### `RevokeServiceListener`

RevokeServiceListener closes the exact listener and revokes its route authority.

`.agentvmm.host.v1.RevokeServiceListenerRequest` → `.agentvmm.host.v1.RevokeServiceListenerResponse`

### `CreateHostShare`

CreateHostShare approves one checked Host directory for the selected environment.

`.agentvmm.host.v1.CreateHostShareRequest` → `.agentvmm.host.v1.CreateHostShareResponse`

### `RebindHostShare`

RebindHostShare changes the checked source of an existing share under its revision fence.

`.agentvmm.host.v1.RebindHostShareRequest` → `.agentvmm.host.v1.RebindHostShareResponse`

### `RevokeHostShare`

RevokeHostShare revokes one share rather than granting general Host filesystem access.

`.agentvmm.host.v1.RevokeHostShareRequest` → `.agentvmm.host.v1.RevokeHostShareResponse`

### `ListHostShares`

ListHostShares reads operator-approved directory shares and their current references.

`.agentvmm.host.v1.ListHostSharesRequest` → `.agentvmm.host.v1.ListHostSharesResponse`

## Message `GetDeviceIdentityRequest`

GetDeviceIdentityRequest carries inputs for an operation that reads public device identity and
custody state without exposing private key bytes.


## Message `CreatePersonalMeshRequest`

CreatePersonalMeshRequest carries inputs for an operation that creates personal mesh intent
rooted in the current device identity.


## Message `ListPersonalMeshesRequest`

ListPersonalMeshesRequest carries inputs for an operation that reads the local projection of
verified personal mesh state.


## Message `ListPersonalMeshesResponse`

ListPersonalMeshesResponse carries results from an operation that reads the local projection
of verified personal mesh state.


## Message `CreateLocalServiceListenerRequest`

CreateLocalServiceListenerRequest carries inputs for an operation that creates an operator-
approved local listener for a current service route.


## Message `ListServiceListenersRequest`

ListServiceListenersRequest carries inputs for an operation that reads active and revoked
listener facts without reconstructing a live listener after restart.


## Message `ListServiceListenersResponse`

ListServiceListenersResponse carries results from an operation that reads active and revoked
listener facts without reconstructing a live listener after restart.


## Message `RevokeServiceListenerRequest`

RevokeServiceListenerRequest carries inputs for an operation that closes the exact listener
and revokes its route authority.


## Message `RevokePersonalMeshMemberRequest`

RevokePersonalMeshMemberRequest carries inputs for an operation that queues the signed removal
of one member under the current mesh revision.


## Message `BeginPersonalMeshPairingRequest`

BeginPersonalMeshPairingRequest carries inputs for an operation that creates a bounded pairing
invitation without granting membership before confirmation.


## Message `JoinPersonalMeshPairingRequest`

JoinPersonalMeshPairingRequest carries inputs for an operation that joins the selected pairing
exchange using its separate PAKE secret.


## Message `GetPersonalMeshPairingRequest`

GetPersonalMeshPairingRequest carries inputs for an operation that reads pairing progress and
its confirmation state.


## Message `ConfirmPersonalMeshPairingRequest`

ConfirmPersonalMeshPairingRequest carries inputs for an operation that confirms the observed
pairing identity before membership publication.


## Message `RejectPersonalMeshPairingRequest`

RejectPersonalMeshPairingRequest carries inputs for an operation that rejects the exact
pairing exchange and its pending authority.


## Message `RepairDeviceIdentityRequest`

RepairDeviceIdentityRequest carries inputs for an operation that replaces unavailable identity
through explicit membership reset while preserving the committed winner of concurrent repair.


## Message `StartVMRequest`

StartVMRequest carries a stable request identity and explicit acknowledgement of the complete
VM impact.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `fence` | 2 | `.agentvmm.host.v1.VMMutationFence` | fence is produced by the same Host's VM impact summary. The VM lifecycle owner validates available owner revisions immediately before mutation. |

## Message `GetVMRequest`

GetVMRequest carries inputs for an operation that reads current VM state without inferring
individual workload readiness.


## Message `StopVMRequest`

StopVMRequest carries the observed whole-VM fence and stable request identity, including
optional forced-stop approval.


## Message `CreateAgentSessionRequest`

CreateAgentSessionRequest carries inputs for an operation that issues a revocable runtime
session fixed to the selected environment.


## Message `CreateAgentSessionResponse`

CreateAgentSessionResponse carries results from an operation that issues a revocable runtime
session fixed to the selected environment.


## Message `RevokeAgentSessionRequest`

RevokeAgentSessionRequest carries inputs for an operation that revokes one runtime capability
without deleting its environment or private data.


## Message `ListAgentIntegrationsRequest`

ListAgentIntegrationsRequest carries inputs for an operation that reads local integration
intent and configuration state for operator review.


## Message `ListAgentIntegrationsResponse`

ListAgentIntegrationsResponse carries results from an operation that reads local integration
intent and configuration state for operator review.


## Message `PrepareAgentIntegrationRequest`

PrepareAgentIntegrationRequest carries inputs for an operation that creates pending
integration intent or reuses its existing environment without changing that environment's
policy.


## Message `CommitAgentIntegrationRequest`

CommitAgentIntegrationRequest carries inputs for an operation that commits the configuration
fingerprint at the current integration revision.


## Message `BeginAgentIntegrationRemovalRequest`

BeginAgentIntegrationRemovalRequest carries inputs for an operation that revokes access and
records removal intent for the exact integration.


## Message `CompleteAgentIntegrationRemovalRequest`

CompleteAgentIntegrationRemovalRequest carries inputs for an operation that completes the
recorded integration removal after its owned cleanup succeeds.


## Message `ListAgentSessionsRequest`

ListAgentSessionsRequest carries inputs for an operation that reads session capability state
separately from workload process state.


## Message `ListAgentSessionsResponse`

ListAgentSessionsResponse carries results from an operation that reads session capability
state separately from workload process state.


## Message `RevokeAgentSessionsRequest`

RevokeAgentSessionsRequest carries inputs for an operation that revokes the selected
integration's runtime sessions without discarding its environment.


## Message `ListEnvironmentsRequest`

ListEnvironmentsRequest carries inputs for an operation that lists operator-visible
environments with their logical ownership and inventory freshness.


## Message `ListEnvironmentsResponse`

ListEnvironmentsResponse carries results from an operation that lists operator-visible
environments with their logical ownership and inventory freshness.


## Message `ListEnvironmentImagesRequest`

ListEnvironmentImagesRequest carries inputs for an operation that reads logical image grants
in the selected operator-visible environment.


## Message `DeleteEnvironmentImageRequest`

DeleteEnvironmentImageRequest carries inputs for an operation that deletes one environment's
image grant while preserving references owned elsewhere.


## Message `GetSystemSnapshotRequest`

GetSystemSnapshotRequest carries inputs for an operation that reads operator inventory across
Host-owned environments and registrations.


## Message `GetSystemHealthSummaryRequest`

GetSystemHealthSummaryRequest carries inputs for an operation that reads bounded Host and
Guest health without enumerating workload contents.


## Message `GetVMImpactSummaryRequest`

GetVMImpactSummaryRequest carries inputs for an operation that captures bounded owner
references for a whole-VM decision, including partial or overflowing observation state.


## Message `GetVMImpactSummaryResponse`

GetVMImpactSummaryResponse carries results from an operation that captures bounded owner
references for a whole-VM decision, including partial or overflowing observation state.

| Field | Number | Type | Description |
| --- | --- | --- | --- |
| `complete` | 3 | `bool` | complete covers the bounded Host owner reads used for this summary. It does not mean that every Guest workload, lease, session, or system appliance has an individual entry in known_workloads. |
| `known_workloads` | 6 | `repeated .agentvmm.host.v1.ResourceReference` | known_workloads contains at most 100 Host-owned environment and registration references. The authorized impact_scope is always whole_vm. |

## Message `WatchEventsRequest`

WatchEventsRequest carries inputs for an operation that streams Host events for observation
without granting mutation authority.


## Message `WatchMetricsRequest`

WatchMetricsRequest carries inputs for an operation that streams sampled usage and freshness
rather than reserved capacity.


## Message `UpdateEnvironmentPolicyRequest`

UpdateEnvironmentPolicyRequest carries inputs for an operation that changes the environment's
approved PID, disk, and egress policy at its current revision.


## Message `ExecuteCleanupRequest`

ExecuteCleanupRequest carries the reviewed plan revision and exact volume confirmations.
Transport uncertainty requires the same request identity.


## Message `GetOperationRequest`

GetOperationRequest carries inputs for an operation that reads the stored result of the
original Host operation.


## Message `RunDiagnosticsRequest`

RunDiagnosticsRequest carries inputs for an operation that collects bounded operator
diagnostics with explicit sensitivity options.


## Message `PreviewSupportBundleRequest`

PreviewSupportBundleRequest carries inputs for an operation that lists diagnostic content and
sensitivity before support bundle export.


## Message `ExportSupportBundleRequest`

ExportSupportBundleRequest carries inputs for an operation that exports only the operator-
approved diagnostic scope.


## Message `PlanLocalDisposalRequest`

PlanLocalDisposalRequest carries inputs for an operation that freezes one local environment or
exact registration for trusted operator disposal.


## Message `ExecuteLocalDisposalRequest`

ExecuteLocalDisposalRequest carries inputs for an operation that accepts confirmed local
disposal and persists its replay fence before resource deletion.


## Message `SignRegistrationRecoveryRequest`

SignRegistrationRecoveryRequest carries inputs for an operation that signs only the bounded
single-use registration-recovery challenge using the current device identity.


## Message `SignRegistrationRecoveryResponse`

SignRegistrationRecoveryResponse carries results from an operation that signs only the bounded
single-use registration-recovery challenge using the current device identity.


## Message `GetLocalDiskSummaryRequest`

GetLocalDiskSummaryRequest carries inputs for an operation that reads Guest filesystem
accounting without scanning every environment or collecting compute metrics.


## Message `ListLocalEnvironmentPageRequest`

ListLocalEnvironmentPageRequest carries inputs for an operation that reads an indexed bounded
page of local environment summaries.


## Message `ListLocalRegistrationPageRequest`

ListLocalRegistrationPageRequest carries inputs for an operation that reads an indexed bounded
page of local registration metadata.


## Message `StartVMResponse`

StartVMResponse carries results from an operation that starts the shared VM with a whole-VM
acknowledgement and the observed mutation fence.


## Message `GetVMResponse`

GetVMResponse carries results from an operation that reads current VM state without inferring
individual workload readiness.


## Message `StopVMResponse`

StopVMResponse carries results from an operation that stops every workload in the shared VM
under the supplied mutation fence.


## Message `RevokeAgentSessionResponse`

RevokeAgentSessionResponse carries results from an operation that revokes one runtime
capability without deleting its environment or private data.


## Message `PrepareAgentIntegrationResponse`

PrepareAgentIntegrationResponse carries results from an operation that creates pending
integration intent or reuses its existing environment without changing that environment's
policy.


## Message `CommitAgentIntegrationResponse`

CommitAgentIntegrationResponse carries results from an operation that commits the
configuration fingerprint at the current integration revision.


## Message `BeginAgentIntegrationRemovalResponse`

BeginAgentIntegrationRemovalResponse carries results from an operation that revokes access and
records removal intent for the exact integration.


## Message `CompleteAgentIntegrationRemovalResponse`

CompleteAgentIntegrationRemovalResponse carries results from an operation that completes the
recorded integration removal after its owned cleanup succeeds.


## Message `RevokeAgentSessionsResponse`

RevokeAgentSessionsResponse carries results from an operation that revokes the selected
integration's runtime sessions without discarding its environment.


## Message `GetSystemHealthSummaryResponse`

GetSystemHealthSummaryResponse carries results from an operation that reads bounded Host and
Guest health without enumerating workload contents.


## Message `OperatorServiceGetEnvironmentRequest`

OperatorServiceGetEnvironmentRequest carries inputs for an operation that reads environment
policy and observed runtime facts under the caller's authority.


## Message `OperatorServiceGetEnvironmentResponse`

OperatorServiceGetEnvironmentResponse carries results from an operation that reads environment
policy and observed runtime facts under the caller's authority.


## Message `ListEnvironmentImagesResponse`

ListEnvironmentImagesResponse carries results from an operation that reads logical image
grants in the selected operator-visible environment.


## Message `DeleteEnvironmentImageResponse`

DeleteEnvironmentImageResponse carries results from an operation that deletes one
environment's image grant while preserving references owned elsewhere.


## Message `WatchEventsResponse`

WatchEventsResponse carries results from an operation that streams Host events for observation
without granting mutation authority.


## Message `WatchMetricsResponse`

WatchMetricsResponse carries results from an operation that streams sampled usage and
freshness rather than reserved capacity.


## Message `OperatorServiceLogsRequest`

OperatorServiceLogsRequest carries inputs for an operation that reads bounded container output
under the caller's environment authority.


## Message `OperatorServiceLogsResponse`

OperatorServiceLogsResponse carries results from an operation that reads bounded container
output under the caller's environment authority.


## Message `OperatorServiceStopContainerRequest`

OperatorServiceStopContainerRequest carries inputs for an operation that stops the exact owned
workload process under its current container fence.


## Message `OperatorServiceStopContainerResponse`

OperatorServiceStopContainerResponse carries results from an operation that stops the exact
owned workload process under its current container fence.


## Message `ContinueContainerRequest`

ContinueContainerRequest carries inputs for an operation that resumes Host desired-running
intent for the selected operator-visible container.


## Message `ContinueContainerResponse`

ContinueContainerResponse carries results from an operation that resumes Host desired-running
intent for the selected operator-visible container.


## Message `OperatorServiceSignalContainerRequest`

OperatorServiceSignalContainerRequest carries inputs for an operation that signals the
retained workload process, whose natural exit can race the request.


## Message `OperatorServiceSignalContainerResponse`

OperatorServiceSignalContainerResponse carries results from an operation that signals the
retained workload process, whose natural exit can race the request.


## Message `OperatorServiceDeleteContainerRequest`

OperatorServiceDeleteContainerRequest carries inputs for an operation that deletes an owned
stopped container without deleting its named volumes.


## Message `OperatorServiceDeleteContainerResponse`

OperatorServiceDeleteContainerResponse carries results from an operation that deletes an owned
stopped container without deleting its named volumes.


## Message `UpdateEnvironmentPolicyResponse`

UpdateEnvironmentPolicyResponse carries results from an operation that changes the
environment's approved PID, disk, and egress policy at its current revision.


## Message `PlanResetEnvironmentRequest`

PlanResetEnvironmentRequest carries inputs for an operation that reads a revision-bound
cleanup plan that preserves named volumes.


## Message `PlanResetEnvironmentResponse`

PlanResetEnvironmentResponse carries results from an operation that reads a revision-bound
cleanup plan that preserves named volumes.


## Message `PlanDiscardEnvironmentRequest`

PlanDiscardEnvironmentRequest carries inputs for an operation that reads a revision-bound plan
for deleting the environment's exact private resources.


## Message `PlanDiscardEnvironmentResponse`

PlanDiscardEnvironmentResponse carries results from an operation that reads a revision-bound
plan for deleting the environment's exact private resources.


## Message `ExecuteCleanupResponse`

ExecuteCleanupResponse carries results from an operation that accepts the reviewed cleanup
revision and exact volume confirmations before Guest side effects.


## Message `PlanLocalDisposalResponse`

PlanLocalDisposalResponse carries results from an operation that freezes one local environment
or exact registration for trusted operator disposal.


## Message `ExecuteLocalDisposalResponse`

ExecuteLocalDisposalResponse carries results from an operation that accepts confirmed local
disposal and persists its replay fence before resource deletion.


## Message `GetOperationResponse`

GetOperationResponse carries results from an operation that reads the stored result of the
original Host operation.


## Message `RunDiagnosticsResponse`

RunDiagnosticsResponse carries results from an operation that collects bounded operator
diagnostics with explicit sensitivity options.


## Message `PreviewSupportBundleResponse`

PreviewSupportBundleResponse carries results from an operation that lists diagnostic content
and sensitivity before support bundle export.


## Message `ExportSupportBundleResponse`

ExportSupportBundleResponse carries results from an operation that exports only the operator-
approved diagnostic scope.


## Message `BeginRemoteEnrollmentResponse`

BeginRemoteEnrollmentResponse carries results from an operation that records one controller
registration and its approved scope before credential enrollment.


## Message `ApplyManagedRemoteEnrollmentResponse`

ApplyManagedRemoteEnrollmentResponse carries results from an operation that applies a managed
registration and one-time token within the local enrollment owner.


## Message `CompleteRemoteEnrollmentResponse`

CompleteRemoteEnrollmentResponse carries results from an operation that commits credential
completion for the current registration revision.


## Message `RetryRemoteEnrollmentResponse`

RetryRemoteEnrollmentResponse carries results from an operation that retries the existing
incomplete registration under its current revision.


## Message `CompleteRemoteRevocationResponse`

CompleteRemoteRevocationResponse carries results from an operation that completes the selected
registration's durable revocation.


## Message `UpdateRemotePoolResponse`

UpdateRemotePoolResponse carries results from an operation that changes one registration's
allocation policy without reallocating existing workloads.


## Message `SetRemoteRegistrationStateResponse`

SetRemoteRegistrationStateResponse carries results from an operation that changes admission
state independently of connector transport connectivity.


## Message `GetRemoteRegistrationResponse`

GetRemoteRegistrationResponse carries results from an operation that reads the selected
registration, connector observation, and owned allocations.


## Message `GetLocalDiskSummaryResponse`

GetLocalDiskSummaryResponse carries results from an operation that reads Guest filesystem
accounting without scanning every environment or collecting compute metrics.


## Message `ListLocalEnvironmentPageResponse`

ListLocalEnvironmentPageResponse carries results from an operation that reads an indexed
bounded page of local environment summaries.


## Message `ListLocalRegistrationPageResponse`

ListLocalRegistrationPageResponse carries results from an operation that reads an indexed
bounded page of local registration metadata.


## Message `GetDeviceIdentityResponse`

GetDeviceIdentityResponse carries results from an operation that reads public device identity
and custody state without exposing private key bytes.


## Message `CreatePersonalMeshResponse`

CreatePersonalMeshResponse carries results from an operation that creates personal mesh intent
rooted in the current device identity.


## Message `RevokePersonalMeshMemberResponse`

RevokePersonalMeshMemberResponse carries results from an operation that queues the signed
removal of one member under the current mesh revision.


## Message `BeginPersonalMeshPairingResponse`

BeginPersonalMeshPairingResponse carries results from an operation that creates a bounded
pairing invitation without granting membership before confirmation.


## Message `JoinPersonalMeshPairingResponse`

JoinPersonalMeshPairingResponse carries results from an operation that joins the selected
pairing exchange using its separate PAKE secret.


## Message `GetPersonalMeshPairingResponse`

GetPersonalMeshPairingResponse carries results from an operation that reads pairing progress
and its confirmation state.


## Message `ConfirmPersonalMeshPairingResponse`

ConfirmPersonalMeshPairingResponse carries results from an operation that confirms the
observed pairing identity before membership publication.


## Message `RejectPersonalMeshPairingResponse`

RejectPersonalMeshPairingResponse carries results from an operation that rejects the exact
pairing exchange and its pending authority.


## Message `RepairDeviceIdentityResponse`

RepairDeviceIdentityResponse carries results from an operation that replaces unavailable
identity through explicit membership reset while preserving the committed winner of concurrent
repair.


## Message `CreateLocalServiceListenerResponse`

CreateLocalServiceListenerResponse carries results from an operation that creates an operator-
approved local listener for a current service route.


## Message `RevokeServiceListenerResponse`

RevokeServiceListenerResponse carries results from an operation that closes the exact listener
and revokes its route authority.


## Message `CreateHostShareResponse`

CreateHostShareResponse carries results from an operation that approves one checked Host
directory for the selected environment.


## Message `RebindHostShareResponse`

RebindHostShareResponse carries results from an operation that changes the checked source of
an existing share under its revision fence.


## Message `RevokeHostShareResponse`

RevokeHostShareResponse carries results from an operation that revokes one share rather than
granting general Host filesystem access.


## Message `GetSystemSnapshotResponse`

GetSystemSnapshotResponse carries results from an operation that reads operator inventory
across Host-owned environments and registrations.


## Message `BeginRemoteRevocationResponse`

BeginRemoteRevocationResponse carries results from an operation that records revocation intent
before stopping the registration's owned workload authority.


## Message `CreateMCPTokenRequest`

CreateMCPTokenRequest selects the built-in MCP appliance token issuer.


## Message `CreateMCPTokenResponse`

CreateMCPTokenResponse returns the issued bearer token.


## Message `RotateMCPTokenKeyRequest`

RotateMCPTokenKeyRequest selects the built-in MCP appliance signing key.


## Message `RotateMCPTokenKeyResponse`

RotateMCPTokenKeyResponse identifies the appliance update that replaces the signing key.
