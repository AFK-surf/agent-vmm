# `agentvmm.appliance.v1`

Source: `appliance/v1/appliance.proto`.

## Service `ApplianceAdminService`

ApplianceAdminService is the operator surface for staged artifacts, approved grants, and durable
installation lifecycle.

### `CreateApplianceServiceImport`

CreateApplianceServiceImport approves service access from the current installation's private
controller domain.

`.agentvmm.appliance.v1.CreateApplianceServiceImportRequest` → `.agentvmm.appliance.v1.CreateApplianceServiceImportResponse`

### `CreateApplianceFrontendServiceImport`

CreateApplianceFrontendServiceImport approves a workload service as the selected static
appliance frontend target.

`.agentvmm.appliance.v1.CreateApplianceFrontendServiceImportRequest` → `.agentvmm.appliance.v1.CreateApplianceFrontendServiceImportResponse`

### `ListApplianceServiceImports`

ListApplianceServiceImports reads only imports owned by the selected installation.

`.agentvmm.appliance.v1.ListApplianceServiceImportsRequest` → `.agentvmm.appliance.v1.ListApplianceServiceImportsResponse`

### `RevokeApplianceServiceImport`

RevokeApplianceServiceImport revokes the exact installation-owned service import revision.

`.agentvmm.appliance.v1.RevokeApplianceServiceImportRequest` → `.agentvmm.appliance.v1.RevokeApplianceServiceImportResponse`

### `BuildArtifact`

BuildArtifact builds an appliance archive and streams its bytes with a final size and digest
commitment.

`stream .agentvmm.appliance.v1.BuildArtifactRequest` → `stream .agentvmm.appliance.v1.BuildArtifactResponse`

### `StageContent`

StageContent stages one content-addressed archive after checking its header, size, and hash.

`stream .agentvmm.appliance.v1.StageContentRequest` → `.agentvmm.appliance.v1.StageContentResponse`

### `GetContentStatus`

GetContentStatus reads whether the exact approved content archive is staged and available.

`.agentvmm.appliance.v1.GetContentStatusRequest` → `.agentvmm.appliance.v1.GetContentStatusResponse`

### `StageArtifact`

StageArtifact validates and records a canonical appliance manifest without installing or
activating it.

`.agentvmm.appliance.v1.StageArtifactRequest` → `.agentvmm.appliance.v1.StageArtifactResponse`

### `PlanInstall`

PlanInstall reads installation impacts and checks the proposed grant before installation.

`.agentvmm.appliance.v1.PlanInstallRequest` → `.agentvmm.appliance.v1.PlanInstallResponse`

### `Install`

Install creates one installation, private registration, and data-volume identity under the
approved grant.

`.agentvmm.appliance.v1.InstallRequest` → `.agentvmm.appliance.v1.InstallResponse`

### `PlanUpdate`

PlanUpdate reads impacts for the selected artifact and grant at the current installation
revision.

`.agentvmm.appliance.v1.PlanUpdateRequest` → `.agentvmm.appliance.v1.PlanUpdateResponse`

### `UpdateAppliance`

UpdateAppliance persists drain, stop, switch, and activation while preserving installation
identity and private data.

`.agentvmm.appliance.v1.UpdateApplianceRequest` → `.agentvmm.appliance.v1.UpdateApplianceResponse`

### `RecoverApplianceUpdate`

RecoverApplianceUpdate resumes the original failed update with a current observation fence and
explicit extended deadline.

`.agentvmm.appliance.v1.RecoverApplianceUpdateRequest` → `.agentvmm.appliance.v1.RecoverApplianceUpdateResponse`

### `PlanActivation`

PlanActivation reads the current installation's activation impacts without starting it.

`.agentvmm.appliance.v1.PlanActivationRequest` → `.agentvmm.appliance.v1.PlanActivationResponse`

### `Activate`

Activate activates the exact installation revision and publishes only its current boot and
instance.

`.agentvmm.appliance.v1.ActivateRequest` → `.agentvmm.appliance.v1.ActivateResponse`

### `ListAppliances`

ListAppliances reads installation status and current frontend availability for the operator.

`.agentvmm.appliance.v1.ListAppliancesRequest` → `.agentvmm.appliance.v1.ListAppliancesResponse`

### `GetApplianceStatus`

GetApplianceStatus reads one installation and preserves unknown runtime-stop observations.

`.agentvmm.appliance.v1.GetApplianceStatusRequest` → `.agentvmm.appliance.v1.GetApplianceStatusResponse`

### `WatchApplianceOperation`

WatchApplianceOperation streams the original installation operation receipt through terminal
state.

`.agentvmm.appliance.v1.WatchApplianceOperationRequest` → `stream .agentvmm.appliance.v1.WatchApplianceOperationResponse`

### `StopAppliance`

StopAppliance stops one installation's runtime publication while retaining its private data
and trust.

`.agentvmm.appliance.v1.StopApplianceRequest` → `.agentvmm.appliance.v1.StopApplianceResponse`

### `RestartAppliance`

RestartAppliance confirms the exact old writer stopped before launching another instance.

`.agentvmm.appliance.v1.RestartApplianceRequest` → `.agentvmm.appliance.v1.RestartApplianceResponse`

### `RevokeAppliance`

RevokeAppliance retires the installation's private controller authority and its owned
workloads.

`.agentvmm.appliance.v1.RevokeApplianceRequest` → `.agentvmm.appliance.v1.RevokeApplianceResponse`

### `PlanRemove`

PlanRemove reads the exact installation domain and data impacts before irreversible removal.

`.agentvmm.appliance.v1.PlanRemoveRequest` → `.agentvmm.appliance.v1.PlanRemoveResponse`

### `ExecuteRemove`

ExecuteRemove accepts the reviewed revision and exact confirmed resources, retaining its
receipt until cleanup completes.

`.agentvmm.appliance.v1.ExecuteRemoveRequest` → `.agentvmm.appliance.v1.ExecuteRemoveResponse`

## Service `ApplianceBootstrapService`

ApplianceBootstrapService is the private launch-authentication surface exposed by a system
appliance.

### `Bootstrap`

Bootstrap authenticates one launch, consumes an optional opaque envelope, and returns receipt
and health frames in order.

`stream .agentvmm.appliance.v1.BootstrapRequest` → `stream .agentvmm.appliance.v1.BootstrapResponse`

## Message `BuildArtifactHeader`

BuildArtifactHeader selects an appliance build request and context-relative Dockerfile options.


## Message `BuildArtifactRequest`

BuildArtifactRequest carries inputs for an operation that builds an appliance archive and
streams its bytes with a final size and digest commitment.


## Message `BuildArtifactResult`

BuildArtifactResult commits the streamed archive size, SHA-256, and OCI manifest digest.


## Message `BuildArtifactResponse`

BuildArtifactResponse carries results from an operation that builds an appliance archive and
streams its bytes with a final size and digest commitment.


## Message `StreamBounds`

StreamBounds limits frame size, concurrency, idle time, deadline, and declared cumulative bytes
for one stream purpose.


## Message `ControllerFrontend`

ControllerFrontend declares one static appliance endpoint, its exposure, and its stream bounds.


## Message `CapacityGrant`

CapacityGrant bounds each environment contract requested by an appliance controller without a
reservation ledger.


## Message `ControllerApplianceManifest`

ControllerApplianceManifest binds a product artifact to immutable content, protocols, requested
limits, and static permissions.


## Message `HostTcpEndpoint`

HostTcpEndpoint approves one exact Host-reachable IPv4 TCP endpoint for installation egress.


## Message `ApprovedGrant`

ApprovedGrant records operator-approved permissions at the Host-owned grant revision.


## Message `ApplianceLaunchClaim`

ApplianceLaunchClaim signs one installation launch under its artifact, boot, instance,
configuration, and content fences.


## Message `ApplianceManagedTrustBundle`

ApplianceManagedTrustBundle binds optional managed authority to the current authenticated
appliance launch.


## Message `OpaqueBootstrapEnvelope`

OpaqueBootstrapEnvelope carries opaque management seed bytes for one installation and single-use
bootstrap identity.


## Message `BootstrapReceipt`

BootstrapReceipt acknowledges consumption of the exact bootstrap envelope nonce.


## Message `ApplianceHealth`

ApplianceHealth binds appliance readiness to the current launch and its readiness nonce.


## Message `ApplianceFrontendStatus`

ApplianceFrontendStatus reports the actual static endpoint and current availability for one
container instance.


## Message `ApplianceStatus`

ApplianceStatus combines durable installation state with current launch, channel, frontend, and
runtime-stop observations.


## Message `ApplianceImpact`

ApplianceImpact declares an exact installation lifecycle effect before operator confirmation.


## Message `AppliancePlan`

AppliancePlan binds approved lifecycle impacts to the current installation revision and
artifact.


## Message `ApplianceOperationError`

ApplianceOperationError retains a safe lifecycle failure reason and message.


## Message `ApplianceOperation`

ApplianceOperation retains original lifecycle request identity, phase, deadline, and terminal
evidence.


## Message `StageArtifactRequest`

StageArtifactRequest carries inputs for an operation that validates and records a canonical
appliance manifest without installing or activating it.


## Message `PlanInstallRequest`

PlanInstallRequest carries inputs for an operation that reads installation impacts and checks
the proposed grant before installation.


## Message `InstallRequest`

InstallRequest carries inputs for an operation that creates one installation, private
registration, and data-volume identity under the approved grant.


## Message `PlanActivationRequest`

PlanActivationRequest carries inputs for an operation that reads the current installation's
activation impacts without starting it.


## Message `ActivateRequest`

ActivateRequest carries inputs for an operation that activates the exact installation revision
and publishes only its current boot and instance.


## Message `ListAppliancesRequest`

ListAppliancesRequest carries inputs for an operation that reads installation status and
current frontend availability for the operator.


## Message `ListAppliancesResponse`

ListAppliancesResponse carries results from an operation that reads installation status and
current frontend availability for the operator.


## Message `GetApplianceStatusRequest`

GetApplianceStatusRequest carries inputs for an operation that reads one installation and
preserves unknown runtime-stop observations.


## Message `WatchApplianceOperationRequest`

WatchApplianceOperationRequest carries inputs for an operation that streams the original
installation operation receipt through terminal state.


## Message `StopApplianceRequest`

StopApplianceRequest carries inputs for an operation that stops one installation's runtime
publication while retaining its private data and trust.


## Message `RestartApplianceRequest`

RestartApplianceRequest carries inputs for an operation that confirms the exact old writer
stopped before launching another instance.


## Message `RevokeApplianceRequest`

RevokeApplianceRequest carries inputs for an operation that retires the installation's private
controller authority and its owned workloads.


## Message `PlanRemoveRequest`

PlanRemoveRequest carries inputs for an operation that reads the exact installation domain and
data impacts before irreversible removal.


## Message `ExecuteRemoveRequest`

ExecuteRemoveRequest carries the frozen installation revision, original deadline, and complete
operator-confirmed resource set.


## Message `PlanUpdateRequest`

PlanUpdateRequest carries inputs for an operation that reads impacts for the selected artifact
and grant at the current installation revision.


## Message `UpdateApplianceRequest`

UpdateApplianceRequest carries inputs for an operation that persists drain, stop, switch, and
activation while preserving installation identity and private data.


## Message `RecoverApplianceUpdateRequest`

RecoverApplianceUpdateRequest carries inputs for an operation that resumes the original failed
update with a current observation fence and explicit extended deadline.


## Message `ApplianceUpdateProgress`

ApplianceUpdateProgress reports persisted update phases for the original source and target
artifacts.


## Message `ApplianceContentReference`

ApplianceContentReference authorizes an exact archive role, size, hash, platform, and image
manifest identity.


## Message `ApplianceContentMount`

ApplianceContentMount projects one authorized immutable dependency archive into the current
controller launch.


## Message `StageContentHeader`

StageContentHeader commits archive identity before bounded content chunks arrive.


## Message `StageContentRequest`

StageContentRequest carries inputs for an operation that stages one content-addressed archive
after checking its header, size, and hash.


## Message `ApplianceContentStatus`

ApplianceContentStatus reports whether an exact content commitment is available in the Host
cache.


## Message `GetContentStatusRequest`

GetContentStatusRequest carries inputs for an operation that reads whether the exact approved
content archive is staged and available.


## Message `GetContentStatusResponse`

GetContentStatusResponse carries results from an operation that reads whether the exact
approved content archive is staged and available.


## Message `CreateApplianceServiceImportRequest`

CreateApplianceServiceImportRequest carries inputs for an operation that approves service
access from the current installation's private controller domain.


## Message `ListApplianceServiceImportsRequest`

ListApplianceServiceImportsRequest carries inputs for an operation that reads only imports
owned by the selected installation.


## Message `ListApplianceServiceImportsResponse`

ListApplianceServiceImportsResponse carries results from an operation that reads only imports
owned by the selected installation.


## Message `RevokeApplianceServiceImportRequest`

RevokeApplianceServiceImportRequest carries inputs for an operation that revokes the exact
installation-owned service import revision.


## Message `CreateApplianceFrontendServiceImportRequest`

CreateApplianceFrontendServiceImportRequest supplies the inputs for
CreateApplianceFrontendServiceImport. The operation approves a workload service as the selected static
appliance frontend target.


## Message `CreateApplianceServiceImportResponse`

CreateApplianceServiceImportResponse carries results from an operation that approves service
access from the current installation's private controller domain.


## Message `CreateApplianceFrontendServiceImportResponse`

CreateApplianceFrontendServiceImportResponse reports the result of
CreateApplianceFrontendServiceImport. The operation approves a workload service as the selected static
appliance frontend target.


## Message `RevokeApplianceServiceImportResponse`

RevokeApplianceServiceImportResponse carries results from an operation that revokes the exact
installation-owned service import revision.


## Message `StageContentResponse`

StageContentResponse carries results from an operation that stages one content-addressed
archive after checking its header, size, and hash.


## Message `StageArtifactResponse`

StageArtifactResponse carries results from an operation that validates and records a canonical
appliance manifest without installing or activating it.


## Message `PlanInstallResponse`

PlanInstallResponse carries results from an operation that reads installation impacts and
checks the proposed grant before installation.


## Message `InstallResponse`

InstallResponse carries results from an operation that creates one installation, private
registration, and data-volume identity under the approved grant.


## Message `PlanUpdateResponse`

PlanUpdateResponse carries results from an operation that reads impacts for the selected
artifact and grant at the current installation revision.


## Message `UpdateApplianceResponse`

UpdateApplianceResponse carries results from an operation that persists drain, stop, switch,
and activation while preserving installation identity and private data.


## Message `RecoverApplianceUpdateResponse`

RecoverApplianceUpdateResponse carries results from an operation that resumes the original
failed update with a current observation fence and explicit extended deadline.


## Message `PlanActivationResponse`

PlanActivationResponse carries results from an operation that reads the current installation's
activation impacts without starting it.


## Message `ActivateResponse`

ActivateResponse carries results from an operation that activates the exact installation
revision and publishes only its current boot and instance.


## Message `GetApplianceStatusResponse`

GetApplianceStatusResponse carries results from an operation that reads one installation and
preserves unknown runtime-stop observations.


## Message `WatchApplianceOperationResponse`

WatchApplianceOperationResponse carries results from an operation that streams the original
installation operation receipt through terminal state.


## Message `StopApplianceResponse`

StopApplianceResponse carries results from an operation that stops one installation's runtime
publication while retaining its private data and trust.


## Message `RestartApplianceResponse`

RestartApplianceResponse carries results from an operation that confirms the exact old writer
stopped before launching another instance.


## Message `RevokeApplianceResponse`

RevokeApplianceResponse carries results from an operation that retires the installation's
private controller authority and its owned workloads.


## Message `PlanRemoveResponse`

PlanRemoveResponse carries results from an operation that reads the exact installation domain
and data impacts before irreversible removal.


## Message `ExecuteRemoveResponse`

ExecuteRemoveResponse carries results from an operation that accepts the reviewed revision and
exact confirmed resources, retaining its receipt until cleanup completes.


## Message `BootstrapRequest`

BootstrapRequest sends a launch claim, optional opaque seed envelope, then explicit write
closure for one launch.


## Message `BootstrapResponse`

BootstrapResponse returns seed consumption receipt when needed, current health, optional
managed trust, and explicit write closure.


## Enum `FrontendTransport`

### `FRONTEND_TRANSPORT_UNSPECIFIED = 0`

### `FRONTEND_TRANSPORT_BYTE_STREAM = 1`

### `FRONTEND_TRANSPORT_HTTP = 2`

## Enum `FrontendExposure`

### `FRONTEND_EXPOSURE_UNSPECIFIED = 0`

### `FRONTEND_EXPOSURE_HOST_LOOPBACK = 1`

All local users can connect; the appliance protocol must authenticate clients.

### `FRONTEND_EXPOSURE_OUTBOUND_ONLY = 2`

### `FRONTEND_EXPOSURE_LAN_HTTP = 3`

## Enum `ApplianceState`

### `APPLIANCE_STATE_UNSPECIFIED = 0`

### `APPLIANCE_STATE_STOPPED = 1`

### `APPLIANCE_STATE_STARTING = 2`

### `APPLIANCE_STATE_RUNNING = 3`

### `APPLIANCE_STATE_STOPPING = 4`

### `APPLIANCE_STATE_FAILED = 5`

### `APPLIANCE_STATE_SEALED = 6`

## Enum `ControllerChannelState`

### `CONTROLLER_CHANNEL_STATE_UNSPECIFIED = 0`

### `CONTROLLER_CHANNEL_STATE_DISCONNECTED = 1`

### `CONTROLLER_CHANNEL_STATE_ENROLLING = 2`

### `CONTROLLER_CHANNEL_STATE_CONNECTING = 3`

### `CONTROLLER_CHANNEL_STATE_RECONCILING = 4`

### `CONTROLLER_CHANNEL_STATE_ACTIVE = 5`

### `CONTROLLER_CHANNEL_STATE_REVOKED = 6`

## Enum `ApplianceOperationState`

### `APPLIANCE_OPERATION_STATE_UNSPECIFIED = 0`

### `APPLIANCE_OPERATION_STATE_RUNNING = 1`

### `APPLIANCE_OPERATION_STATE_SUCCEEDED = 2`

### `APPLIANCE_OPERATION_STATE_FAILED = 3`

### `APPLIANCE_OPERATION_STATE_INTERRUPTED = 4`

## Enum `AppliancePlanKind`

### `APPLIANCE_PLAN_KIND_UNSPECIFIED = 0`

### `APPLIANCE_PLAN_KIND_INSTALL = 1`

### `APPLIANCE_PLAN_KIND_ACTIVATE = 2`

### `APPLIANCE_PLAN_KIND_REMOVE = 3`

### `APPLIANCE_PLAN_KIND_UPDATE = 5`

## Enum `ApplianceOperationKind`

### `APPLIANCE_OPERATION_KIND_UNSPECIFIED = 0`

### `APPLIANCE_OPERATION_KIND_STAGE = 1`

### `APPLIANCE_OPERATION_KIND_INSTALL = 2`

### `APPLIANCE_OPERATION_KIND_ACTIVATE = 3`

### `APPLIANCE_OPERATION_KIND_STOP = 4`

### `APPLIANCE_OPERATION_KIND_RESTART = 5`

### `APPLIANCE_OPERATION_KIND_REVOKE = 6`

### `APPLIANCE_OPERATION_KIND_REMOVE = 7`

### `APPLIANCE_OPERATION_KIND_UPDATE = 9`

### `APPLIANCE_OPERATION_KIND_STAGE_CONTENT = 10`

## Enum `ApplianceOperationPhase`

### `APPLIANCE_OPERATION_PHASE_UNSPECIFIED = 0`

### `APPLIANCE_OPERATION_PHASE_PLANNED = 1`

### `APPLIANCE_OPERATION_PHASE_REPAIRING_SEALED = 3`

### `APPLIANCE_OPERATION_PHASE_PENDING_DESTROY = 4`

### `APPLIANCE_OPERATION_PHASE_TERMINAL = 5`

### `APPLIANCE_OPERATION_PHASE_PREPARING = 6`

### `APPLIANCE_OPERATION_PHASE_DRAINING = 7`

### `APPLIANCE_OPERATION_PHASE_STOPPING = 8`

### `APPLIANCE_OPERATION_PHASE_SWITCHING = 9`

### `APPLIANCE_OPERATION_PHASE_ACTIVATING = 10`

## Enum `ApplianceTerminalReason`

### `APPLIANCE_TERMINAL_REASON_UNSPECIFIED = 0`

### `APPLIANCE_TERMINAL_REASON_SUCCEEDED = 1`

### `APPLIANCE_TERMINAL_REASON_FENCE_CONFLICT = 2`

### `APPLIANCE_TERMINAL_REASON_CORRUPTION = 3`

### `APPLIANCE_TERMINAL_REASON_KEY_LOST = 4`

### `APPLIANCE_TERMINAL_REASON_OPERATOR_REVOKED = 6`

### `APPLIANCE_TERMINAL_REASON_REMOVED = 7`

### `APPLIANCE_TERMINAL_REASON_OPERATION_FAILED = 8`

### `APPLIANCE_TERMINAL_REASON_OPERATOR_STOPPED = 9`

## Enum `ApplianceContentRole`

### `APPLIANCE_CONTENT_ROLE_UNSPECIFIED = 0`

### `APPLIANCE_CONTENT_ROLE_CONTROLLER = 1`

### `APPLIANCE_CONTENT_ROLE_HARNESS = 2`

### `APPLIANCE_CONTENT_ROLE_WORKLOAD = 3`
