# `agentvmm.host.v1`

Source: `host/v1/runtime.proto`.

## Service `AgentRuntimeService`

AgentRuntimeService is scoped by the Host to one authenticated environment and cannot select
another namespace.

### `GetEnvironment`

GetEnvironment reads environment policy and observed runtime facts under the caller's
authority.

`.agentvmm.host.v1.GetEnvironmentRequest` → `.agentvmm.host.v1.GetEnvironmentResponse`

### `PullImage`

PullImage resolves an existing grant or imports an anonymous linux/arm64 registry image
through the immutable archive path.

`.agentvmm.host.v1.PullImageRequest` → `stream .agentvmm.host.v1.PullImageResponse`

### `ImportImage`

ImportImage accepts one digest-bound header and either a public HTTPS source or bounded local
archive chunks.

`stream .agentvmm.host.v1.ImportImageRequest` → `.agentvmm.host.v1.ImportImageResponse`

### `ListImages`

ListImages reads materialized image grants visible to this runtime session.

`.agentvmm.host.v1.ListImagesRequest` → `.agentvmm.host.v1.ListImagesResponse`

### `DeleteImage`

DeleteImage deletes this environment's image grant only when no owned container still
references it.

`.agentvmm.host.v1.DeleteImageRequest` → `.agentvmm.host.v1.DeleteImageResponse`

### `CreateContainer`

CreateContainer creates an owned container configuration using authorized images, volumes, and
mounts.

`.agentvmm.host.v1.CreateContainerRequest` → `.agentvmm.host.v1.CreateContainerResponse`

### `StartContainer`

StartContainer starts the retained workload process and returns its new instance identity.

`.agentvmm.host.v1.StartContainerRequest` → `.agentvmm.host.v1.StartContainerResponse`

### `StopContainer`

StopContainer stops the exact owned workload process under its current container fence.

`.agentvmm.host.v1.StopContainerRequest` → `.agentvmm.host.v1.StopContainerResponse`

### `AcquireExecution`

AcquireExecution claims an instance-bound execution right before an operation that can outlive
its transport.

`.agentvmm.host.v1.AcquireExecutionRequest` → `.agentvmm.host.v1.AcquireExecutionResponse`

### `ReleaseExecution`

ReleaseExecution settles the exact execution right without substituting another process
instance.

`.agentvmm.host.v1.ReleaseExecutionRequest` → `.agentvmm.host.v1.ReleaseExecutionResponse`

### `ListExecutions`

ListExecutions reads execution settlement facts for the selected current container instance.

`.agentvmm.host.v1.ListExecutionsRequest` → `.agentvmm.host.v1.ListExecutionsResponse`

### `QuiesceContainer`

QuiesceContainer closes new execution admission and waits for the selected container's
accepted executions.

`.agentvmm.host.v1.QuiesceContainerRequest` → `.agentvmm.host.v1.QuiesceContainerResponse`

### `WaitContainer`

WaitContainer observes the retained process's terminal result without treating request
cancellation as process exit.

`.agentvmm.host.v1.WaitContainerRequest` → `.agentvmm.host.v1.WaitContainerResponse`

### `DeleteContainer`

DeleteContainer deletes an owned stopped container without deleting its named volumes.

`.agentvmm.host.v1.DeleteContainerRequest` → `.agentvmm.host.v1.DeleteContainerResponse`

### `GetContainer`

GetContainer reads reconciled task state and the current instance identity.

`.agentvmm.host.v1.GetContainerRequest` → `.agentvmm.host.v1.GetContainerResponse`

### `ListContainers`

ListContainers lists only containers owned by the authenticated runtime environment.

`.agentvmm.host.v1.ListContainersRequest` → `.agentvmm.host.v1.ListContainersResponse`

### `Logs`

Logs reads bounded container output under the caller's environment authority.

`.agentvmm.host.v1.LogsRequest` → `stream .agentvmm.host.v1.LogsResponse`

### `Exec`

Exec starts one instance-fenced process and exchanges explicit stdin, resize, output, and
terminal frames.

`stream .agentvmm.host.v1.ExecRequest` → `stream .agentvmm.host.v1.ExecResponse`

### `SignalContainer`

SignalContainer signals the retained workload process, whose natural exit can race the
request.

`.agentvmm.host.v1.SignalContainerRequest` → `.agentvmm.host.v1.SignalContainerResponse`

### `CreateVolume`

CreateVolume creates a named private volume with optional numeric ownership and permissions.

`.agentvmm.host.v1.CreateVolumeRequest` → `.agentvmm.host.v1.CreateVolumeResponse`

### `DeleteVolume`

DeleteVolume deletes an owned unmounted volume under its generation fence.

`.agentvmm.host.v1.DeleteVolumeRequest` → `.agentvmm.host.v1.DeleteVolumeResponse`

### `GetVolume`

GetVolume reads one owned volume and its environment quota context.

`.agentvmm.host.v1.GetVolumeRequest` → `.agentvmm.host.v1.GetVolumeResponse`

### `ListVolumes`

ListVolumes lists named volumes within the authenticated environment.

`.agentvmm.host.v1.ListVolumesRequest` → `.agentvmm.host.v1.ListVolumesResponse`

### `ForwardTCP`

ForwardTCP forwards a bounded authorized stream to one current owned container instance and
port.

`stream .agentvmm.host.v1.ForwardTCPRequest` → `stream .agentvmm.host.v1.ForwardTCPResponse`

### `BuildImage`

BuildImage builds in a separate rootless sandbox and retains the original content-bound
operation receipt.

`stream .agentvmm.host.v1.BuildImageRequest` → `stream .agentvmm.host.v1.BuildImageResponse`

### `CopyImage`

CopyImage copies an authorized image between exact source and target allocation generations
under one registration.

`.agentvmm.host.v1.CopyImageRequest` → `.agentvmm.host.v1.CopyImageResponse`

### `ObserveImageOperation`

ObserveImageOperation reads the original image operation result and independent writer-
settlement fact.

`.agentvmm.host.v1.ObserveImageOperationRequest` → `.agentvmm.host.v1.ObserveImageOperationResponse`

### `CancelImageOperation`

CancelImageOperation cancels the original image operation and observes writer cleanup before
settlement.

`.agentvmm.host.v1.CancelImageOperationRequest` → `.agentvmm.host.v1.CancelImageOperationResponse`

### `ReleaseSession`

ReleaseSession closes this runtime capability while preserving allocation data and independent
workload ownership.

`.agentvmm.host.v1.ReleaseSessionRequest` → `.agentvmm.host.v1.ReleaseSessionResponse`

### `StatWorkspace`

StatWorkspace reads metadata beneath the owned workspace root without following an escaping
path.

`.agentvmm.host.v1.StatWorkspaceRequest` → `.agentvmm.host.v1.StatWorkspaceResponse`

### `ListWorkspace`

ListWorkspace reads a bounded directory view beneath the owned workspace root.

`.agentvmm.host.v1.ListWorkspaceRequest` → `.agentvmm.host.v1.ListWorkspaceResponse`

### `ReadWorkspace`

ReadWorkspace streams bytes from an authorized workspace file without exposing raw Host paths.

`.agentvmm.host.v1.ReadWorkspaceRequest` → `stream .agentvmm.host.v1.ReadWorkspaceResponse`

### `WriteWorkspace`

WriteWorkspace writes bounded chunks to the header-selected workspace file with the supplied
prefix check.

`stream .agentvmm.host.v1.WriteWorkspaceRequest` → `.agentvmm.host.v1.WriteWorkspaceResponse`

### `ArchiveWorkspace`

ArchiveWorkspace streams an archive of the authorized workspace scope.

`.agentvmm.host.v1.ArchiveWorkspaceRequest` → `stream .agentvmm.host.v1.ArchiveWorkspaceResponse`

### `ExtractWorkspace`

ExtractWorkspace extracts a bounded archive beneath the owned workspace root.

`stream .agentvmm.host.v1.ExtractWorkspaceRequest` → `.agentvmm.host.v1.ExtractWorkspaceResponse`

### `CreateServiceExport`

CreateServiceExport authorizes a port on the current owned container and follows that
container ID across later instances.

`.agentvmm.host.v1.CreateServiceExportRequest` → `.agentvmm.host.v1.CreateServiceExportResponse`

### `ListServiceExports`

ListServiceExports lists exports visible within the caller's authority, including retained
revoked records.

`.agentvmm.host.v1.ListServiceExportsRequest` → `.agentvmm.host.v1.ListServiceExportsResponse`

### `RevokeServiceExport`

RevokeServiceExport revokes the exact export revision and its dependent route projections.

`.agentvmm.host.v1.RevokeServiceExportRequest` → `.agentvmm.host.v1.RevokeServiceExportResponse`

### `CreateServiceImport`

CreateServiceImport binds a current source instance to a local export or current controller-
issued capability.

`.agentvmm.host.v1.CreateServiceImportRequest` → `.agentvmm.host.v1.CreateServiceImportResponse`

### `CreatePersonalServiceImport`

CreatePersonalServiceImport obtains destination-issued personal route authority using current
mesh membership and explicit budgets.

`.agentvmm.host.v1.CreatePersonalServiceImportRequest` → `.agentvmm.host.v1.CreatePersonalServiceImportResponse`

### `ListServiceImports`

ListServiceImports lists imports owned by the authenticated environment.

`.agentvmm.host.v1.ListServiceImportsRequest` → `.agentvmm.host.v1.ListServiceImportsResponse`

### `RevokeServiceImport`

RevokeServiceImport revokes one import revision and its active route projections.

`.agentvmm.host.v1.RevokeServiceImportRequest` → `.agentvmm.host.v1.RevokeServiceImportResponse`

## Message `ListWorkspaceRequest`

ListWorkspaceRequest carries inputs for an operation that reads a bounded directory view
beneath the owned workspace root.


## Message `ListWorkspaceResponse`

ListWorkspaceResponse carries results from an operation that reads a bounded directory view
beneath the owned workspace root.


## Message `ReadWorkspaceRequest`

ReadWorkspaceRequest carries inputs for an operation that streams bytes from an authorized
workspace file without exposing raw Host paths.


## Message `ReadWorkspaceResponse`

ReadWorkspaceResponse carries results from an operation that streams bytes from an authorized
workspace file without exposing raw Host paths.


## Message `WriteWorkspaceRequest`

WriteWorkspaceRequest carries inputs for an operation that writes bounded chunks to the
header-selected workspace file with the supplied prefix check.


## Message `ArchiveWorkspaceRequest`

ArchiveWorkspaceRequest carries inputs for an operation that streams an archive of the
authorized workspace scope.


## Message `ArchiveWorkspaceResponse`

ArchiveWorkspaceResponse carries results from an operation that streams an archive of the
authorized workspace scope.


## Message `ExtractWorkspaceRequest`

ExtractWorkspaceRequest carries inputs for an operation that extracts a bounded archive
beneath the owned workspace root.


## Message `CreateServiceExportRequest`

CreateServiceExportRequest carries inputs for an operation that authorizes a port on the
current owned container and follows that container ID across later instances.


## Message `CreatePersonalServiceImportRequest`

CreatePersonalServiceImportRequest carries inputs for an operation that obtains destination-
issued personal route authority using current mesh membership and explicit budgets.


## Message `ListServiceImportsRequest`

ListServiceImportsRequest carries inputs for an operation that lists imports owned by the
authenticated environment.


## Message `ListServiceImportsResponse`

ListServiceImportsResponse carries results from an operation that lists imports owned by the
authenticated environment.


## Message `ReleaseSessionRequest`

ReleaseSessionRequest carries inputs for an operation that closes this runtime capability
while preserving allocation data and independent workload ownership.


## Message `CreateContainerRequest`

CreateContainerRequest carries inputs for an operation that creates an owned container
configuration using authorized images, volumes, and mounts.


## Message `StopContainerRequest`

StopContainerRequest carries inputs for an operation that stops the exact owned workload
process under its current container fence.


## Message `ListExecutionsRequest`

ListExecutionsRequest carries inputs for an operation that reads execution settlement facts
for the selected current container instance.


## Message `ListExecutionsResponse`

ListExecutionsResponse carries results from an operation that reads execution settlement facts
for the selected current container instance.


## Message `QuiesceContainerRequest`

QuiesceContainerRequest carries inputs for an operation that closes new execution admission
and waits for the selected container's accepted executions.


## Message `WaitContainerResponse`

WaitContainerResponse carries results from an operation that observes the retained process's
terminal result without treating request cancellation as process exit.


## Message `ListContainersRequest`

ListContainersRequest carries inputs for an operation that lists only containers owned by the
authenticated runtime environment.


## Message `ListContainersResponse`

ListContainersResponse carries results from an operation that lists only containers owned by
the authenticated runtime environment.


## Message `PullImageRequest`

PullImageRequest carries inputs for an operation that resolves an existing grant or imports an
anonymous linux/arm64 registry image through the immutable archive path.


## Message `PullImageResponse`

PullImageResponse carries results from an operation that resolves an existing grant or imports
an anonymous linux/arm64 registry image through the immutable archive path.


## Message `ImportImageRequest`

ImportImageRequest starts with one immutable header. Local mode sends bounded archive chunks.
URL mode sends EOF after the header.


## Message `ListImagesRequest`

ListImagesRequest carries inputs for an operation that reads materialized image grants visible
to this runtime session.


## Message `DeleteImageRequest`

DeleteImageRequest carries inputs for an operation that deletes this environment's image grant
only when no owned container still references it.


## Message `LogsRequest`

LogsRequest carries inputs for an operation that reads bounded container output under the
caller's environment authority.


## Message `SignalContainerRequest`

SignalContainerRequest carries inputs for an operation that signals the retained workload
process, whose natural exit can race the request.


## Message `ExecRequest`

ExecRequest starts with ExecStart, then sends input or resize events. close_stdin explicitly
ends process input.


## Message `ExecResponse`

ExecResponse carries process output and terminal exit evidence. A disconnected stream does not
establish execution settlement.


## Message `CreateVolumeRequest`

CreateVolumeRequest carries inputs for an operation that creates a named private volume with
optional numeric ownership and permissions.


## Message `DeleteVolumeRequest`

DeleteVolumeRequest carries inputs for an operation that deletes an owned unmounted volume
under its generation fence.


## Message `ListVolumesRequest`

ListVolumesRequest carries inputs for an operation that lists named volumes within the
authenticated environment.


## Message `ListVolumesResponse`

ListVolumesResponse carries results from an operation that lists named volumes within the
authenticated environment.


## Message `ForwardTCPRequest`

ForwardTCPRequest carries inputs for an operation that forwards a bounded authorized stream to
one current owned container instance and port.


## Message `ForwardTCPResponse`

ForwardTCPResponse carries results from an operation that forwards a bounded authorized stream
to one current owned container instance and port.


## Message `BuildImageRequest`

BuildImageRequest carries inputs for an operation that builds in a separate rootless sandbox
and retains the original content-bound operation receipt.


## Message `CopyImageRequest`

CopyImageRequest carries inputs for an operation that copies an authorized image between exact
source and target allocation generations under one registration.


## Message `BuildImageResponse`

BuildImageResponse carries results from an operation that builds in a separate rootless
sandbox and retains the original content-bound operation receipt.


## Message `CreateServiceImportRequest`

CreateServiceImportRequest carries inputs for an operation that binds a current source
instance to a local export or current controller-issued capability.


## Message `RevokeServiceImportRequest`

RevokeServiceImportRequest carries inputs for an operation that revokes one import revision
and its active route projections.


## Message `GetEnvironmentRequest`

GetEnvironmentRequest carries inputs for an operation that reads environment policy and
observed runtime facts under the caller's authority.


## Message `ImportImageResponse`

ImportImageResponse carries results from an operation that accepts one digest-bound header and
either a public HTTPS source or bounded local archive chunks.


## Message `DeleteImageResponse`

DeleteImageResponse carries results from an operation that deletes this environment's image
grant only when no owned container still references it.


## Message `CreateContainerResponse`

CreateContainerResponse carries results from an operation that creates an owned container
configuration using authorized images, volumes, and mounts.


## Message `StartContainerRequest`

StartContainerRequest carries inputs for an operation that starts the retained workload
process and returns its new instance identity.


## Message `StartContainerResponse`

StartContainerResponse carries results from an operation that starts the retained workload
process and returns its new instance identity.


## Message `StopContainerResponse`

StopContainerResponse carries results from an operation that stops the exact owned workload
process under its current container fence.


## Message `AcquireExecutionRequest`

AcquireExecutionRequest carries inputs for an operation that claims an instance-bound
execution right before an operation that can outlive its transport.


## Message `AcquireExecutionResponse`

AcquireExecutionResponse carries results from an operation that claims an instance-bound
execution right before an operation that can outlive its transport.


## Message `ReleaseExecutionRequest`

ReleaseExecutionRequest carries inputs for an operation that settles the exact execution right
without substituting another process instance.


## Message `ReleaseExecutionResponse`

ReleaseExecutionResponse carries results from an operation that settles the exact execution
right without substituting another process instance.


## Message `QuiesceContainerResponse`

QuiesceContainerResponse carries results from an operation that closes new execution admission
and waits for the selected container's accepted executions.


## Message `WaitContainerRequest`

WaitContainerRequest carries inputs for an operation that observes the retained process's
terminal result without treating request cancellation as process exit.


## Message `DeleteContainerRequest`

DeleteContainerRequest carries inputs for an operation that deletes an owned stopped container
without deleting its named volumes.


## Message `DeleteContainerResponse`

DeleteContainerResponse carries results from an operation that deletes an owned stopped
container without deleting its named volumes.


## Message `GetContainerRequest`

GetContainerRequest carries inputs for an operation that reads reconciled task state and the
current instance identity.


## Message `GetContainerResponse`

GetContainerResponse carries results from an operation that reads reconciled task state and
the current instance identity.


## Message `LogsResponse`

LogsResponse carries results from an operation that reads bounded container output under the
caller's environment authority.


## Message `SignalContainerResponse`

SignalContainerResponse carries results from an operation that signals the retained workload
process, whose natural exit can race the request.


## Message `CreateVolumeResponse`

CreateVolumeResponse carries results from an operation that creates a named private volume
with optional numeric ownership and permissions.


## Message `DeleteVolumeResponse`

DeleteVolumeResponse carries results from an operation that deletes an owned unmounted volume
under its generation fence.


## Message `GetVolumeRequest`

GetVolumeRequest carries inputs for an operation that reads one owned volume and its
environment quota context.


## Message `GetVolumeResponse`

GetVolumeResponse carries results from an operation that reads one owned volume and its
environment quota context.


## Message `CopyImageResponse`

CopyImageResponse carries results from an operation that copies an authorized image between
exact source and target allocation generations under one registration.


## Message `ObserveImageOperationRequest`

ObserveImageOperationRequest carries inputs for an operation that reads the original image
operation result and independent writer-settlement fact.


## Message `ObserveImageOperationResponse`

ObserveImageOperationResponse carries results from an operation that reads the original image
operation result and independent writer-settlement fact.


## Message `CancelImageOperationRequest`

CancelImageOperationRequest carries inputs for an operation that cancels the original image
operation and observes writer cleanup before settlement.


## Message `CancelImageOperationResponse`

CancelImageOperationResponse carries results from an operation that cancels the original image
operation and observes writer cleanup before settlement.


## Message `ReleaseSessionResponse`

ReleaseSessionResponse carries results from an operation that closes this runtime capability
while preserving allocation data and independent workload ownership.


## Message `StatWorkspaceRequest`

StatWorkspaceRequest carries inputs for an operation that reads metadata beneath the owned
workspace root without following an escaping path.


## Message `StatWorkspaceResponse`

StatWorkspaceResponse carries results from an operation that reads metadata beneath the owned
workspace root without following an escaping path.


## Message `ExtractWorkspaceResponse`

ExtractWorkspaceResponse carries results from an operation that extracts a bounded archive
beneath the owned workspace root.


## Message `CreateServiceExportResponse`

CreateServiceExportResponse carries results from an operation that authorizes a port on the
current owned container and follows that container ID across later instances.


## Message `ListServiceExportsRequest`

ListServiceExportsRequest carries inputs for an operation that lists exports visible within
the caller's authority, including retained revoked records.


## Message `ListServiceExportsResponse`

ListServiceExportsResponse carries results from an operation that lists exports visible within
the caller's authority, including retained revoked records.


## Message `RevokeServiceExportRequest`

RevokeServiceExportRequest carries inputs for an operation that revokes the exact export
revision and its dependent route projections.


## Message `RevokeServiceExportResponse`

RevokeServiceExportResponse carries results from an operation that revokes the exact export
revision and its dependent route projections.


## Message `CreateServiceImportResponse`

CreateServiceImportResponse carries results from an operation that binds a current source
instance to a local export or current controller-issued capability.


## Message `CreatePersonalServiceImportResponse`

CreatePersonalServiceImportResponse carries results from an operation that obtains
destination-issued personal route authority using current mesh membership and explicit
budgets.


## Message `RevokeServiceImportResponse`

RevokeServiceImportResponse carries results from an operation that revokes one import revision
and its active route projections.


## Message `GetEnvironmentResponse`

GetEnvironmentResponse carries results from an operation that reads environment policy and
observed runtime facts under the caller's authority.


## Message `ListImagesResponse`

ListImagesResponse carries results from an operation that reads materialized image grants
visible to this runtime session.


## Message `WriteWorkspaceResponse`

WriteWorkspaceResponse carries results from an operation that writes bounded chunks to the
header-selected workspace file with the supplied prefix check.
