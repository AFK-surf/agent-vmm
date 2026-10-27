//go:build darwin || linux

package servicenet

import (
	"context"
	"encoding/binary"
	"errors"
	"regexp"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var virtualServiceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)

const ServiceTunnelAudience = "agent-vmm/service-tunnel/1"

const localServiceRouteLease = 24 * time.Hour

func (controller *Owner) CreateServiceExport(ctx context.Context, request *hostv1.CreateServiceExportRequest) (*hostv1.CreateServiceExportResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if identity.RegistrationID != "" {
		unlock, lockErr := controller.serviceLocks.Lock(ctx, "service-domain:"+identity.RegistrationID)
		if lockErr != nil {
			return nil, lockErr
		}

		defer unlock()
	}
	if request.GetRequestId() == "" || request.GetContainerId() == "" || request.GetContainerInstanceId() == "" || request.GetPort() == 0 || request.GetPort() > 65535 ||
		(request.GetProtocol() != servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP && request.GetProtocol() != servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP) {
		return nil, hostv1.InvalidField("request_id/container_instance/port/protocol", "complete current service export is required")
	}
	observed, err := controller.ServiceContainerIdentity(ctx, identity.NamespaceID, request.GetContainerId(), request.GetContainerInstanceId())
	if err != nil {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_CONTAINER_INSTANCE, err.Error(), hostv1.WithResource("container", request.GetContainerId()))
	}
	exportID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	value, err := controller.store.CreateServiceExport(ctx, request.GetRequestId(), &servicev1.ServiceExport{ExportId: exportID,
		EnvironmentId: identity.EnvironmentID, ContainerId: request.GetContainerId(), ContainerInstanceId: request.GetContainerInstanceId(),
		Port: request.GetPort(), Protocol: request.GetProtocol(), ContainerGenerationId: observed.GetGenerationId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.CreateServiceExportResponse{Export: value}, nil
}

func (controller *Owner) ListServiceExports(ctx context.Context, _ *hostv1.ListServiceExportsRequest) (*hostv1.ListServiceExportsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	values, err := controller.store.ListServiceExports(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, err
	}
	approved, err := controller.store.ListApprovedServiceExports(ctx, identity.RegistrationID)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, value := range values {
		seen[value.GetExportId()] = true
	}
	for _, value := range approved {
		if !seen[value.GetExportId()] {
			values = append(values, value)
		}
	}
	response := &servicev1.ListServiceExportsResponse{}
	for _, value := range values {
		response.Exports = append(response.Exports, value)
	}
	return &hostv1.ListServiceExportsResponse{Exports: response}, nil
}

func (controller *Owner) RevokeServiceExport(ctx context.Context, request *hostv1.RevokeServiceExportRequest) (*hostv1.RevokeServiceExportResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExportId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/export_id/expected_revision", "complete export revoke is required")
	}
	unlock, lockErr := controller.serviceLocks.Lock(ctx, "service-export:"+request.GetExportId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	exported, err := controller.store.GetServiceExport(ctx, request.GetExportId())
	if err != nil || exported.GetEnvironmentId() != identity.EnvironmentID || exported.GetRevoked() || exported.GetRevision() != request.GetExpectedRevision() {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_SERVICE_EXPORT_REVISION_CONFLICT, "export owner or revision changed", hostv1.WithResource("service_export", request.GetExportId()))
	}
	imports, err := controller.store.ListActiveServiceImportsForExport(ctx, request.GetExportId())
	if err != nil {
		return nil, err
	}
	for _, importID := range imports {
		if err := controller.DeleteServiceProjection(ctx, request.GetRequestId()+":"+importID, importID); err != nil {
			return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_PROJECTION_DELETE_FAILED, err.Error(), hostv1.WithResource("service_import", importID))
		}
	}
	if err := controller.store.RevokeServiceExport(ctx, identity.EnvironmentID, request.GetExportId(), request.GetExpectedRevision()); err != nil {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_SERVICE_EXPORT_REVISION_CONFLICT, err.Error(), hostv1.WithResource("service_export", request.GetExportId()))
	}
	if err := controller.RestoreContainerServiceListeners(ctx, identity.EnvironmentID, exported.GetContainerId()); err != nil {
		return nil, err
	}
	return &hostv1.RevokeServiceExportResponse{}, nil
}

func (controller *Owner) RevokeServiceImport(ctx context.Context, request *hostv1.RevokeServiceImportRequest) (*hostv1.RevokeServiceImportResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetImportId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/import_id/expected_revision", "complete import revoke is required")
	}
	unlock, lockErr := controller.serviceLocks.Lock(ctx, "service-import:"+request.GetImportId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	value, err := controller.store.GetServiceImport(ctx, request.GetImportId())
	if err != nil || value.GetSourceEnvironmentId() != identity.EnvironmentID || value.GetRevoked() || value.GetRevision() != request.GetExpectedRevision() {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_SERVICE_IMPORT_REVISION_CONFLICT, "import owner or revision changed", hostv1.WithResource("service_import", request.GetImportId()))
	}
	if err := controller.DeleteServiceProjection(ctx, request.GetRequestId(), request.GetImportId()); err != nil {
		return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_PROJECTION_DELETE_FAILED, err.Error(), hostv1.WithResource("service_import", request.GetImportId()))
	}
	controller.egress.DeleteServiceRoute(request.GetImportId())
	if err := controller.store.RevokeServiceImport(ctx, identity.EnvironmentID, request.GetImportId(), request.GetExpectedRevision()); err != nil {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_SERVICE_IMPORT_REVISION_CONFLICT, err.Error(), hostv1.WithResource("service_import", request.GetImportId()))
	}
	return &hostv1.RevokeServiceImportResponse{}, nil
}

func (controller *Owner) applyServiceProjection(ctx context.Context, requestID string, projection *vmmdv1.ServiceProjection) (*vmmdv1.ApplyServiceProjectionResponse, error) {
	client, err := controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	return client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{RequestId: requestID, Projection: projection})
}

func (controller *Owner) DeleteServiceProjection(ctx context.Context, requestID, projectionID string) error {
	controller.egress.DeleteServiceRoute(projectionID)
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	_, err = client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: requestID, ProjectionId: projectionID})
	return err
}

func (controller *Owner) InvalidateStaleServiceExports(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID, containerID, currentInstanceID string) error {
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return err
	}

	defer controller.projectionMu.Unlock()
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return err
	}
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
	generation := ""
	if err == nil {
		generation = observed.GetContainer().GetGenerationId()
	} else if status.Code(err) != codes.NotFound {
		return err
	}
	return controller.ProjectServiceContainerLocked(ctx, client, environmentID, containerID, generation, currentInstanceID)
}

func (controller *Owner) ValidateContainerInstance(ctx context.Context, namespaceID, containerID, instanceID string) error {
	_, err := controller.ServiceContainerIdentity(ctx, namespaceID, containerID, instanceID)
	return err
}

func (controller *Owner) ServiceContainerIdentity(ctx context.Context, namespaceID, containerID, instanceID string) (*vmmdv1.Container, error) {
	client, err := controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	container, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: namespaceID, ContainerId: containerID})
	if err != nil {
		return nil, err
	}
	if container.GetContainer().GetInstanceId() != instanceID || container.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return nil, errors.New("container instance was replaced")
	}
	return container.GetContainer(), nil
}

func hostServiceRouteGeneration(digest [32]byte) uint64 {
	generation := binary.BigEndian.Uint64(digest[:8]) & (1<<63 - 1)
	if generation == 0 {
		return 1
	}
	return generation
}

func ValidVirtualServiceName(name string) bool { return virtualServiceName.MatchString(name) }
