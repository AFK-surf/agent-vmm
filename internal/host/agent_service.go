//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net/netip"
	"regexp"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var virtualServiceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)

const serviceTunnelAudience = "agent-vmm/service-tunnel/1"

const localServiceRouteLease = 24 * time.Hour

func (server *agentServer) CreateServiceExport(ctx context.Context, request *hostv1.CreateServiceExportRequest) (*servicev1.ServiceExport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if identity.RegistrationID != "" {
		unlock := server.controller.lockIntegration("service-domain:" + identity.RegistrationID)
		defer unlock()
	}
	if request.GetRequestId() == "" || request.GetContainerId() == "" || request.GetContainerInstanceId() == "" || request.GetPort() == 0 || request.GetPort() > 65535 ||
		(request.GetProtocol() != servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP && request.GetProtocol() != servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP) {
		return nil, invalidField("request_id/container_instance/port/protocol", "complete current service export is required")
	}
	observed, err := server.controller.serviceContainerIdentity(ctx, identity.NamespaceID, request.GetContainerId(), request.GetContainerInstanceId())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "CONTAINER_INSTANCE_STALE", "container", request.GetContainerId(), err.Error())
	}
	exportID, err := randomID()
	if err != nil {
		return nil, err
	}
	value, err := server.controller.store.CreateServiceExport(ctx, request.GetRequestId(), &servicev1.ServiceExport{ExportId: exportID,
		EnvironmentId: identity.EnvironmentID, ContainerId: request.GetContainerId(), ContainerInstanceId: request.GetContainerInstanceId(),
		Port: request.GetPort(), Protocol: request.GetProtocol(), ContainerGenerationId: observed.GetGenerationId()})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (server *agentServer) ListServiceExports(ctx context.Context, _ *hostv1.ListOwnServiceExportsRequest) (*servicev1.ListServiceExportsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	values, err := server.controller.store.ListServiceExports(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, err
	}
	approved, err := server.controller.store.ListApprovedServiceExports(ctx, identity.RegistrationID)
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
	return response, nil
}

func (server *agentServer) GetServiceExport(ctx context.Context, request *hostv1.GetOwnServiceExportRequest) (*servicev1.ServiceExport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	value, err := server.controller.store.GetServiceExport(ctx, request.GetExportId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_NOT_FOUND", "service_export", request.GetExportId(), "service export does not exist")
	}
	if value.GetEnvironmentId() != identity.EnvironmentID {
		approved, err := server.controller.store.ServiceExportApproved(ctx, identity.RegistrationID, value.GetExportId())
		if err != nil {
			return nil, err
		}
		if !approved {
			return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_NOT_FOUND", "service_export", request.GetExportId(), "service export does not exist")
		}
	}
	return value, nil
}

func (server *agentServer) RevokeServiceExport(ctx context.Context, request *hostv1.RevokeOwnServiceExportRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExportId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/export_id/expected_revision", "complete export revoke is required")
	}
	unlock := server.controller.lockIntegration("service-export:" + request.GetExportId())
	defer unlock()
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	exported, err := server.controller.store.GetServiceExport(ctx, request.GetExportId())
	if err != nil || exported.GetEnvironmentId() != identity.EnvironmentID || exported.GetRevoked() || exported.GetRevision() != request.GetExpectedRevision() {
		return nil, resourceError(codes.Aborted, "SERVICE_EXPORT_REVISION_CONFLICT", "service_export", request.GetExportId(), "export owner or revision changed")
	}
	imports, err := server.controller.store.ListActiveServiceImportsForExport(ctx, request.GetExportId())
	if err != nil {
		return nil, err
	}
	for _, importID := range imports {
		if err := server.controller.deleteServiceProjection(ctx, request.GetRequestId()+":"+importID, importID); err != nil {
			return nil, resourceError(codes.Unavailable, "SERVICE_PROJECTION_DELETE_FAILED", "service_import", importID, err.Error())
		}
	}
	if err := server.controller.store.RevokeServiceExport(ctx, identity.EnvironmentID, request.GetExportId(), request.GetExpectedRevision()); err != nil {
		return nil, resourceError(codes.Aborted, "SERVICE_EXPORT_REVISION_CONFLICT", "service_export", request.GetExportId(), err.Error())
	}
	if err := server.controller.restoreContainerServiceListeners(ctx, identity.EnvironmentID, exported.GetContainerId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) RevokeServiceImport(ctx context.Context, request *hostv1.RevokeServiceImportRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetImportId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/import_id/expected_revision", "complete import revoke is required")
	}
	unlock := server.controller.lockIntegration("service-import:" + request.GetImportId())
	defer unlock()
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	value, err := server.controller.store.GetServiceImport(ctx, request.GetImportId())
	if err != nil || value.GetSourceEnvironmentId() != identity.EnvironmentID || value.GetRevoked() || value.GetRevision() != request.GetExpectedRevision() {
		return nil, resourceError(codes.Aborted, "SERVICE_IMPORT_REVISION_CONFLICT", "service_import", request.GetImportId(), "import owner or revision changed")
	}
	if err := server.controller.deleteServiceProjection(ctx, request.GetRequestId(), request.GetImportId()); err != nil {
		return nil, resourceError(codes.Unavailable, "SERVICE_PROJECTION_DELETE_FAILED", "service_import", request.GetImportId(), err.Error())
	}
	server.controller.egress.DeleteServiceRoute(request.GetImportId())
	if err := server.controller.store.RevokeServiceImport(ctx, identity.EnvironmentID, request.GetImportId(), request.GetExpectedRevision()); err != nil {
		return nil, resourceError(codes.Aborted, "SERVICE_IMPORT_REVISION_CONFLICT", "service_import", request.GetImportId(), err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (controller *Controller) applyServiceProjection(ctx context.Context, requestID string, projection *vmmdv1.ServiceProjection) (*vmmdv1.ApplyServiceProjectionResponse, error) {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{RequestId: requestID, Projection: projection})
}

func remoteServiceAddress(id string) netip.Addr {
	digest := sha256.Sum256([]byte(id))
	return netip.AddrFrom4([4]byte{198, 19, digest[0], 1 + byte(binary.BigEndian.Uint16(digest[1:3])%254)})
}

func (controller *Controller) deleteServiceProjection(ctx context.Context, requestID, projectionID string) error {
	controller.egress.DeleteServiceRoute(projectionID)
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, err = client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: requestID, ProjectionId: projectionID})
	return err
}

func (controller *Controller) invalidateStaleServiceExports(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID, containerID, currentInstanceID string) error {
	controller.projectionMu.Lock()
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
	return controller.projectServiceContainerLocked(ctx, client, environmentID, containerID, generation, currentInstanceID)
}

func (controller *Controller) validateContainerInstance(ctx context.Context, namespaceID, containerID, instanceID string) error {
	_, err := controller.serviceContainerIdentity(ctx, namespaceID, containerID, instanceID)
	return err
}

func (controller *Controller) serviceContainerIdentity(ctx context.Context, namespaceID, containerID, instanceID string) (*vmmdv1.Container, error) {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
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
