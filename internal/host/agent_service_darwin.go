//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"regexp"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var virtualServiceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)

const serviceTunnelAudience = "agent-vmm/service-tunnel/1"

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
	if err := server.controller.validateContainerInstance(ctx, identity.NamespaceID, request.GetContainerId(), request.GetContainerInstanceId()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "CONTAINER_INSTANCE_STALE", "container", request.GetContainerId(), err.Error())
	}
	exportID, err := randomID()
	if err != nil {
		return nil, err
	}
	value, err := server.controller.store.CreateServiceExport(ctx, request.GetRequestId(), &servicev1.ServiceExport{ExportId: exportID,
		EnvironmentId: identity.EnvironmentID, ContainerId: request.GetContainerId(), ContainerInstanceId: request.GetContainerInstanceId(),
		Port: request.GetPort(), Protocol: request.GetProtocol()})
	if err != nil {
		return nil, err
	}
	return serviceExportForAgent(value), nil
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
	response := &servicev1.ListServiceExportsResponse{}
	for _, value := range values {
		response.Exports = append(response.Exports, serviceExportForAgent(value))
	}
	return response, nil
}

func (server *agentServer) GetServiceExport(ctx context.Context, request *hostv1.GetOwnServiceExportRequest) (*servicev1.ServiceExport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	value, err := server.controller.store.GetServiceExport(ctx, request.GetExportId())
	if err != nil || value.GetEnvironmentId() != identity.EnvironmentID {
		return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_NOT_FOUND", "service_export", request.GetExportId(), "service export does not exist")
	}
	return serviceExportForAgent(value), nil
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
	return &emptypb.Empty{}, nil
}

func (server *agentServer) CreateServiceImport(ctx context.Context, request *hostv1.CreateServiceImportRequest) (*servicev1.ServiceImport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if identity.RegistrationID != "" {
		unlock := server.controller.lockIntegration("service-domain:" + identity.RegistrationID)
		defer unlock()
	}
	if request.GetRequestId() == "" || request.GetSourceContainerId() == "" || request.GetSourceContainerInstanceId() == "" ||
		request.GetDestinationDeviceId() == "" || request.GetDestinationExportId() == "" || !virtualServiceName.MatchString(request.GetVirtualServiceName()) || request.GetCapability() == nil {
		return nil, invalidField("service_import", "complete service import and route capability are required")
	}
	if err := server.controller.validateContainerInstance(ctx, identity.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "SOURCE_CONTAINER_INSTANCE_STALE", "container", request.GetSourceContainerId(), err.Error())
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.FailedPrecondition, "TRUST_UNAVAILABLE", "device", "local", "device trust is not configured")
	}
	device, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "DEVICE_IDENTITY_UNAVAILABLE", "device", "local", "device identity is unavailable")
	}
	localDestination := request.GetDestinationDeviceId() == device.Identity.GetDeviceId()
	var destination *servicev1.ServiceExport
	var endpoint *trustv1.EndpointObservation
	if localDestination {
		destination, err = server.controller.store.GetServiceExport(ctx, request.GetDestinationExportId())
		if err != nil || destination.GetRevoked() {
			return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_NOT_FOUND", "service_export", request.GetDestinationExportId(), "destination export is unavailable")
		}
		if identity.RegistrationID != "" {
			destinationEnvironment, environmentErr := server.controller.store.GetEnvironment(ctx, destination.GetEnvironmentId())
			if environmentErr != nil || destinationEnvironment.RegistrationID == "" || destinationEnvironment.RegistrationID != identity.RegistrationID {
				return nil, resourceError(codes.PermissionDenied, "CROSS_DOMAIN_SERVICE_ROUTE", "service_export", request.GetDestinationExportId(), "service route destination is outside the controller domain")
			}
		}
	} else {
		if request.GetDestinationPort() == 0 || request.GetDestinationPort() > 65535 || server.controller.dataWorkerSocket == "" {
			return nil, resourceError(codes.FailedPrecondition, "REMOTE_SERVICE_TRANSPORT_UNAVAILABLE", "device", request.GetDestinationDeviceId(), "remote destination port and data worker are required")
		}
		endpoint, err = server.controller.trust.GetEndpointObservation(ctx, request.GetDestinationDeviceId())
		if err != nil || endpoint.GetExpiresAt() == nil || !endpoint.GetExpiresAt().AsTime().After(time.Now()) {
			return nil, resourceError(codes.FailedPrecondition, "DESTINATION_ENDPOINT_STALE", "device", request.GetDestinationDeviceId(), "a current signed destination endpoint is required")
		}
		destination = &servicev1.ServiceExport{ExportId: request.GetDestinationExportId(), Port: request.GetDestinationPort(), Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}
	}
	capability := request.GetCapability()
	if err := server.controller.trust.AuthorizeRoute(ctx, capability, hosttrust.RouteExpectation{SourceDeviceID: device.Identity.GetDeviceId(),
		SourceAllocationID: identity.AllocationID, DestinationDeviceID: request.GetDestinationDeviceId(), DestinationExportID: destination.GetExportId(),
		RouteClass: routeClassValueForImport(localDestination, destination.GetEnvironmentId() == identity.EnvironmentID),
		Audience:   serviceTunnelAudience, RouteGeneration: capability.GetRouteGeneration(), PolicyRevision: capability.GetPolicyRevision(), ObservedAt: time.Now()}); err != nil {
		return nil, resourceError(codes.PermissionDenied, "ROUTE_CAPABILITY_DENIED", "service_export", destination.GetExportId(), err.Error())
	}
	importID, err := randomID()
	if err != nil {
		return nil, err
	}
	routeID, err := randomID()
	if err != nil {
		return nil, err
	}
	leaseID, err := randomID()
	if err != nil {
		return nil, err
	}
	routeClass := servicev1.RouteClass_ROUTE_CLASS_COMPUTE
	if localDestination && destination.GetEnvironmentId() == identity.EnvironmentID {
		routeClass = servicev1.RouteClass_ROUTE_CLASS_LOCAL
	}
	expires := capability.GetExpiresAt()
	value := &servicev1.ServiceImport{ImportId: importID, SourceEnvironmentId: identity.EnvironmentID,
		SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(),
		DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(),
		VirtualServiceName: request.GetVirtualServiceName(), DestinationPort: destination.GetPort()}
	route := &servicev1.AccessRoute{RouteId: routeID, RouteClass: routeClass, SourceDeviceId: device.Identity.GetDeviceId(),
		DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(),
		State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: expires,
		SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: request.GetSourceContainerInstanceId(), PolicyRevision: capability.GetPolicyRevision()}
	if identity.AllocationID != "" {
		route.SourceAllocationId = &identity.AllocationID
	}
	lease := &servicev1.RouteLease{LeaseId: leaseID, RouteId: routeID, RouteRevision: 1, Generation: capability.GetRouteGeneration(),
		ExpiresAt: expires, Capability: capability}
	created, err := server.controller.store.CreateServiceImport(ctx, request.GetRequestId(), value, route, lease)
	if err != nil {
		return nil, err
	}
	sourceEnvironment, err := server.controller.store.GetEnvironment(ctx, created.GetSourceEnvironmentId())
	if err != nil {
		return nil, err
	}
	projection := &vmmdv1.ServiceProjection{
		Id: created.GetImportId(), SourceNamespaceId: sourceEnvironment.NamespaceID, SourceContainerId: created.GetSourceContainerId(),
		SourceContainerInstanceId: created.GetSourceContainerInstanceId(), DestinationPort: destination.GetPort(),
		DnsAlias: created.GetVirtualServiceName(), ExpiresUnixMillis: expires.AsTime().UnixMilli(),
	}
	if localDestination {
		destinationEnvironment, environmentErr := server.controller.store.GetEnvironment(ctx, destination.GetEnvironmentId())
		if environmentErr != nil {
			return nil, environmentErr
		}
		projection.DestinationNamespaceId = destinationEnvironment.NamespaceID
		projection.DestinationContainerId = destination.GetContainerId()
		projection.DestinationContainerInstanceId = destination.GetContainerInstanceId()
	} else {
		projection.Remote = true
		projection.VirtualAddress = remoteServiceAddress(created.GetImportId()).String()
	}
	projected, err := server.controller.applyServiceProjection(ctx, request.GetRequestId(), projection)
	if err != nil {
		_ = server.controller.store.RevokeServiceImport(ctx, identity.EnvironmentID, created.GetImportId(), created.GetRevision())
		return nil, resourceError(codes.Unavailable, "SERVICE_PROJECTION_APPLY_FAILED", "service_import", created.GetImportId(), err.Error())
	}
	if !localDestination {
		sourceAddress, parseErr := netip.ParseAddr(projected.GetSourceAddress())
		header := &servicev1.OpenServiceForwardHeader{RouteId: routeID, LeaseId: leaseID, RouteGeneration: capability.GetRouteGeneration(),
			SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: created.GetSourceContainerInstanceId(), Capability: capability}
		if parseErr != nil || server.controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: created.GetImportId(), EnvironmentID: identity.EnvironmentID,
			SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(remoteServiceAddress(created.GetImportId()), uint16(destination.GetPort())), ExpiresAt: expires.AsTime(),
			Dial: func(routeContext context.Context) (net.Conn, error) {
				return server.controller.dialRemoteService(routeContext, header, endpoint)
			}}) != nil {
			_ = server.controller.deleteServiceProjection(ctx, request.GetRequestId()+":rollback", created.GetImportId())
			_ = server.controller.store.RevokeServiceImport(ctx, identity.EnvironmentID, created.GetImportId(), created.GetRevision())
			return nil, resourceError(codes.Unavailable, "SERVICE_ROUTE_APPLY_FAILED", "service_import", created.GetImportId(), "remote service route could not be installed")
		}
	}
	return serviceImportForAgent(created), nil
}

func (server *agentServer) CreatePersonalServiceImport(ctx context.Context, request *hostv1.CreatePersonalServiceImportRequest) (*servicev1.ServiceImport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	budget := request.GetBudget()
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetSourceContainerId() == "" ||
		request.GetSourceContainerInstanceId() == "" || request.GetDestinationDeviceId() == "" || request.GetDestinationExportId() == "" ||
		!virtualServiceName.MatchString(request.GetVirtualServiceName()) || budget.GetConnectionLimit() == 0 ||
		budget.GetConcurrencyLimit() == 0 || budget.GetByteLimit() == 0 {
		return nil, invalidField("personal_service_import", "complete personal mesh service import and budget are required")
	}
	if err := server.controller.validateContainerInstance(ctx, identity.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "SOURCE_CONTAINER_INSTANCE_STALE", "container", request.GetSourceContainerId(), err.Error())
	}
	device, err := server.controller.trust.Identity(ctx)
	if err != nil || request.GetDestinationDeviceId() == device.Identity.GetDeviceId() {
		return nil, resourceError(codes.FailedPrecondition, "PERSONAL_ROUTE_DESTINATION_INVALID", "device", request.GetDestinationDeviceId(), "personal route must target another active mesh device")
	}
	endpoint, err := server.controller.trust.GetEndpointObservation(ctx, request.GetDestinationDeviceId())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "DESTINATION_ENDPOINT_STALE", "device", request.GetDestinationDeviceId(), "a current signed destination endpoint is required")
	}
	digest := sha256.Sum256([]byte(request.GetRequestId()))
	generation := binary.BigEndian.Uint64(digest[:8])
	if generation == 0 {
		generation = 1
	}
	unsigned := &servicev1.PersonalRouteRequest{RequestId: request.GetRequestId(), MeshId: request.GetMeshId(),
		SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: request.GetSourceContainerInstanceId(),
		DestinationExportId: request.GetDestinationExportId(), RouteGeneration: generation,
		Budget: proto.Clone(budget).(*trustv1.RouteBudget), ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute)), Nonce: digest[:],
		RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE}
	signed, err := server.controller.trust.SignPersonalRouteRequest(ctx, unsigned)
	if err != nil {
		return nil, resourceError(codes.PermissionDenied, "PERSONAL_ROUTE_REQUEST_DENIED", "mesh", request.GetMeshId(), "source mesh membership is not current")
	}
	response, err := server.controller.requestPersonalRoute(ctx, endpoint, signed)
	if err != nil || response.GetDestinationExport().GetPort() == 0 {
		return nil, resourceError(codes.Unavailable, "PERSONAL_ROUTE_RESULT_UNKNOWN", "device", request.GetDestinationDeviceId(), "read current service facts before retrying the same request id")
	}
	return server.CreateServiceImport(ctx, &hostv1.CreateServiceImportRequest{RequestId: request.GetRequestId(),
		SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(),
		DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(),
		DestinationPort: response.GetDestinationExport().GetPort(), VirtualServiceName: request.GetVirtualServiceName(), Capability: response.GetCapability()})
}

func routeClassValueForImport(localDestination, sameEnvironment bool) string {
	if localDestination && sameEnvironment {
		return "local"
	}
	return "compute"
}

func (server *agentServer) ListServiceImports(ctx context.Context, _ *hostv1.ListServiceImportsRequest) (*hostv1.ListServiceImportsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	values, err := server.controller.store.ListServiceImports(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListServiceImportsResponse{}
	for _, value := range values {
		response.Imports = append(response.Imports, serviceImportForAgent(value))
	}
	return response, nil
}

func (server *agentServer) GetServiceImport(ctx context.Context, request *hostv1.GetServiceImportRequest) (*servicev1.ServiceImport, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	value, err := server.controller.store.GetServiceImport(ctx, request.GetImportId())
	if err != nil || value.GetSourceEnvironmentId() != identity.EnvironmentID {
		return nil, resourceError(codes.NotFound, "SERVICE_IMPORT_NOT_FOUND", "service_import", request.GetImportId(), "service import does not exist")
	}
	return serviceImportForAgent(value), nil
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
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, err = client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: requestID, ProjectionId: projectionID})
	return err
}

func (controller *Controller) validateContainerInstance(ctx context.Context, namespaceID, containerID, instanceID string) error {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	container, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: namespaceID, ContainerId: containerID})
	if err != nil {
		return err
	}
	if container.GetContainer().GetInstanceId() != instanceID || container.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return errors.New("container instance was replaced")
	}
	return nil
}

func serviceExportForAgent(value *servicev1.ServiceExport) *servicev1.ServiceExport {
	if value == nil {
		return nil
	}
	result := proto.Clone(value).(*servicev1.ServiceExport)
	result.EnvironmentId = ""
	return result
}

func serviceImportForAgent(value *servicev1.ServiceImport) *servicev1.ServiceImport {
	if value == nil {
		return nil
	}
	result := proto.Clone(value).(*servicev1.ServiceImport)
	result.SourceEnvironmentId = ""
	return result
}
