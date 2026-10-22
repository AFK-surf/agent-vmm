//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
		request.GetDestinationDeviceId() == "" || request.GetDestinationExportId() == "" || !virtualServiceName.MatchString(request.GetVirtualServiceName()) {
		return nil, invalidField("service_import", "source, destination, and virtual service name are required")
	}
	sourceContainer, err := server.controller.serviceContainerIdentity(ctx, identity.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "SOURCE_CONTAINER_INSTANCE_STALE", "container", request.GetSourceContainerId(), err.Error())
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.FailedPrecondition, "TRUST_UNAVAILABLE", "device", "local", "device trust is not configured")
	}
	device, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "DEVICE_IDENTITY_UNAVAILABLE", "device", "local", "device identity is unavailable")
	}
	effectiveRequest := request
	if request.GetDestinationDeviceId() == "local" {
		effectiveRequest = proto.Clone(request).(*hostv1.CreateServiceImportRequest)
		effectiveRequest.DestinationDeviceId = device.Identity.GetDeviceId()
	}
	localDestination := effectiveRequest.GetDestinationDeviceId() == device.Identity.GetDeviceId()
	var destination *servicev1.ServiceExport
	var endpoint *trustv1.EndpointObservation
	var destinationEnvironment store.Environment
	if localDestination {
		destination, err = server.controller.store.GetServiceExport(ctx, effectiveRequest.GetDestinationExportId())
		if err != nil || destination.GetRevoked() {
			return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_NOT_FOUND", "service_export", effectiveRequest.GetDestinationExportId(), "destination export is unavailable")
		}
		destinationEnvironment, err = server.controller.store.GetEnvironment(ctx, destination.GetEnvironmentId())
		if err != nil {
			return nil, resourceError(codes.NotFound, "SERVICE_EXPORT_ENVIRONMENT_NOT_FOUND", "service_export", effectiveRequest.GetDestinationExportId(), "destination export environment is unavailable")
		}
		if err := server.controller.validateContainerInstance(ctx, destinationEnvironment.NamespaceID, destination.GetContainerId(), destination.GetContainerInstanceId()); err != nil {
			if status.Code(err) == codes.Unavailable || errors.Is(err, context.DeadlineExceeded) {
				return nil, resourceError(codes.Unavailable, "SERVICE_PROJECTION_APPLY_FAILED", "service_import", effectiveRequest.GetDestinationExportId(), "destination runtime is unavailable while validating the exported container")
			}
			return nil, resourceError(codes.FailedPrecondition, "DESTINATION_CONTAINER_INSTANCE_STALE", "container", destination.GetContainerId(), "destination export references a stopped or replaced container instance")
		}
		if effectiveRequest.GetDestinationPort() != 0 {
			return nil, invalidField("destination_port", "destination_port must be omitted for a local route")
		}
		if !sameServiceDomain(identity.EnvironmentID, identity.RegistrationID, &destinationEnvironment) {
			approved, err := server.controller.store.ServiceExportApproved(ctx, identity.RegistrationID, destination.GetExportId())
			if err != nil {
				return nil, err
			}
			if !approved {
				return nil, resourceError(codes.PermissionDenied, "CROSS_DOMAIN_SERVICE_ROUTE", "service_export", effectiveRequest.GetDestinationExportId(), "service route destination is outside the approved controller domain")
			}
		}
	} else {
		if effectiveRequest.GetDestinationPort() == 0 || effectiveRequest.GetDestinationPort() > 65535 || server.controller.dataWorkerSocket == "" {
			return nil, resourceError(codes.FailedPrecondition, "REMOTE_SERVICE_TRANSPORT_UNAVAILABLE", "device", effectiveRequest.GetDestinationDeviceId(), "remote destination port and data worker are required")
		}
		endpoint, err = server.controller.trust.GetEndpointObservation(ctx, effectiveRequest.GetDestinationDeviceId())
		if err != nil || endpoint.GetExpiresAt() == nil || !endpoint.GetExpiresAt().AsTime().After(time.Now()) {
			return nil, resourceError(codes.FailedPrecondition, "DESTINATION_ENDPOINT_STALE", "device", effectiveRequest.GetDestinationDeviceId(), "a current signed destination endpoint is required")
		}
		destination = &servicev1.ServiceExport{ExportId: effectiveRequest.GetDestinationExportId(), Port: effectiveRequest.GetDestinationPort(), Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}
	}
	capability := effectiveRequest.GetCapability()
	if capability == nil {
		if !localDestination {
			return nil, invalidField("capability", "a controller capability is required for a cross-device route")
		}
		capability = localServiceRouteCapability(effectiveRequest.GetRequestId(), device.Identity.GetDeviceId(), identity.AllocationID, destination.GetExportId(), destination.GetEnvironmentId() == identity.EnvironmentID)
	} else if err := server.controller.trust.AuthorizeRoute(ctx, capability, hosttrust.RouteExpectation{SourceDeviceID: device.Identity.GetDeviceId(),
		SourceAllocationID: identity.AllocationID, DestinationDeviceID: effectiveRequest.GetDestinationDeviceId(), DestinationExportID: destination.GetExportId(),
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
		SourceContainerId: effectiveRequest.GetSourceContainerId(), SourceContainerInstanceId: effectiveRequest.GetSourceContainerInstanceId(), SourceContainerGenerationId: sourceContainer.GetGenerationId(),
		DestinationDeviceId: effectiveRequest.GetDestinationDeviceId(), DestinationExportId: effectiveRequest.GetDestinationExportId(),
		VirtualServiceName: effectiveRequest.GetVirtualServiceName(), DestinationPort: destination.GetPort()}
	route := &servicev1.AccessRoute{RouteId: routeID, RouteClass: routeClass, SourceDeviceId: device.Identity.GetDeviceId(),
		DestinationDeviceId: effectiveRequest.GetDestinationDeviceId(), DestinationExportId: effectiveRequest.GetDestinationExportId(),
		State: servicev1.RouteState_ROUTE_STATE_PREPARING, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: expires,
		SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: effectiveRequest.GetSourceContainerInstanceId(), PolicyRevision: capability.GetPolicyRevision()}
	if identity.AllocationID != "" {
		route.SourceAllocationId = &identity.AllocationID
	}
	lease := &servicev1.RouteLease{LeaseId: leaseID, RouteId: routeID, RouteRevision: 1, Generation: capability.GetRouteGeneration(),
		ExpiresAt: expires, Capability: capability}
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	created, err := server.controller.store.CreateServiceImport(ctx, effectiveRequest.GetRequestId(), value, route, lease)
	if err != nil {
		return nil, resourceError(codes.Internal, "SERVICE_IMPORT_PERSIST_FAILED", "service_import", effectiveRequest.GetDestinationExportId(), "service import could not be recorded")
	}
	if created.GetRevoked() {
		return created, nil
	}
	ids, err := server.controller.store.ProjectServiceContainer(ctx, identity.EnvironmentID, sourceContainer.GetId(), sourceContainer.GetGenerationId(), sourceContainer.GetInstanceId())
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		server.controller.egress.DeleteServiceRoute(id)
		if err := server.controller.deleteServiceProjection(ctx, derivedRequestID("service-import-retire/"+id+"/"+sourceContainer.GetInstanceId()), id); err != nil {
			return nil, err
		}
	}
	record, err := server.controller.store.ServiceRouteForImport(ctx, created.GetImportId())
	if err != nil {
		return nil, err
	}
	created, capability, expires = record.Import, record.Capability, record.Lease.GetExpiresAt()
	routeID, leaseID = record.Route.GetRouteId(), record.Lease.GetLeaseId()
	sourceEnvironment, err := server.controller.store.GetEnvironment(ctx, created.GetSourceEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.Internal, "SERVICE_IMPORT_ENVIRONMENT_UNAVAILABLE", "environment", created.GetSourceEnvironmentId(), "source environment is unavailable")
	}
	projection := &vmmdv1.ServiceProjection{
		Id: created.GetImportId(), SourceNamespaceId: sourceEnvironment.NamespaceID, SourceContainerId: created.GetSourceContainerId(),
		SourceContainerInstanceId: created.GetSourceContainerInstanceId(), DestinationPort: destination.GetPort(),
		DnsAlias: created.GetVirtualServiceName(), ExpiresUnixMillis: expires.AsTime().UnixMilli(),
	}
	if localDestination {
		projection.DestinationNamespaceId = destinationEnvironment.NamespaceID
		projection.DestinationContainerId = destination.GetContainerId()
		projection.DestinationContainerInstanceId = destination.GetContainerInstanceId()
	} else {
		projection.Remote = true
		projection.VirtualAddress = remoteServiceAddress(created.GetImportId()).String()
	}
	projected, err := server.controller.applyServiceProjection(ctx, derivedRequestID(fmt.Sprintf("service-import-project/%s/%s/%s/%d", created.GetImportId(), created.GetSourceContainerInstanceId(), projection.GetDestinationContainerInstanceId(), expires.AsTime().UnixMilli())), projection)
	if err != nil {
		return nil, resourceError(codes.Unavailable, "SERVICE_PROJECTION_APPLY_FAILED", "service_import", created.GetImportId(), err.Error())
	}
	if !localDestination {
		sourceAddress, parseErr := netip.ParseAddr(projected.GetSourceAddress())
		header := &servicev1.OpenServiceForwardHeader{RouteId: routeID, LeaseId: leaseID, RouteGeneration: capability.GetRouteGeneration(),
			SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: created.GetSourceContainerInstanceId(), Capability: capability}
		if parseErr != nil || server.controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: created.GetImportId(), Generation: capability.GetRouteGeneration(), EnvironmentID: identity.EnvironmentID,
			SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(remoteServiceAddress(created.GetImportId()), uint16(destination.GetPort())), ExpiresAt: expires.AsTime(),
			Dial: func(routeContext context.Context) (net.Conn, error) {
				return server.controller.dialRemoteService(routeContext, header, endpoint)
			}}) != nil {
			_ = server.controller.deleteServiceProjection(ctx, effectiveRequest.GetRequestId()+":rollback", created.GetImportId())
			return nil, resourceError(codes.Unavailable, "SERVICE_ROUTE_APPLY_FAILED", "service_import", created.GetImportId(), "remote service route could not be installed")
		}
	}
	if err := server.controller.store.ActivateServiceProjection(ctx, created.GetImportId(), record.Route.GetGeneration()); err != nil {
		return nil, err
	}
	return created, nil
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
	generation := hostServiceRouteGeneration(digest)
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

func sameServiceDomain(sourceEnvironmentID, sourceRegistrationID string, destination *store.Environment) bool {
	if destination == nil {
		return false
	}
	if destination.ID == sourceEnvironmentID {
		return true
	}
	return sourceRegistrationID != "" && destination.RegistrationID != "" && destination.RegistrationID == sourceRegistrationID
}

func localServiceRouteCapability(requestID, deviceID, allocationID, exportID string, sameEnvironment bool) *trustv1.RouteCapability {
	digest := sha256.Sum256([]byte(requestID))
	generation := hostServiceRouteGeneration(digest)
	now := time.Now()
	capability := &trustv1.RouteCapability{
		CapabilityId:        "local-" + requestID,
		SourceDeviceId:      deviceID,
		DestinationDeviceId: deviceID,
		DestinationExportId: exportID,
		RouteGeneration:     generation,
		PolicyRevision:      1,
		NotBefore:           timestamppb.New(now),
		ExpiresAt:           timestamppb.New(now.Add(localServiceRouteLease)),
		Nonce:               digest[:],
		Audience:            serviceTunnelAudience,
		RouteClass:          routeClassValueForImport(true, sameEnvironment),
	}
	if allocationID != "" {
		capability.SourceAllocationId = &allocationID
	}
	return capability
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
		response.Imports = append(response.Imports, value)
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
	return value, nil
}
