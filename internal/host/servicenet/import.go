//go:build darwin || linux

package servicenet

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
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (controller *Owner) CreateServiceImport(ctx context.Context, request *hostv1.CreateServiceImportRequest) (*hostv1.CreateServiceImportResponse, error) {
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
	if request.GetRequestId() == "" || request.GetSourceContainerId() == "" || request.GetSourceContainerInstanceId() == "" ||
		request.GetDestinationDeviceId() == "" || request.GetDestinationExportId() == "" || !virtualServiceName.MatchString(request.GetVirtualServiceName()) {
		return nil, hostv1.InvalidField("service_import", "source, destination, and virtual service name are required")
	}
	sourceContainer, err := controller.ServiceContainerIdentity(ctx, identity.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId())
	if err != nil {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_SOURCE_CONTAINER_INSTANCE_STALE, err.Error(), hostv1.WithResource("container", request.GetSourceContainerId()))
	}
	device, err := controller.trust.Identity(ctx)
	if err != nil {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_DEVICE_IDENTITY_UNAVAILABLE, "device identity is unavailable", hostv1.WithResource("device", "local"))
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
		destination, err = controller.store.GetServiceExport(ctx, effectiveRequest.GetDestinationExportId())
		if err != nil || destination.GetRevoked() {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_SERVICE_EXPORT_NOT_FOUND, "destination export is unavailable", hostv1.WithResource("service_export", effectiveRequest.GetDestinationExportId()))
		}
		destinationEnvironment, err = controller.store.GetEnvironment(ctx, destination.GetEnvironmentId())
		if err != nil {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_SERVICE_EXPORT_ENVIRONMENT_NOT_FOUND, "destination export environment is unavailable", hostv1.WithResource("service_export", effectiveRequest.GetDestinationExportId()))
		}
		if err := controller.ValidateContainerInstance(ctx, destinationEnvironment.NamespaceID, destination.GetContainerId(), destination.GetContainerInstanceId()); err != nil {
			if status.Code(err) == codes.Unavailable || errors.Is(err, context.DeadlineExceeded) {
				return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_PROJECTION_APPLY_FAILED, "destination runtime is unavailable while validating the exported container", hostv1.WithResource("service_import", effectiveRequest.GetDestinationExportId()))
			}
			return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_DESTINATION_CONTAINER_INSTANCE_STALE, "destination export references a stopped or replaced container instance", hostv1.WithResource("container", destination.GetContainerId()))
		}
		if effectiveRequest.GetDestinationPort() != 0 {
			return nil, hostv1.InvalidField("destination_port", "destination_port must be omitted for a local route")
		}
		if !sameServiceDomain(identity.EnvironmentID, identity.RegistrationID, &destinationEnvironment) {
			approved, err := controller.store.ServiceExportApproved(ctx, identity.RegistrationID, destination.GetExportId())
			if err != nil {
				return nil, err
			}
			if !approved {
				return nil, hostv1.NewError(codes.PermissionDenied, hostv1.ErrorReason_ERROR_REASON_CROSS_DOMAIN_SERVICE_ROUTE, "service route destination is outside the approved controller domain", hostv1.WithResource("service_export", effectiveRequest.GetDestinationExportId()))
			}
		}
	} else {
		if effectiveRequest.GetDestinationPort() == 0 || effectiveRequest.GetDestinationPort() > 65535 || controller.dataWorkerSocket == "" {
			return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_REMOTE_SERVICE_TRANSPORT_UNAVAILABLE, "remote destination port and data worker are required", hostv1.WithResource("device", effectiveRequest.GetDestinationDeviceId()))
		}
		endpoint, err = controller.trust.GetEndpointObservation(ctx, effectiveRequest.GetDestinationDeviceId())
		if err != nil || endpoint.GetExpiresAt() == nil || !endpoint.GetExpiresAt().AsTime().After(time.Now()) {
			return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_DESTINATION_ENDPOINT_STALE, "a current signed destination endpoint is required", hostv1.WithResource("device", effectiveRequest.GetDestinationDeviceId()))
		}
		destination = &servicev1.ServiceExport{ExportId: effectiveRequest.GetDestinationExportId(), Port: effectiveRequest.GetDestinationPort(), Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}
	}
	capability := effectiveRequest.GetCapability()
	if capability == nil {
		if !localDestination {
			return nil, hostv1.InvalidField("capability", "a controller capability is required for a cross-device route")
		}
		capability = LocalServiceRouteCapability(effectiveRequest.GetRequestId(), device.Identity.GetDeviceId(), identity.AllocationID, destination.GetExportId(), destination.GetEnvironmentId() == identity.EnvironmentID)
	} else if err := controller.trust.AuthorizeRoute(ctx, capability, hosttrust.RouteExpectation{SourceDeviceID: device.Identity.GetDeviceId(),
		SourceAllocationID: identity.AllocationID, DestinationDeviceID: effectiveRequest.GetDestinationDeviceId(), DestinationExportID: destination.GetExportId(),
		RouteClass: routeClassValueForImport(localDestination, destination.GetEnvironmentId() == identity.EnvironmentID),
		Audience:   ServiceTunnelAudience, RouteGeneration: capability.GetRouteGeneration(), PolicyRevision: capability.GetPolicyRevision()}); err != nil {
		return nil, hostv1.NewError(codes.PermissionDenied, hostv1.ErrorReason_ERROR_REASON_ROUTE_CAPABILITY_DENIED, err.Error(), hostv1.WithResource("service_export", destination.GetExportId()))
	}
	importID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	routeID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	leaseID, err := opreceipt.NewID()
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
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	created, err := controller.store.CreateServiceImport(ctx, effectiveRequest.GetRequestId(), value, route, lease)
	if err != nil {
		return nil, status.Error(codes.Internal, "service import could not be recorded")
	}
	if created.GetRevoked() {
		return &hostv1.CreateServiceImportResponse{Import: created}, nil
	}
	ids, err := controller.store.ProjectServiceContainer(ctx, identity.EnvironmentID, sourceContainer.GetId(), sourceContainer.GetGenerationId(), sourceContainer.GetInstanceId())
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		controller.egress.DeleteServiceRoute(id)
		if err := controller.DeleteServiceProjection(ctx, opreceipt.DerivedRequestID("service-import-retire/"+id+"/"+sourceContainer.GetInstanceId()), id); err != nil {
			return nil, err
		}
	}
	record, err := controller.store.ServiceRouteForImport(ctx, created.GetImportId())
	if err != nil {
		return nil, err
	}
	created, capability, expires = record.Import, record.Capability, record.Lease.GetExpiresAt()
	routeID, leaseID = record.Route.GetRouteId(), record.Lease.GetLeaseId()
	sourceEnvironment, err := controller.store.GetEnvironment(ctx, created.GetSourceEnvironmentId())
	if err != nil {
		return nil, status.Error(codes.Internal, "source environment is unavailable")
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
		projection.VirtualAddress = egresslink.ServiceAddress(created.GetImportId()).String()
	}
	projected, err := controller.applyServiceProjection(ctx, opreceipt.DerivedRequestID(fmt.Sprintf("service-import-project/%s/%s/%s/%d", created.GetImportId(), created.GetSourceContainerInstanceId(), projection.GetDestinationContainerInstanceId(), expires.AsTime().UnixMilli())), projection)
	if err != nil {
		return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_PROJECTION_APPLY_FAILED, err.Error(), hostv1.WithResource("service_import", created.GetImportId()))
	}
	if !localDestination {
		sourceAddress, parseErr := netip.ParseAddr(projected.GetSourceAddress())
		header := &servicev1.OpenServiceForwardHeader{RouteId: routeID, LeaseId: leaseID, RouteGeneration: capability.GetRouteGeneration(),
			SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: created.GetSourceContainerInstanceId(), Capability: capability}
		if parseErr != nil || controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: created.GetImportId(), Generation: capability.GetRouteGeneration(), EnvironmentID: identity.EnvironmentID,
			SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(egresslink.ServiceAddress(created.GetImportId()), uint16(destination.GetPort())), ExpiresAt: expires.AsTime(),
			Dial: func(routeContext context.Context) (net.Conn, error) {
				return controller.dialRemoteService(routeContext, header, endpoint)
			}}) != nil {
			_ = controller.DeleteServiceProjection(ctx, effectiveRequest.GetRequestId()+":rollback", created.GetImportId())
			return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_ROUTE_APPLY_FAILED, "remote service route could not be installed", hostv1.WithResource("service_import", created.GetImportId()))
		}
	}
	if err := controller.store.ActivateServiceProjection(ctx, created.GetImportId(), record.Route.GetGeneration()); err != nil {
		return nil, err
	}
	return &hostv1.CreateServiceImportResponse{Import: created}, nil
}

func (controller *Owner) CreatePersonalServiceImport(ctx context.Context, request *hostv1.CreatePersonalServiceImportRequest) (*hostv1.CreatePersonalServiceImportResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	budget := request.GetBudget()
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetSourceContainerId() == "" ||
		request.GetSourceContainerInstanceId() == "" || request.GetDestinationDeviceId() == "" || request.GetDestinationExportId() == "" ||
		!virtualServiceName.MatchString(request.GetVirtualServiceName()) || budget.GetConnectionLimit() == 0 ||
		budget.GetConcurrencyLimit() == 0 || budget.GetByteLimit() == 0 {
		return nil, hostv1.InvalidField("personal_service_import", "complete personal mesh service import and budget are required")
	}
	if err := controller.ValidateContainerInstance(ctx, identity.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId()); err != nil {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_SOURCE_CONTAINER_INSTANCE_STALE, err.Error(), hostv1.WithResource("container", request.GetSourceContainerId()))
	}
	device, err := controller.trust.Identity(ctx)
	if err != nil || request.GetDestinationDeviceId() == device.Identity.GetDeviceId() {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_PERSONAL_ROUTE_DESTINATION_INVALID, "personal route must target another active mesh device", hostv1.WithResource("device", request.GetDestinationDeviceId()))
	}
	endpoint, err := controller.trust.GetEndpointObservation(ctx, request.GetDestinationDeviceId())
	if err != nil {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_DESTINATION_ENDPOINT_STALE, "a current signed destination endpoint is required", hostv1.WithResource("device", request.GetDestinationDeviceId()))
	}
	digest := sha256.Sum256([]byte(request.GetRequestId()))
	generation := hostServiceRouteGeneration(digest)
	unsigned := &servicev1.RequestPersonalRouteRequest{RequestId: request.GetRequestId(), MeshId: request.GetMeshId(),
		SourceEnvironmentId: identity.EnvironmentID, SourceContainerInstanceId: request.GetSourceContainerInstanceId(),
		DestinationExportId: request.GetDestinationExportId(), RouteGeneration: generation,
		Budget: proto.Clone(budget).(*trustv1.RouteBudget), ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute)), Nonce: digest[:],
		RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE}
	signed, err := controller.trust.SignPersonalRouteRequest(ctx, unsigned)
	if err != nil {
		return nil, hostv1.NewError(codes.PermissionDenied, hostv1.ErrorReason_ERROR_REASON_PERSONAL_ROUTE_REQUEST_DENIED, "source mesh membership is not current", hostv1.WithResource("mesh", request.GetMeshId()))
	}
	response, err := controller.requestPersonalRoute(ctx, endpoint, signed)
	if err != nil || response.GetDestinationExport().GetPort() == 0 {
		return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_PERSONAL_ROUTE_RESULT_UNKNOWN, "read current service facts before retrying the same request id", hostv1.WithResource("device", request.GetDestinationDeviceId()))
	}
	value, err := controller.CreateServiceImport(ctx, &hostv1.CreateServiceImportRequest{RequestId: request.GetRequestId(),
		SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(),
		DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(),
		DestinationPort: response.GetDestinationExport().GetPort(), VirtualServiceName: request.GetVirtualServiceName(), Capability: response.GetCapability()})
	return &hostv1.CreatePersonalServiceImportResponse{Import: value.GetImport()}, err
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

func LocalServiceRouteCapability(requestID, deviceID, allocationID, exportID string, sameEnvironment bool) *trustv1.RouteCapability {
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
		Audience:            ServiceTunnelAudience,
		RouteClass:          routeClassValueForImport(true, sameEnvironment),
	}
	if allocationID != "" {
		capability.SourceAllocationId = &allocationID
	}
	return capability
}

func (controller *Owner) ListServiceImports(ctx context.Context, _ *hostv1.ListServiceImportsRequest) (*hostv1.ListServiceImportsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	values, err := controller.store.ListServiceImports(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListServiceImportsResponse{}
	for _, value := range values {
		response.Imports = append(response.Imports, value)
	}
	return response, nil
}
