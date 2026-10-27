//go:build darwin || linux

package servicenet

import (
	"io"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Owner) OpenServiceForward(stream servicev1.ServiceRouteService_OpenServiceForwardServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetRouteId() == "" || header.GetLeaseId() == "" || header.GetRouteGeneration() == 0 || header.GetCapability() == nil {
		return status.Error(codes.InvalidArgument, "the first service forward message must contain a complete route header")
	}
	record, routeErr := controller.store.GetServiceRoute(stream.Context(), header.GetRouteId(), header.GetLeaseId(), header.GetRouteGeneration())
	now := time.Now()
	identity, err := controller.trust.Identity(stream.Context())
	if err != nil || identity.State != store.DeviceIdentityReady || header.GetCapability().GetDestinationDeviceId() != identity.Identity.GetDeviceId() {
		return status.Error(codes.PermissionDenied, "service destination identity is stale")
	}
	capability := header.GetCapability()
	export, exportErr := controller.store.GetServiceExport(stream.Context(), capability.GetDestinationExportId())
	if exportErr != nil || export.GetRevoked() {
		return status.Error(codes.PermissionDenied, "service export is unavailable")
	}
	localRoute := routeErr == nil
	if localRoute && (record.Route.GetState() != servicev1.RouteState_ROUTE_STATE_ACTIVE || record.Import.GetRevoked() || record.Export.GetRevoked() ||
		record.Route.GetRevision() != record.Lease.GetRouteRevision() || record.Route.GetGeneration() != record.Lease.GetGeneration() ||
		record.Route.GetExpiresAt().AsTime().Before(now) || record.Lease.GetExpiresAt().AsTime().Before(now) ||
		header.GetSourceEnvironmentId() != record.Import.GetSourceEnvironmentId() ||
		header.GetSourceContainerInstanceId() != record.Import.GetSourceContainerInstanceId() || !proto.Equal(capability, record.Capability)) {
		return status.Error(codes.PermissionDenied, "service route fence is stale")
	}
	if err := controller.trust.AuthorizeRoute(stream.Context(), capability, hosttrust.RouteExpectation{
		SourceAllocationID:  capability.GetSourceAllocationId(),
		DestinationDeviceID: identity.Identity.GetDeviceId(), DestinationExportID: export.GetExportId(),
		RouteClass: expectedForwardRouteClass(localRoute, record, header), Audience: ServiceTunnelAudience,
		RouteGeneration: header.GetRouteGeneration(), PolicyRevision: capability.GetPolicyRevision(),
	}); err != nil {
		return status.Error(codes.PermissionDenied, "service route capability is invalid")
	}
	budgetLease, err := controller.AcquireRouteBudget(capability, now)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "service route budget is exhausted")
	}
	defer budgetLease.Release()
	if localRoute {
		source, sourceErr := controller.store.GetEnvironment(stream.Context(), record.Import.GetSourceEnvironmentId())
		if sourceErr != nil || controller.ValidateContainerInstance(stream.Context(), source.NamespaceID, record.Import.GetSourceContainerId(), record.Import.GetSourceContainerInstanceId()) != nil {
			return status.Error(codes.FailedPrecondition, "source container instance is stale")
		}
	}
	destination, err := controller.store.GetEnvironment(stream.Context(), export.GetEnvironmentId())
	if err != nil || controller.ValidateContainerInstance(stream.Context(), destination.NamespaceID, export.GetContainerId(), export.GetContainerInstanceId()) != nil {
		return status.Error(codes.FailedPrecondition, "destination container instance is stale")
	}
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	guest, err := client.ForwardTCP(stream.Context())
	if err != nil {
		return err
	}
	if err := guest.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Header{Header: &vmmdv1.ForwardTCPHeader{
		ActorNamespaceId: destination.NamespaceID, ContainerId: export.GetContainerId(), ContainerInstanceId: export.GetContainerInstanceId(), Port: export.GetPort(),
	}}}); err != nil {
		return err
	}
	receiveError := make(chan error, 1)
	go func() {
		for {
			request, receiveErr := stream.Recv()
			if receiveErr == io.EOF {
				receiveError <- guest.CloseSend()
				return
			}
			if receiveErr != nil {
				receiveError <- receiveErr
				return
			}
			switch payload := request.GetPayload().(type) {
			case *servicev1.OpenServiceForwardRequest_Data:
				if !budgetLease.Consume(uint64(len(payload.Data))) {
					receiveError <- status.Error(codes.ResourceExhausted, "service route byte budget exceeded")
					return
				}
				if sendErr := guest.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Data{Data: payload.Data}}); sendErr != nil {
					receiveError <- sendErr
					return
				}
			case *servicev1.OpenServiceForwardRequest_CloseWrite:
				if sendErr := guest.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_CloseWrite{CloseWrite: payload.CloseWrite}}); sendErr != nil {
					receiveError <- sendErr
					return
				}
			default:
				receiveError <- status.Error(codes.InvalidArgument, "service route header may only appear once")
				return
			}
		}
	}()
	for {
		response, receiveErr := guest.Recv()
		if receiveErr == io.EOF {
			return guestconn.StreamReceiveResult(receiveError)
		}
		if receiveErr != nil {
			return receiveErr
		}
		converted := &servicev1.OpenServiceForwardResponse{}
		switch event := response.GetEvent().(type) {
		case *vmmdv1.ForwardTCPResponse_Data:
			if !budgetLease.Consume(uint64(len(event.Data))) {
				return status.Error(codes.ResourceExhausted, "service route byte budget exceeded")
			}
			converted.Payload = &servicev1.OpenServiceForwardResponse_Data{Data: event.Data}
		case *vmmdv1.ForwardTCPResponse_CloseRead:
			converted.Payload = &servicev1.OpenServiceForwardResponse_CloseWrite{CloseWrite: event.CloseRead}
		}
		if err := stream.Send(converted); err != nil {
			return err
		}
	}
}

func expectedForwardRouteClass(localRoute bool, record store.ServiceRouteRecord, header *servicev1.OpenServiceForwardHeader) string {
	if localRoute {
		return routeClassValue(record.Route.GetRouteClass())
	}
	if header.GetSourceEnvironmentId() != "" {
		return "compute"
	}
	return header.GetCapability().GetRouteClass()
}

func routeClassValue(value servicev1.RouteClass) string {
	switch value {
	case servicev1.RouteClass_ROUTE_CLASS_LOCAL:
		return "local"
	case servicev1.RouteClass_ROUTE_CLASS_DEVICE:
		return "device"
	case servicev1.RouteClass_ROUTE_CLASS_COMPUTE:
		return "compute"
	case servicev1.RouteClass_ROUTE_CLASS_LAN:
		return "lan"
	case servicev1.RouteClass_ROUTE_CLASS_PUBLIC_HTTP:
		return "public_http"
	default:
		return ""
	}
}
