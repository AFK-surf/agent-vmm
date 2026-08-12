//go:build darwin

package host

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const connectorAuthorizationHeader = "authorization"

type trustServer struct {
	trustv1.UnimplementedTrustServiceServer
	controller *Controller
}

type serviceRouteServer struct {
	servicev1.UnimplementedServiceRouteServiceServer
	controller *Controller
}

func (controller *Controller) NewTrustServer(capability []byte) (*grpc.Server, error) {
	if controller.trust == nil || len(capability) < 32 {
		return nil, errors.New("trust manager and 256-bit connector capability are required")
	}
	authorize := func(ctx context.Context) error {
		values := metadata.ValueFromIncomingContext(ctx, connectorAuthorizationHeader)
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), capability) != 1 {
			return status.Error(codes.Unauthenticated, "connector capability is required")
		}
		return nil
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := authorize(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, request)
		}),
		grpc.StreamInterceptor(func(value any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := authorize(stream.Context()); err != nil {
				return err
			}
			return handler(value, stream)
		}),
	)
	trustv1.RegisterTrustServiceServer(server, &trustServer{controller: controller})
	servicev1.RegisterServiceRouteServiceServer(server, &serviceRouteServer{controller: controller})
	return server, nil
}

// NewServiceTunnelServer is served only behind servicepreface.Listener. It
// exposes destination forwarding, never trust mutation.
func (controller *Controller) NewServiceTunnelServer() (*grpc.Server, error) {
	if controller.trust == nil {
		return nil, errors.New("trust manager is required")
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32),
	)
	servicev1.RegisterServiceRouteServiceServer(server, &serviceRouteServer{controller: controller})
	return server, nil
}

func (server *serviceRouteServer) ListServiceExports(context.Context, *servicev1.ListServiceExportsRequest) (*servicev1.ListServiceExportsResponse, error) {
	return nil, status.Error(codes.PermissionDenied, "connector export enumeration is not permitted")
}

func (server *serviceRouteServer) RevokeServiceExport(context.Context, *servicev1.RevokeServiceExportRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.PermissionDenied, "connector export mutation is not permitted")
}

func (server *serviceRouteServer) GetServiceExport(ctx context.Context, request *servicev1.GetServiceExportRequest) (*servicev1.ServiceExport, error) {
	value, err := server.controller.store.GetServiceExport(ctx, request.GetExportId())
	if err != nil || value.GetRevoked() {
		return nil, status.Error(codes.NotFound, "service export is unavailable")
	}
	result := *value
	result.EnvironmentId, result.ContainerId, result.ContainerInstanceId = "", "", ""
	return &result, nil
}

func (server *serviceRouteServer) OpenServiceForward(stream servicev1.ServiceRouteService_OpenServiceForwardServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetRouteId() == "" || header.GetLeaseId() == "" || header.GetRouteGeneration() == 0 || header.GetCapability() == nil {
		return status.Error(codes.InvalidArgument, "the first service forward message must contain a complete route header")
	}
	record, routeErr := server.controller.store.GetServiceRoute(stream.Context(), header.GetRouteId(), header.GetLeaseId(), header.GetRouteGeneration())
	now := time.Now()
	identity, err := server.controller.trust.Identity(stream.Context())
	if err != nil || identity.State != store.DeviceIdentityReady || header.GetCapability().GetDestinationDeviceId() != identity.Identity.GetDeviceId() {
		return status.Error(codes.PermissionDenied, "service destination identity is stale")
	}
	capability := header.GetCapability()
	export, exportErr := server.controller.store.GetServiceExport(stream.Context(), capability.GetDestinationExportId())
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
	if err := server.controller.trust.AuthorizeRoute(stream.Context(), capability, hosttrust.RouteExpectation{
		SourceDeviceID: capability.GetSourceDeviceId(), SourceAllocationID: capability.GetSourceAllocationId(),
		DestinationDeviceID: identity.Identity.GetDeviceId(), DestinationExportID: export.GetExportId(),
		RouteClass: expectedForwardRouteClass(localRoute, record, header), Audience: serviceTunnelAudience,
		RouteGeneration: header.GetRouteGeneration(), PolicyRevision: capability.GetPolicyRevision(), ObservedAt: now,
	}); err != nil {
		return status.Error(codes.PermissionDenied, "service route capability is invalid")
	}
	budgetLease, err := server.controller.routeBudgets.acquire(capability, now)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "service route budget is exhausted")
	}
	defer budgetLease.release()
	if localRoute {
		source, sourceErr := server.controller.store.GetEnvironment(stream.Context(), record.Import.GetSourceEnvironmentId())
		if sourceErr != nil || server.controller.validateContainerInstance(stream.Context(), source.NamespaceID, record.Import.GetSourceContainerId(), record.Import.GetSourceContainerInstanceId()) != nil {
			return status.Error(codes.FailedPrecondition, "source container instance is stale")
		}
	}
	destination, err := server.controller.store.GetEnvironment(stream.Context(), export.GetEnvironmentId())
	if err != nil || server.controller.validateContainerInstance(stream.Context(), destination.NamespaceID, export.GetContainerId(), export.GetContainerInstanceId()) != nil {
		return status.Error(codes.FailedPrecondition, "destination container instance is stale")
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
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
				if !budgetLease.consume(uint64(len(payload.Data))) {
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
			return streamReceiveResult(receiveError)
		}
		if receiveErr != nil {
			return receiveErr
		}
		converted := &servicev1.OpenServiceForwardResponse{}
		switch event := response.GetEvent().(type) {
		case *vmmdv1.ForwardTCPResponse_Data:
			if !budgetLease.consume(uint64(len(event.Data))) {
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

func (server *trustServer) GetDeviceIdentity(ctx context.Context, _ *trustv1.GetDeviceIdentityRequest) (*trustv1.DeviceIdentity, error) {
	record, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	if record.State != store.DeviceIdentityReady {
		return nil, status.Error(codes.FailedPrecondition, "device identity needs repair")
	}
	return record.Identity, nil
}

func (server *trustServer) SignEndpointObservation(ctx context.Context, request *trustv1.SignEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	return server.controller.trust.SignEndpointObservation(ctx, request.GetUnsignedObservation())
}

func (server *trustServer) ApplyMeshEndpointObservation(ctx context.Context, request *trustv1.ApplyMeshEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	return server.controller.trust.ApplyMeshEndpointObservation(ctx, request.GetMeshId(), request.GetObservation())
}

func (server *trustServer) GetEndpointObservation(ctx context.Context, request *trustv1.GetEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	return server.controller.trust.GetEndpointObservation(ctx, request.GetDeviceId())
}

func (server *trustServer) ListPersonalMeshes(ctx context.Context, _ *trustv1.ListPersonalMeshesRequest) (*trustv1.ListPersonalMeshesResponse, error) {
	meshes, err := server.controller.trust.ListPersonalMeshes(ctx)
	if err != nil {
		return nil, err
	}
	return &trustv1.ListPersonalMeshesResponse{Meshes: meshes}, nil
}

func (server *trustServer) InstallManagedTrust(ctx context.Context, request *trustv1.InstallManagedTrustRequest) (*trustv1.InstallManagedTrustResponse, error) {
	if err := server.controller.trust.InstallManagedTrust(ctx, request.GetTrustAnchor(), request.GetMembershipCredential()); err != nil {
		return nil, status.Error(codes.PermissionDenied, "managed trust bundle is invalid")
	}
	return &trustv1.InstallManagedTrustResponse{
		AuthorityId:    request.GetTrustAnchor().GetAuthority().GetAuthorityId(),
		AnchorRevision: request.GetTrustAnchor().GetRevision(),
		PolicyRevision: request.GetMembershipCredential().GetPolicyRevision(),
	}, nil
}

func (server *trustServer) ApplyMeshSnapshot(ctx context.Context, request *trustv1.ApplyMeshSnapshotRequest) (*trustv1.ApplyMeshSnapshotResponse, error) {
	if request.GetSnapshot() == nil || request.GetExpectedRevision() == 0 || request.GetSnapshot().GetRevision() <= request.GetExpectedRevision() {
		return nil, status.Error(codes.InvalidArgument, "a post-genesis mesh snapshot is required")
	}
	if err := server.controller.trust.ApplyPersonalMeshSnapshot(ctx, request.GetSnapshot(), request.GetExpectedRevision()); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &trustv1.ApplyMeshSnapshotResponse{AppliedRevision: request.GetSnapshot().GetRevision()}, nil
}

func (server *trustServer) AuthorizeRoute(ctx context.Context, request *trustv1.AuthorizeRouteRequest) (*trustv1.AuthorizeRouteResponse, error) {
	observedAt := time.Now()
	if request.GetObservedAt() != nil {
		observedAt = request.GetObservedAt().AsTime()
	}
	capability := request.GetCapability()
	if server.controller.manager != nil && server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return &trustv1.AuthorizeRouteResponse{DenialCode: "LOCAL_RUNTIME_NOT_ACTIVE"}, nil
	}
	identity, identityErr := server.controller.trust.Identity(ctx)
	if identityErr != nil || identity.State != store.DeviceIdentityReady || request.GetExpectedDestinationDeviceId() != identity.Identity.GetDeviceId() {
		return &trustv1.AuthorizeRouteResponse{DenialCode: "DESTINATION_IDENTITY_MISMATCH"}, nil
	}
	err := server.controller.trust.AuthorizeRoute(ctx, capability, hosttrust.RouteExpectation{
		SourceDeviceID: request.GetExpectedSourceDeviceId(), SourceAllocationID: request.GetExpectedSourceAllocationId(),
		DestinationDeviceID: identity.Identity.GetDeviceId(), DestinationExportID: request.GetExpectedDestinationExportId(),
		RouteClass: request.GetExpectedRouteClass(), Audience: request.GetExpectedAudience(), RouteGeneration: request.GetExpectedRouteGeneration(),
		PolicyRevision: request.GetExpectedPolicyRevision(), ObservedAt: observedAt,
	})
	if err != nil {
		return &trustv1.AuthorizeRouteResponse{DenialCode: "ROUTE_UNAUTHORIZED"}, nil
	}
	return &trustv1.AuthorizeRouteResponse{Authorized: true, RemainingBudget: capability.GetBudget()}, nil
}

func (server *trustServer) PullRegistryOperations(ctx context.Context, request *trustv1.PullRegistryOperationsRequest) (*trustv1.PullRegistryOperationsResponse, error) {
	operations, err := server.controller.trust.PendingRegistryOperations(ctx, request.GetAfterSequence(), request.GetLimit())
	if err != nil {
		return nil, err
	}
	response := &trustv1.PullRegistryOperationsResponse{}
	for _, operation := range operations {
		response.Operations = append(response.Operations, &trustv1.PendingRegistryOperation{Sequence: operation.Sequence,
			Operation: operation.Operation, MeshDescriptor: operation.Descriptor})
	}
	return response, nil
}

func (server *trustServer) CommitRegistryOperation(ctx context.Context, request *trustv1.CommitRegistryOperationRequest) (*trustv1.ApplyMeshSnapshotResponse, error) {
	if request.GetOperationId() == "" || request.GetSnapshot() == nil {
		return nil, status.Error(codes.InvalidArgument, "operation ID and registry snapshot are required")
	}
	if err := server.controller.trust.CommitRegistryOperation(ctx, request.GetOperationId(), request.GetSnapshot()); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	server.controller.commitPairingOperation(ctx, request.GetOperationId())
	return &trustv1.ApplyMeshSnapshotResponse{AppliedRevision: request.GetSnapshot().GetRevision()}, nil
}
