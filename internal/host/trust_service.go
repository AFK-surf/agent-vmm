//go:build darwin || linux

package host

import (
	"context"
	"errors"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type trustServer struct {
	connectorv1.UnimplementedTrustServiceServer
	controller *Controller
}

type serviceRouteServer struct {
	servicev1.UnimplementedServiceRouteServiceServer
	controller *Controller
}

func (controller *Controller) NewTrustServer() (*grpc.Server, error) {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32))
	connectorv1.RegisterTrustServiceServer(server, &trustServer{controller: controller})
	servicev1.RegisterServiceRouteServiceServer(server, &serviceRouteServer{controller: controller})
	return server, nil
}

// NewServiceTunnelServer exposes destination forwarding on the private service socket.
func (controller *Controller) NewServiceTunnelServer() (*grpc.Server, error) {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32),
	)
	servicev1.RegisterServiceRouteServiceServer(server, &serviceRouteServer{controller: controller})
	return server, nil
}

func (server *serviceRouteServer) OpenServiceForward(stream servicev1.ServiceRouteService_OpenServiceForwardServer) error {
	return server.controller.services.OpenServiceForward(stream)
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
		// A validation failure names only its bounded stage and reason, such as a clock outside
		// the anchor window; other failures keep the generic message.
		if errors.Is(err, hosttrust.ErrRouteUnauthorized) {
			return nil, status.Error(codes.PermissionDenied, "managed trust bundle is invalid: "+err.Error())
		}
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
	server.controller.services.CommitPairingOperation(ctx, request.GetOperationId())
	return &trustv1.ApplyMeshSnapshotResponse{AppliedRevision: request.GetSnapshot().GetRevision()}, nil
}
