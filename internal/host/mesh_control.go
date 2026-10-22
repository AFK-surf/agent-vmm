//go:build darwin || linux

package host

import (
	"context"
	"encoding/json"
	"errors"
	"net"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type meshControlServer struct {
	servicev1.UnimplementedMeshControlServiceServer
	controller *Controller
}

func (controller *Controller) NewMeshControlServer() (*grpc.Server, error) {
	if controller.trust == nil {
		return nil, errors.New("trust manager is required")
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32))
	servicev1.RegisterMeshControlServiceServer(server, &meshControlServer{controller: controller})
	return server, nil
}

func (server *meshControlServer) RequestPersonalRoute(ctx context.Context, request *servicev1.PersonalRouteRequest) (*servicev1.PersonalRouteResponse, error) {
	destination, err := server.controller.store.GetServiceExport(ctx, request.GetDestinationExportId())
	if err != nil || destination.GetRevoked() {
		return nil, status.Error(codes.NotFound, "destination service export is unavailable")
	}
	response, err := server.controller.trust.IssuePersonalRoute(ctx, request, destination)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "personal route request is unauthorized")
	}
	result := proto.Clone(response.DestinationExport).(*servicev1.ServiceExport)
	result.EnvironmentId, result.ContainerId, result.ContainerInstanceId = "", "", ""
	response.DestinationExport = result
	return response, nil
}

func (controller *Controller) requestPersonalRoute(ctx context.Context, observation *trustv1.EndpointObservation, request *servicev1.PersonalRouteRequest) (*servicev1.PersonalRouteResponse, error) {
	if controller.dataWorkerSocket == "" || observation == nil || len(observation.GetEndpointNodeId()) == 0 {
		return nil, errors.New("mesh control data path is unavailable")
	}
	endpoint, err := json.Marshal(map[string]string{"id": string(observation.GetEndpointNodeId())})
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient("passthrough:///agent-vmm-mesh-control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dialContext context.Context, _ string) (net.Conn, error) {
			return dataworker.DialMeshControlTunnel(dialContext, controller.dataWorkerSocket, endpoint)
		}))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return servicev1.NewMeshControlServiceClient(connection).RequestPersonalRoute(ctx, request)
}
