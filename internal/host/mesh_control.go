//go:build darwin || linux

package host

import (
	"context"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"google.golang.org/grpc"
)

type meshControlServer struct {
	servicev1.UnimplementedMeshControlServiceServer
	controller *Controller
}

func (controller *Controller) NewMeshControlServer() (*grpc.Server, error) {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(32))
	servicev1.RegisterMeshControlServiceServer(server, &meshControlServer{controller: controller})
	return server, nil
}

func (server *meshControlServer) RequestPersonalRoute(ctx context.Context, request *servicev1.RequestPersonalRouteRequest) (*servicev1.RequestPersonalRouteResponse, error) {
	return server.controller.services.IssuePersonalRoute(ctx, request)
}
