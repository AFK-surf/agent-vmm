//go:build darwin || linux

package host

import (
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc"
)

type remoteComputeServer struct {
	connectorv1.UnimplementedRemoteComputeServiceServer
	controller *Controller
}

func (controller *Controller) NewRemoteComputeServer() *grpc.Server {
	limiter := newRPCRateLimiter(100, 200)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(64),
		grpc.ChainUnaryInterceptor(auth.RemoteConnectorUnaryInterceptor(), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
	)
	connectorv1.RegisterRemoteComputeServiceServer(server, &remoteComputeServer{controller: controller})
	return server
}
