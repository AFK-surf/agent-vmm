//go:build darwin || linux

package host

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *operatorServer) CreateMCPToken(ctx context.Context, _ *hostv1.CreateMCPTokenRequest) (*hostv1.CreateMCPTokenResponse, error) {
	token, err := server.controller.appliances.CreateMCPToken(ctx)
	if err != nil {
		return nil, err
	}
	return &hostv1.CreateMCPTokenResponse{Token: token}, nil
}

func (server *operatorServer) RotateMCPTokenKey(ctx context.Context, _ *hostv1.RotateMCPTokenKeyRequest) (*hostv1.RotateMCPTokenKeyResponse, error) {
	operation, err := server.controller.appliances.RotateMCPTokenKey(ctx)
	if err != nil {
		return nil, err
	}
	return &hostv1.RotateMCPTokenKeyResponse{OperationId: operation.GetOperationId()}, nil
}
