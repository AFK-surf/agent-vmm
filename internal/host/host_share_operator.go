//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *operatorServer) ListHostShares(ctx context.Context, request *hostv1.ListHostSharesRequest) (*hostv1.ListHostSharesResponse, error) {
	return server.controller.hostShares.ListHostShares(ctx, request)
}

func (server *operatorServer) CreateHostShare(ctx context.Context, request *hostv1.CreateHostShareRequest) (*hostv1.CreateHostShareResponse, error) {
	return server.controller.hostShares.CreateHostShare(ctx, request)
}

func (server *operatorServer) RebindHostShare(ctx context.Context, request *hostv1.RebindHostShareRequest) (*hostv1.RebindHostShareResponse, error) {
	return server.controller.hostShares.RebindHostShare(ctx, request)
}

func (server *operatorServer) RevokeHostShare(ctx context.Context, request *hostv1.RevokeHostShareRequest) (*hostv1.RevokeHostShareResponse, error) {
	return server.controller.hostShares.RevokeHostShare(ctx, request)
}
