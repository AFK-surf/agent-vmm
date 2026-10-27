//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *operatorServer) BeginPersonalMeshPairing(ctx context.Context, request *hostv1.BeginPersonalMeshPairingRequest) (*hostv1.BeginPersonalMeshPairingResponse, error) {
	return server.controller.services.BeginPersonalMeshPairing(ctx, request)
}
func (server *operatorServer) JoinPersonalMeshPairing(ctx context.Context, request *hostv1.JoinPersonalMeshPairingRequest) (*hostv1.JoinPersonalMeshPairingResponse, error) {
	return server.controller.services.JoinPersonalMeshPairing(ctx, request)
}
func (server *operatorServer) GetPersonalMeshPairing(ctx context.Context, request *hostv1.GetPersonalMeshPairingRequest) (*hostv1.GetPersonalMeshPairingResponse, error) {
	return server.controller.services.GetPersonalMeshPairing(ctx, request)
}
func (server *operatorServer) ConfirmPersonalMeshPairing(ctx context.Context, request *hostv1.ConfirmPersonalMeshPairingRequest) (*hostv1.ConfirmPersonalMeshPairingResponse, error) {
	return server.controller.services.ConfirmPersonalMeshPairing(ctx, request)
}
func (server *operatorServer) RejectPersonalMeshPairing(ctx context.Context, request *hostv1.RejectPersonalMeshPairingRequest) (*hostv1.RejectPersonalMeshPairingResponse, error) {
	return server.controller.services.RejectPersonalMeshPairing(ctx, request)
}
