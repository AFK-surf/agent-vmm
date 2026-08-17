//go:build linux

package control

import (
	"context"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) EnsureSystemAppliance(ctx context.Context, request *vmmdv1.EnsureSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	value, err := server.mutate(ctx, "ensure-system-appliance", request, func() (any, error) {
		appliance, ensureErr := server.appliances.ensure(ctx, request)
		return &vmmdv1.SystemApplianceResponse{Appliance: appliance}, ensureErr
	})
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, boundedSystemError(err))
	}
	return value.(*vmmdv1.SystemApplianceResponse), nil
}

func boundedSystemError(err error) string {
	message := "system appliance launch failed: " + string(boundedStatus(err))
	if len(message) > 1024 {
		message = message[:1024]
	}
	return message
}

func (server *Server) StopSystemAppliance(ctx context.Context, request *vmmdv1.StopSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	value, err := server.mutate(ctx, "stop-system-appliance", request, func() (any, error) {
		appliance, stopErr := server.appliances.stop(ctx, request)
		return &vmmdv1.SystemApplianceResponse{Appliance: appliance}, stopErr
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.SystemApplianceResponse), nil
}

func (server *Server) ListSystemAppliances(context.Context, *vmmdv1.ListSystemAppliancesRequest) (*vmmdv1.ListSystemAppliancesResponse, error) {
	return &vmmdv1.ListSystemAppliancesResponse{Appliances: server.appliances.list()}, nil
}
