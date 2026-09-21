//go:build linux

package control

import (
	"context"
	"errors"

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
	prefix := "system appliance launch failed: "
	var stage runtimeStageError
	if errors.As(err, &stage) {
		prefix += "stage=" + stage.stage + ": "
	}
	message := prefix + string(boundedStatus(err))
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

func (server *Server) DeleteSystemApplianceData(ctx context.Context, request *vmmdv1.DeleteSystemApplianceDataRequest) (*vmmdv1.DeleteSystemApplianceDataResponse, error) {
	value, err := server.mutate(ctx, "delete-system-appliance-data", request, func() (any, error) {
		return &vmmdv1.DeleteSystemApplianceDataResponse{}, server.appliances.deleteData(ctx, request.GetInstallationId(), request.GetDataVolumeId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteSystemApplianceDataResponse), nil
}

func (server *Server) GetSystemAppliance(_ context.Context, request *vmmdv1.GetSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	appliance, err := server.appliances.get(request.GetInstallationId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.SystemApplianceResponse{Appliance: appliance}, nil
}

func (server *Server) ListSystemAppliances(_ context.Context, request *vmmdv1.ListSystemAppliancesRequest) (*vmmdv1.ListSystemAppliancesResponse, error) {
	appliances, next := server.appliances.list(request.GetAfterInstallationId(), int(request.GetLimit()))
	return &vmmdv1.ListSystemAppliancesResponse{Appliances: appliances, NextCursor: next}, nil
}
