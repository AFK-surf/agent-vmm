//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *applianceAdminServer) List(ctx context.Context, _ *appliancev1.ListAppliancesRequest) (*appliancev1.ListAppliancesResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	installations, err := server.controller.store.ListApplianceInstallations(ctx)
	if err != nil {
		return nil, err
	}
	runtime := server.controller.systemApplianceProjection(ctx)
	response := new(appliancev1.ListAppliancesResponse)
	for _, installation := range installations {
		response.Appliances = append(response.Appliances, server.applianceStatus(ctx, installation, runtime[installation.ID]))
	}
	return response, nil
}

func (server *applianceAdminServer) GetStatus(ctx context.Context, request *appliancev1.GetApplianceStatusRequest) (*appliancev1.ApplianceStatusResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	installation, err := server.controller.store.GetApplianceInstallation(ctx, request.GetInstallationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "appliance installation was not found")
	}
	if err != nil {
		return nil, err
	}
	var runtime *vmmdv1.SystemAppliance
	var observedStopped *bool
	if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		client, connection, queryErr := server.controller.guestClient(ctx)
		if queryErr == nil {
			defer connection.Close()
			runtime, queryErr = getSystemAppliance(ctx, client, installation.ID)
			if queryErr == nil {
				stopped := runtime == nil || runtime.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED
				observedStopped = &stopped
			}
		}
	}
	projection := server.applianceStatus(ctx, installation, runtime)
	projection.RuntimeStopped = observedStopped
	return &appliancev1.ApplianceStatusResponse{Status: projection}, nil
}

func (server *applianceAdminServer) WatchOperation(request *appliancev1.WatchApplianceOperationRequest, stream grpc.ServerStreamingServer[appliancev1.ApplianceOperationResponse]) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		operation, err := server.controller.store.GetApplianceOperation(stream.Context(), request.GetOperationId())
		if errors.Is(err, sql.ErrNoRows) || (err == nil && operation.InstallationID != request.GetInstallationId()) {
			return status.Error(codes.NotFound, "appliance operation was not found")
		}
		if err != nil {
			return err
		}
		if err := stream.Send(applianceOperationResponse(operation)); err != nil {
			return err
		}
		if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) ||
			operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL) {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}
