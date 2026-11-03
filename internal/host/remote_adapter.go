//go:build darwin || linux

package host

import (
	"context"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *remoteComputeServer) GetRemoteState(ctx context.Context, request *connectorv1.GetRemoteStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	return server.controller.remote.GetRemoteState(ctx, request)
}
func (server *remoteComputeServer) GetRemoteObservation(ctx context.Context, request *connectorv1.GetRemoteObservationRequest) (*connectorv1.GetRemoteObservationResponse, error) {
	return server.controller.remote.GetRemoteObservation(ctx, request)
}
func (server *remoteComputeServer) SetConnectionState(ctx context.Context, request *connectorv1.SetConnectionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	return server.controller.remote.SetConnectionState(ctx, request)
}
func (server *remoteComputeServer) SetRemoteAdmissionState(ctx context.Context, request *connectorv1.SetRemoteAdmissionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	return server.controller.remote.SetRemoteAdmissionState(ctx, request)
}
func (server *remoteComputeServer) ReportConnectorStatus(ctx context.Context, request *connectorv1.ReportConnectorStatusRequest) (*connectorv1.GetRemoteStateResponse, error) {
	return server.controller.remote.ReportConnectorStatus(ctx, request)
}
func (server *remoteComputeServer) OpenRuntimeSession(ctx context.Context, request *connectorv1.OpenRuntimeSessionRequest) (*connectorv1.OpenRuntimeSessionResponse, error) {
	return server.controller.remote.OpenRuntimeSession(ctx, request)
}
func (server *remoteComputeServer) RevokeRuntimeSession(ctx context.Context, request *connectorv1.RevokeRuntimeSessionRequest) (*emptypb.Empty, error) {
	return server.controller.remote.RevokeRuntimeSession(ctx, request)
}
func (server *remoteComputeServer) EnsureAllocation(ctx context.Context, request *connectorv1.EnsureAllocationRequest) (*connectorv1.AllocationResponse, error) {
	return server.controller.remote.EnsureAllocation(ctx, request)
}
func (server *remoteComputeServer) ReleaseAllocation(ctx context.Context, request *connectorv1.ReleaseAllocationRequest) (*connectorv1.AllocationResponse, error) {
	return server.controller.remote.ReleaseAllocation(ctx, request)
}
func (server *operatorServer) BeginRemoteRevocation(ctx context.Context, request *hostv1.BeginRemoteRevocationRequest) (*hostv1.BeginRemoteRevocationResponse, error) {
	return server.controller.remote.BeginRemoteRevocation(ctx, request)
}
func (server *operatorServer) CompleteRemoteRevocation(ctx context.Context, request *hostv1.CompleteRemoteRevocationRequest) (*hostv1.CompleteRemoteRevocationResponse, error) {
	return server.controller.remote.CompleteRemoteRevocation(ctx, request)
}
func (server *operatorServer) UpdateRemotePool(ctx context.Context, request *hostv1.UpdateRemotePoolRequest) (*hostv1.UpdateRemotePoolResponse, error) {
	return server.controller.remote.UpdateRemotePool(ctx, request)
}
func (server *operatorServer) SetRemoteRegistrationState(ctx context.Context, request *hostv1.SetRemoteRegistrationStateRequest) (*hostv1.SetRemoteRegistrationStateResponse, error) {
	return server.controller.remote.SetRemoteRegistrationState(ctx, request)
}
func (server *operatorServer) GetRemoteRegistration(ctx context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.GetRemoteRegistrationResponse, error) {
	return server.controller.remote.GetRemoteRegistration(ctx, request)
}
func (server *remoteComputeServer) DisposeAllocation(ctx context.Context, request *connectorv1.DisposeAllocationRequest) (*connectorv1.AllocationResponse, error) {
	return server.controller.remote.DisposeAllocation(ctx, request)
}
func (server *operatorServer) SignRegistrationRecovery(ctx context.Context, request *hostv1.SignRegistrationRecoveryRequest) (*hostv1.SignRegistrationRecoveryResponse, error) {
	return server.controller.remote.SignRegistrationRecovery(ctx, request)
}
func (server *remoteComputeServer) PlanCleanupAllocation(ctx context.Context, request *connectorv1.PlanCleanupAllocationRequest) (*connectorv1.AllocationCleanupPlanResponse, error) {
	return server.controller.cleanup.PlanCleanupAllocation(ctx, request)
}
func (server *remoteComputeServer) ExecuteCleanupAllocation(ctx context.Context, request *connectorv1.ExecuteCleanupAllocationRequest) (*connectorv1.AllocationResponse, error) {
	return server.controller.cleanup.ExecuteCleanupAllocation(ctx, request)
}

func (server *operatorServer) ApplyRemoteRegistration(ctx context.Context, request *hostv1.ApplyRemoteRegistrationRequest) (*hostv1.ApplyRemoteRegistrationResponse, error) {
	return server.controller.remote.ApplyRemoteRegistration(ctx, request)
}
