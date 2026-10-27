//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *operatorServer) ExecuteCleanup(ctx context.Context, request *hostv1.ExecuteCleanupRequest) (*hostv1.ExecuteCleanupResponse, error) {
	return server.controller.cleanup.ExecuteCleanup(ctx, request)
}
func (server *operatorServer) PlanDiscardEnvironment(ctx context.Context, request *hostv1.PlanDiscardEnvironmentRequest) (*hostv1.PlanDiscardEnvironmentResponse, error) {
	return server.controller.cleanup.PlanDiscardEnvironment(ctx, request)
}
func (server *operatorServer) PlanResetEnvironment(ctx context.Context, request *hostv1.PlanResetEnvironmentRequest) (*hostv1.PlanResetEnvironmentResponse, error) {
	return server.controller.cleanup.PlanResetEnvironment(ctx, request)
}
func (server *operatorServer) ExecuteLocalDisposal(ctx context.Context, request *hostv1.ExecuteLocalDisposalRequest) (*hostv1.ExecuteLocalDisposalResponse, error) {
	return server.controller.cleanup.ExecuteLocalDisposal(ctx, request)
}
func (server *operatorServer) PlanLocalDisposal(ctx context.Context, request *hostv1.PlanLocalDisposalRequest) (*hostv1.PlanLocalDisposalResponse, error) {
	return server.controller.cleanup.PlanLocalDisposal(ctx, request)
}
