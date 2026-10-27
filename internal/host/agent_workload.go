//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *agentServer) AcquireExecution(ctx context.Context, request *hostv1.AcquireExecutionRequest) (*hostv1.AcquireExecutionResponse, error) {
	return server.controller.workloads.AcquireExecution(ctx, request)
}
func (server *agentServer) ReleaseExecution(ctx context.Context, request *hostv1.ReleaseExecutionRequest) (*hostv1.ReleaseExecutionResponse, error) {
	return server.controller.workloads.ReleaseExecution(ctx, request)
}
func (server *agentServer) ListExecutions(ctx context.Context, request *hostv1.ListExecutionsRequest) (*hostv1.ListExecutionsResponse, error) {
	return server.controller.workloads.ListExecutions(ctx, request)
}
func (server *agentServer) QuiesceContainer(ctx context.Context, request *hostv1.QuiesceContainerRequest) (*hostv1.QuiesceContainerResponse, error) {
	return server.controller.workloads.QuiesceContainer(ctx, request)
}
