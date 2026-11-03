//go:build darwin || linux

package workload

import (
	"context"
	"log/slog"
	"net"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type workloadTestServices struct{}

func (workloadTestServices) ProjectEnvironmentFromGuest(context.Context, vmmdv1.VMMDServiceClient, store.Environment) error {
	return nil
}
func (workloadTestServices) ProjectServiceContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, string, string, string) error {
	return nil
}
func (workloadTestServices) RestoreContainerServiceListeners(context.Context, string, string) error {
	return nil
}
func (workloadTestServices) RestoreManagedServiceContainers(context.Context, vmmdv1.VMMDServiceClient) error {
	return nil
}
func (workloadTestServices) RestoreServiceProjectionsLocked(context.Context, vmmdv1.VMMDServiceClient) error {
	return nil
}
func (workloadTestServices) AuthorizeHostShareMounts(context.Context, string, []*vmmdv1.VolumeMount) error {
	return nil
}
func (workloadTestServices) ReconcileHostShares(context.Context, vmmdv1.VMMDServiceClient) error {
	return nil
}
func newTestWorkloadOwner(owner *Owner) *Owner {
	if owner.manager == nil {
		owner.manager = localResourceVM{&hostv1.VM{}}
	}
	owner.services = workloadTestServices{}
	owner.hostShares = workloadTestServices{}
	owner.projectionMu = &locks.RWMutex{}
	owner.logger = slog.Default()
	return owner
}

type workloadExecutionTestServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	owner *Owner
}

func (s workloadExecutionTestServer) AcquireExecution(ctx context.Context, r *hostv1.AcquireExecutionRequest) (*hostv1.AcquireExecutionResponse, error) {
	return s.owner.AcquireExecution(ctx, r)
}
func (s workloadExecutionTestServer) ReleaseExecution(ctx context.Context, r *hostv1.ReleaseExecutionRequest) (*hostv1.ReleaseExecutionResponse, error) {
	return s.owner.ReleaseExecution(ctx, r)
}
func (s workloadExecutionTestServer) ListExecutions(ctx context.Context, r *hostv1.ListExecutionsRequest) (*hostv1.ListExecutionsResponse, error) {
	return s.owner.ListExecutions(ctx, r)
}
func (s workloadExecutionTestServer) QuiesceContainer(ctx context.Context, r *hostv1.QuiesceContainerRequest) (*hostv1.QuiesceContainerResponse, error) {
	return s.owner.QuiesceContainer(ctx, r)
}
func executionClient(t *testing.T, owner *Owner, identity auth.Identity) hostv1.AgentRuntimeServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(identity)))
	hostv1.RegisterAgentRuntimeServiceServer(server, workloadExecutionTestServer{owner: owner})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///workload", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return hostv1.NewAgentRuntimeServiceClient(conn)
}
