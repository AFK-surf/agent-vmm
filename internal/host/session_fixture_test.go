//go:build darwin || linux

package host

import (
	"context"
	"net"
	"os"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type sessionRuntimeFunc func(context.Context) hostv1.AgentRuntimeServiceServer

func (fn sessionRuntimeFunc) RuntimeService(ctx context.Context) hostv1.AgentRuntimeServiceServer {
	return fn(ctx)
}

type sessionServicesFixture struct{}

func (sessionServicesFixture) ReconcileEnvironment(context.Context, store.Environment) error {
	return nil
}
func testSessionConfig(t *testing.T, controller *Controller) session.Config {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vmm-session-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	exempt := make(map[string]bool)
	for method, policy := range rpcPolicies {
		exempt[method] = policy.readOnly || policy.sessionLockExempt
	}
	return session.Config{Store: controller.store, Services: sessionServicesFixture{}, Runtime: runtimeSessionFactory{controller}, Directory: directory, MaxMessageBytes: maxMessageBytes, UnaryInterceptors: []grpc.UnaryServerInterceptor{auditUnary(controller), deadlineUnary, errorDetailsUnary}, StreamInterceptors: []grpc.StreamServerInterceptor{auditStream(controller), deadlineStream, errorDetailsStream}, MutationExempt: exempt}
}
func createTestSession(t *testing.T, controller *Controller, environmentID, integrationID string, revision uint64, identity auth.Identity) store.Session {
	t.Helper()
	environment, err := controller.store.GetEnvironment(t.Context(), environmentID)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := controller.sessions.Create(t.Context(), environmentID, integrationID, revision, environment.OwnerKind, identity)
	if err != nil {
		t.Fatal(err)
	}
	return record
}
func dialTestSession(t *testing.T, record store.Session) (hostv1.AgentRuntimeServiceClient, *grpc.ClientConn) {
	t.Helper()
	connection, err := grpc.NewClient("passthrough:///session", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", record.SocketPath)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection), connection
}
