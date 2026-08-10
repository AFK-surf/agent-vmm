package mcpserver

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type bootstrapOperator struct {
	hostv1.UnimplementedOperatorServiceServer
	sessionPath string
	owner       hostv1.EnvironmentOwnerKind
}

func (server bootstrapOperator) EnsureLocalEnvironment(context.Context, *hostv1.EnsureLocalEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	return &hostv1.GetEnvironmentResponse{Environment: &hostv1.AgentEnvironment{Owner: &hostv1.EnvironmentOwner{Kind: server.owner}}}, nil
}

func (server bootstrapOperator) CreateAgentSession(context.Context, *hostv1.CreateAgentSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	return &hostv1.CreateAgentSessionResponse{SessionId: "session", SocketPath: server.sessionPath, Environment: &hostv1.AgentEnvironment{Origin: hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL}}, nil
}

type releaseRuntime struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	released chan struct{}
}

func (server releaseRuntime) ReleaseSession(context.Context, *hostv1.ReleaseSessionRequest) (*emptypb.Empty, error) {
	select {
	case server.released <- struct{}{}:
	default:
	}
	return &emptypb.Empty{}, nil
}

func serveUnix(t *testing.T, path string, register func(*grpc.Server)) {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close() })
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "avm-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	return directory
}

func TestBootstrapLocalSessionAndRelease(t *testing.T) {
	directory := shortTempDir(t)
	operatorPath, sessionPath := filepath.Join(directory, "operator.sock"), filepath.Join(directory, "session.sock")
	released := make(chan struct{}, 1)
	serveUnix(t, sessionPath, func(server *grpc.Server) {
		hostv1.RegisterAgentRuntimeServiceServer(server, releaseRuntime{released: released})
	})
	serveUnix(t, operatorPath, func(server *grpc.Server) {
		hostv1.RegisterOperatorServiceServer(server, bootstrapOperator{sessionPath: sessionPath, owner: hostv1.EnvironmentOwnerKind_ENVIRONMENT_OWNER_KIND_LOCAL})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := Bootstrap(ctx, operatorPath, "workspace", "Workspace")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("session was not released")
	}
}

func TestBootstrapRejectsRemoteOwnerAndUnsafeSocket(t *testing.T) {
	for _, test := range []struct {
		name  string
		owner hostv1.EnvironmentOwnerKind
		mode  os.FileMode
	}{
		{name: "remote", owner: hostv1.EnvironmentOwnerKind_ENVIRONMENT_OWNER_KIND_REMOTE_REGISTRATION, mode: 0o600},
		{name: "permissions", owner: hostv1.EnvironmentOwnerKind_ENVIRONMENT_OWNER_KIND_LOCAL, mode: 0o666},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := shortTempDir(t)
			operatorPath, sessionPath := filepath.Join(directory, "operator.sock"), filepath.Join(directory, "session.sock")
			serveUnix(t, sessionPath, func(server *grpc.Server) {
				hostv1.RegisterAgentRuntimeServiceServer(server, releaseRuntime{released: make(chan struct{}, 1)})
			})
			if err := os.Chmod(sessionPath, test.mode); err != nil {
				t.Fatal(err)
			}
			serveUnix(t, operatorPath, func(server *grpc.Server) {
				hostv1.RegisterOperatorServiceServer(server, bootstrapOperator{sessionPath: sessionPath, owner: test.owner})
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if session, err := Bootstrap(ctx, operatorPath, "workspace", ""); err == nil {
				session.Close(ctx)
				t.Fatal("unsafe bootstrap succeeded")
			}
		})
	}
}
