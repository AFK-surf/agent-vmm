//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPrepareAgentIntegrationAppliesDeclaredLimitsOnlyOnCreate(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &operatorServer{controller: &Controller{store: database}}
	limits := &hostv1.ResourceLimits{CpuMillis: proto.Uint32(1000), MemoryBytes: proto.Uint64(1 << 30), Pids: proto.Uint32(128), DiskBytes: proto.Uint64(4 << 30)}
	request := &hostv1.PrepareAgentIntegrationRequest{RequestId: "prepare", AdapterId: "service", DisplayName: "Service",
		Scope: hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL, ConfigTarget: "service://test", EnvironmentId: proto.String("environment"), Limits: limits}
	request.Limits = nil
	if _, err := callAsOperator(t, func(ctx context.Context) (*hostv1.AgentIntegrationResponse, error) {
		return server.PrepareAgentIntegration(ctx, request)
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing limits: %v", err)
	}
	if _, err := database.GetEnvironment(context.Background(), "environment"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("invalid prepare created environment: %v", err)
	}
	request.Limits = limits
	prepared, err := callAsOperator(t, func(ctx context.Context) (*hostv1.AgentIntegrationResponse, error) {
		return server.PrepareAgentIntegration(ctx, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := database.GetEnvironment(context.Background(), prepared.GetIntegration().GetEnvironmentId())
	if err != nil || environment.CPUMillis != limits.GetCpuMillis() || environment.MemoryBytes != limits.GetMemoryBytes() ||
		environment.PIDs != limits.GetPids() || environment.DiskBytes != limits.GetDiskBytes() {
		t.Fatalf("new environment does not have declared limits: %+v %v", environment, err)
	}
	for _, nextLimits := range []*hostv1.ResourceLimits{nil, {CpuMillis: proto.Uint32(2000), MemoryBytes: proto.Uint64(2 << 30), Pids: proto.Uint32(512), DiskBytes: proto.Uint64(16 << 30)}} {
		request.RequestId += "-next"
		request.Limits = nextLimits
		if _, err := callAsOperator(t, func(ctx context.Context) (*hostv1.AgentIntegrationResponse, error) {
			return server.PrepareAgentIntegration(ctx, request)
		}); err != nil {
			t.Fatal(err)
		}
		current, err := database.GetEnvironment(context.Background(), environment.ID)
		if err != nil || current != environment {
			t.Fatalf("prepare changed an existing environment policy: %+v %v", current, err)
		}
	}
}

func TestAgentSessionProjectionTracksConnectionsAndTerminalState(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, store.Environment{
		ID: "environment", NamespaceID: "namespace", DisplayName: "Environment",
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{
		ID: "integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "/tmp/config", EnvironmentID: "environment", DesiredState: 1,
		ConfigState: 2, PrepareRequestID: "prepare",
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, store.Session{
		ID: "session", IntegrationID: "integration", EnvironmentID: "environment",
		SocketPath: "/tmp/session.sock", Origin: int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION),
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	session, err := database.GetSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	tracked := &sessionServer{}
	tracked.connections.Store(2)
	controller := &Controller{store: database, sessions: map[string]*sessionServer{"session": tracked}}
	connected := controller.sessionToProto(session)
	if connected.GetState() != hostv1.AgentSessionState_AGENT_SESSION_STATE_CONNECTED || connected.GetActiveConnectionCount() != 2 {
		t.Fatalf("connected projection=%+v", connected)
	}
	if err := database.EndSession(ctx, session.ID, "CLIENT_DISCONNECTED", true); err != nil {
		t.Fatal(err)
	}
	delete(controller.sessions, session.ID)
	closed, err := database.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	projection := controller.sessionToProto(closed)
	if projection.GetState() != hostv1.AgentSessionState_AGENT_SESSION_STATE_CLOSED || projection.GetTerminalReason() != "CLIENT_DISCONNECTED" {
		t.Fatalf("closed projection=%+v", projection)
	}
}

func TestRemoteRegistrationProjectionExpiresStaleHeartbeat(t *testing.T) {
	registration := store.RemoteRegistration{
		ConnectionState:     int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED),
		ConnectorPhase:      int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED),
		ConnectorObservedAt: time.Now().Add(-time.Minute),
	}
	projection := registrationToProto(registration)
	if projection.GetConnectionState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED ||
		projection.GetConnectorStatus().GetFailureCode() != "HEARTBEAT_STALE" {
		t.Fatalf("stale connector projected online: %+v", projection)
	}
}

func TestIntegrationBulkRevocationPropagatesFailure(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.revokeIntegrationSessions(ctx, "integration"); err == nil {
		t.Fatal("bulk revocation swallowed storage failure")
	}
}
