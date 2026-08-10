//go:build darwin

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

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
