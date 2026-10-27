//go:build darwin || linux

package session

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
		PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal,
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
	tracked := &server{}
	tracked.connections.Store(2)
	owner := New(Config{Store: database})
	owner.servers["session"] = tracked
	connected := owner.ToProto(session)
	if connected.GetState() != hostv1.AgentSessionState_AGENT_SESSION_STATE_CONNECTED || connected.GetActiveConnectionCount() != 2 {
		t.Fatalf("connected projection=%+v", connected)
	}
	if err := database.EndSession(ctx, session.ID, "CLIENT_DISCONNECTED", true); err != nil {
		t.Fatal(err)
	}
	delete(owner.servers, session.ID)
	closed, err := database.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	projection := owner.ToProto(closed)
	if projection.GetState() != hostv1.AgentSessionState_AGENT_SESSION_STATE_CLOSED || projection.GetTerminalReason() != "CLIENT_DISCONNECTED" {
		t.Fatalf("closed projection=%+v", projection)
	}
}

func TestActiveSessionsAreRegistrationScoped(t *testing.T) {
	owner := New(Config{})
	owner.servers["first"] = &server{registrationID: "registration-a"}
	owner.servers["second"] = &server{registrationID: "registration-b"}
	if owner.ActiveSessions("registration-a") != 1 || owner.ActiveSessions("registration-b") != 1 || owner.ActiveSessions("unknown") != 0 {
		t.Fatal("session counts crossed registration authority")
	}
}
