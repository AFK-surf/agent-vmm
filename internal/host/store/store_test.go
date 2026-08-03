package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestEnvironmentPolicyAndSessionPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	environment, err := store.CreateEnvironment(ctx, Environment{
		ID: "environment-a", NamespaceID: "namespace-a", DisplayName: "Agent A",
		CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if environment.PolicyRevision != 1 {
		t.Fatalf("revision = %d", environment.PolicyRevision)
	}
	updated, err := store.UpdatePolicy(ctx, environment.ID, 1, Environment{
		CPUMillis: 2000, MemoryBytes: 2 << 30, PIDs: 256, DiskBytes: 8 << 30, EgressMode: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.PolicyRevision != 2 || updated.CPUMillis != 2000 {
		t.Fatalf("updated = %+v", updated)
	}
	if _, err := store.UpdatePolicy(ctx, environment.ID, 1, Environment{}); err == nil {
		t.Fatal("stale revision was accepted")
	}
	if err := store.PutVolumeOwner(ctx, "volume-a", environment.NamespaceID); err != nil {
		t.Fatal(err)
	}
	if exists, err := store.HasVolumePolicy(ctx, "volume-a", environment.NamespaceID); err != nil || !exists {
		t.Fatalf("volume policy exists=%t, err=%v", exists, err)
	}
	expires := time.Now().Add(time.Hour).Truncate(time.Nanosecond)
	if err := store.CreateSession(ctx, Session{ID: "session-a", EnvironmentID: environment.ID, SocketPath: "/tmp/a.sock", ExpiresAt: expires}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	session, err := store.GetSession(ctx, "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if session.NamespaceID != "namespace-a" || !session.ExpiresAt.Equal(expires) {
		t.Fatalf("session = %+v", session)
	}
	if err := store.RevokeSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeSession(ctx, session.ID); err != sql.ErrNoRows {
		t.Fatalf("second revoke error = %v", err)
	}
}

func TestOperationRequestIDDeduplication(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	first, inserted, err := store.PutOperation(ctx, Operation{ID: "operation-a", Kind: "diagnostic", EnvironmentID: "system", State: 1, Stage: "queued", RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first operation was not inserted")
	}
	second, inserted, err := store.PutOperation(ctx, Operation{ID: "operation-b", Kind: "diagnostic", EnvironmentID: "system", State: 1, Stage: "queued", RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("duplicate operation was inserted")
	}
	if first.ID != second.ID {
		t.Fatalf("dedupe returned %q and %q", first.ID, second.ID)
	}
}

func TestOpenMarksRunningOperationsInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sqlite")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := first.PutOperation(context.Background(), Operation{ID: "operation-a", Kind: "diagnostic", State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	recovered, err := second.GetOperation(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || recovered.ErrorReason != "SIDECAR_RESTARTED" {
		t.Fatalf("recovered operation = %+v", recovered)
	}
}

func TestRevokedGrantRemainsAsReconciliationTombstone(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.PutGrant(ctx, Grant{ID: "link-a", Kind: "network-link", SourceNamespaceID: "source", TargetNamespaceID: "target", Payload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeGrant(ctx, "link-a"); err != nil {
		t.Fatal(err)
	}
	if grants, err := store.ActiveGrants(ctx); err != nil || len(grants) != 0 {
		t.Fatalf("active grants after revoke = %v, %v", grants, err)
	}
	grants, err := store.AllGrants(ctx)
	if err != nil || len(grants) != 1 || !grants[0].Revoked {
		t.Fatalf("grant tombstones = %v, %v", grants, err)
	}
}
