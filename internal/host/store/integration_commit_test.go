package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestIntegrationCommitReceiptSurvivesLostResponseAndNewIntent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "Service", OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "integration", EnvironmentID: "environment", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER fail_commit_receipt BEFORE INSERT ON operations BEGIN SELECT RAISE(ABORT,'receipt failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitAgentIntegration(ctx, "install", prepared.ID, prepared.Revision, "first", 3); err == nil {
		t.Fatal("receipt failure committed configuration")
	}
	unchanged, err := database.GetAgentIntegration(ctx, prepared.ID)
	if err != nil || unchanged.Revision != prepared.Revision || unchanged.ConfigFingerprint != "" {
		t.Fatalf("receipt failure did not roll back CAS: %+v %v", unchanged, err)
	}
	if _, err := database.db.ExecContext(ctx, `DROP TRIGGER fail_commit_receipt`); err != nil {
		t.Fatal(err)
	}
	first, err := database.CommitAgentIntegration(ctx, "install", prepared.ID, prepared.Revision, "first", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.VerifyAgentIntegration(ctx, first.ID, first.Revision); err != nil {
		t.Fatal(err)
	}
	verified, err := database.GetAgentIntegration(ctx, first.ID)
	if err != nil || verified.Revision != first.Revision || verified.ConfigState != 2 {
		t.Fatalf("verification changed intent: %+v %v", verified, err)
	}
	newer, err := database.CommitAgentIntegration(ctx, "update", first.ID, first.Revision, "newer", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.VerifyAgentIntegration(ctx, first.ID, first.Revision); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetAgentIntegration(ctx, first.ID)
	if err != nil || current.ConfigState != 3 || current.Revision != newer.Revision {
		t.Fatalf("stale connection verified newer config: %+v %v", current, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	replay, err := database.CommitAgentIntegration(ctx, "install", prepared.ID, prepared.Revision, "first", 3)
	if err != nil || replay.Revision != first.Revision || replay.ConfigFingerprint != first.ConfigFingerprint || replay.ConfigState != first.ConfigState {
		t.Fatalf("lost response replay: %+v %v", replay, err)
	}
	if _, err := database.CommitAgentIntegration(ctx, "install", prepared.ID, newer.Revision, "first", 3); err == nil {
		t.Fatal("same request accepted new revision")
	}
	if _, err := database.CommitAgentIntegration(ctx, "install", prepared.ID, prepared.Revision, "changed", 3); err == nil {
		t.Fatal("same request accepted changed configuration")
	}
	current, err = database.GetAgentIntegration(ctx, prepared.ID)
	if err != nil || current.Revision != newer.Revision || current.ConfigFingerprint != "newer" {
		t.Fatalf("old intent changed current: %+v %v", current, err)
	}
	session := Session{ID: "stale", IntegrationID: first.ID, EnvironmentID: first.EnvironmentID, SocketPath: "/tmp/stale", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := database.CreateAuthorizedSession(ctx, session, first.Revision); err == nil {
		t.Fatal("stale session was authorized")
	}
	session.ID = "current"
	if err := database.CreateAuthorizedSession(ctx, session, newer.Revision); err != nil {
		t.Fatal(err)
	}
}
