package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestSchemaV1MigrationAssignsLocalOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateV1(tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO environments(id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, created_at, updated_at)
		VALUES ('existing', 'namespace', 'Existing', 1000, 1073741824, 128, 4294967296, 1, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	environment, err := store.GetEnvironment(context.Background(), "existing")
	if err != nil {
		t.Fatal(err)
	}
	if environment.OwnerKind != OwnerLocal || environment.RegistrationID != "" || environment.AllocationID != "" || environment.AllocationState != 0 {
		t.Fatalf("migrated environment = %+v", environment)
	}
}

func TestSchemaV2MigrationRequiresVerificationForExistingRemoteRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateV1(tx); err != nil {
		t.Fatal(err)
	}
	if err := migrateV2(tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (1, 1), (2, 2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO remote_registrations(
		id, endpoint, display_name, protocol_version, max_environments,
		aggregate_cpu_millis, aggregate_memory_bytes, aggregate_pids, aggregate_disk_bytes,
		per_env_cpu_millis, per_env_memory_bytes, per_env_pids, per_env_disk_bytes,
		max_egress_mode, max_lease_seconds, disconnect_grace_seconds, stop_grace_seconds,
		state, connection_state, created_at, updated_at)
		VALUES ('legacy', 'controller.example:443', 'Legacy', '1', 2,
		2000, 2147483648, 256, 8589934592, 1000, 1073741824, 128, 4294967296,
		1, 3600, 30, 10, 1, 2, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	registration, err := migrated.GetRemoteRegistration(context.Background(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if registration.EnrollmentState != 3 || registration.ConnectionState != 1 || registration.ConnectorPhase != 1 {
		t.Fatalf("legacy registration was trusted without verification: %+v", registration)
	}
	var version int
	if err := migrated.db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil || version != 4 {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
}

func TestSchemaV3MigrationAllowsConfiguringIntegrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateV1(tx); err != nil {
		t.Fatal(err)
	}
	if err := migrateV2(tx); err != nil {
		t.Fatal(err)
	}
	if err := migrateV3(tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (1, 1), (2, 2), (3, 3)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO environments(
		id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode,
		owner_kind, created_at, updated_at) VALUES ('environment', 'namespace', 'Development', 1000, 1, 1, 1, 1, 1, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO agent_integrations(
		id, adapter_id, display_name, scope, config_target, environment_id, desired_state, config_state,
		created_at, updated_at, prepare_request_id)
		VALUES ('integration', 'codex', 'Codex', 1, 'config', 'environment', 1, 1, 1, 1, 'prepare')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO sessions(
		id, environment_id, socket_path, expires_at, integration_id, origin, created_at)
		VALUES ('session', 'environment', '/tmp/session', 2, 'integration', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	prepared, err := migrated.GetAgentIntegration(context.Background(), "integration")
	if err != nil {
		t.Fatal(err)
	}
	configuring, err := migrated.BeginAgentIntegrationConfiguration(context.Background(), prepared.ID, prepared.Revision)
	if err != nil || configuring.ConfigState != 8 {
		t.Fatalf("configuring integration = %+v, %v", configuring, err)
	}
	var foreignKeyViolation string
	if err := migrated.db.QueryRow("PRAGMA foreign_key_check").Scan(&foreignKeyViolation); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign key check = %q, %v", foreignKeyViolation, err)
	}
}

func TestNewerSchemaIsRejectedWithoutRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);
		INSERT INTO schema_migrations(version, applied_at) VALUES (5, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(path); err == nil {
		store.Close()
		t.Fatal("newer schema was opened")
	}
}

func TestEnvironmentOwnerConstraints(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	cases := []Environment{
		{ID: "local-with-remote", NamespaceID: "n1", DisplayName: "bad", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal, RegistrationID: "r", AllocationID: "a"},
		{ID: "remote-without-owner", NamespaceID: "n2", DisplayName: "bad", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerRemoteRegistration, AllocationState: 1},
	}
	for _, value := range cases {
		if _, err := store.CreateEnvironment(ctx, value); err == nil {
			t.Fatalf("invalid owner was accepted: %+v", value)
		}
	}
	remote := Environment{ID: "remote-a", NamespaceID: "rn1", DisplayName: "remote", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerRemoteRegistration, RegistrationID: "r", AllocationID: "a", AllocationState: 1}
	if _, err := store.CreateEnvironment(ctx, remote); err != nil {
		t.Fatal(err)
	}
	remote.ID, remote.NamespaceID = "remote-b", "rn2"
	if _, err := store.CreateEnvironment(ctx, remote); err == nil {
		t.Fatal("duplicate registration/allocation mapping was accepted")
	}
}

func TestSingleActiveRemoteRegistration(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	registration := RemoteRegistration{
		ID: "one", Endpoint: "controller.example:443", DisplayName: "one", ProtocolVersion: "1", MaxEnvironments: 2,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 1 << 30, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 3600, DisconnectGraceSeconds: 30, StopGraceSeconds: 10, State: 1, ConnectionState: 1,
		EnrollmentState: 2, ConnectorPhase: 1,
	}
	created, err := store.CreateRemoteRegistration(ctx, registration)
	if err != nil || created.Revision != 1 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	registration.ID = "two"
	if _, err := store.CreateRemoteRegistration(ctx, registration); err == nil {
		t.Fatal("second active registration was accepted")
	}
	if _, err := store.SetRemoteRegistrationState(ctx, created.ID, created.Revision, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentPolicyAndSessionPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	environment, err := store.CreateEnvironment(ctx, Environment{
		ID: "environment-a", NamespaceID: "namespace-a", DisplayName: "Agent A",
		CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1, OwnerKind: OwnerLocal,
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
	if session.RevokedAt == nil || session.TerminalReason != "SIDECAR_RESTARTED" {
		t.Fatalf("live session survived host restart: %+v", session)
	}
	if err := store.RevokeSession(ctx, session.ID); err != sql.ErrNoRows {
		t.Fatalf("revoke after restart error = %v", err)
	}
}

func TestAgentIntegrationOwnsSessionAuthorizationAndRemovalKeepsEnvironment(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "agent-data", NamespaceID: "agent-namespace", DisplayName: "Agent data",
		CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30,
		EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "integration-a", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "/tmp/config", EnvironmentID: "agent-data", DesiredState: 1,
		ConfigState: 1, PrepareRequestID: "prepare-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := database.CommitAgentIntegration(ctx, prepared.ID, prepared.Revision, "fingerprint", 3)
	if err != nil || waiting.ConfigState != 3 || waiting.Revision != 2 {
		t.Fatalf("waiting=%+v err=%v", waiting, err)
	}
	if err := database.VerifyAgentIntegration(ctx, waiting.ID); err != nil {
		t.Fatal(err)
	}
	ready, err := database.GetAgentIntegration(ctx, waiting.ID)
	if err != nil || ready.ConfigState != 2 || ready.Revision != 3 || ready.LastVerifiedAt == nil {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	createdAt := time.Now().Add(-time.Minute).Truncate(time.Nanosecond)
	if err := database.CreateSession(ctx, Session{
		ID: "session-a", IntegrationID: ready.ID, EnvironmentID: ready.EnvironmentID,
		SocketPath: "/tmp/integration.sock", Origin: 1, CreatedAt: createdAt, ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.MarkSessionConnected(ctx, "session-a"); err != nil {
		t.Fatal(err)
	}
	sessions, err := database.ListSessions(ctx, ready.ID, false)
	if err != nil || len(sessions) != 1 || sessions[0].IntegrationID != ready.ID || sessions[0].ConnectedAt == nil {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, ready.ID, ready.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RevokeSession(ctx, "session-a"); err != nil {
		t.Fatal(err)
	}
	removed, err := database.CompleteAgentIntegrationRemoval(ctx, ready.ID, removing.Revision)
	if err != nil || removed.DesiredState != 2 || removed.ConfigState != 7 {
		t.Fatalf("removed=%+v err=%v", removed, err)
	}
	if _, err := database.GetEnvironment(ctx, "agent-data"); err != nil {
		t.Fatalf("access removal deleted retained environment: %v", err)
	}
}

func TestAgentSessionAuthorizationIsAtomicWithRemoval(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "Environment", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: "environment", DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := database.CommitAgentIntegration(ctx, prepared.ID, prepared.Revision, "fingerprint", 2)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- database.CreateAuthorizedSession(ctx, Session{ID: "session", IntegrationID: ready.ID, EnvironmentID: ready.EnvironmentID, SocketPath: "/tmp/session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)})
	}()
	go func() {
		<-start
		_, err := database.BeginAgentIntegrationRemoval(ctx, ready.ID, ready.Revision)
		results <- err
	}()
	close(start)
	first, second := <-results, <-results
	integration, err := database.GetAgentIntegration(ctx, ready.ID)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := database.ListSessions(ctx, ready.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if integration.ConfigState == 6 && len(sessions) == 0 && first == nil && second == nil {
		t.Fatal("removal and authorization both succeeded without a revocable session")
	}
}

func TestCancelledPendingIntegrationRemovesOnlyItsEmptyEnvironment(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	integrationID := "cancelled"
	environmentID := "integration-" + integrationID
	if _, err := database.CreateEnvironment(ctx, Environment{ID: environmentID, NamespaceID: "namespace", DisplayName: "Pending", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: integrationID, AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: environmentID, DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, prepared.ID, removing.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetAgentIntegration(ctx, prepared.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("integration still exists: %v", err)
	}
	if _, err := database.GetEnvironment(ctx, environmentID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("environment still exists: %v", err)
	}
}

func TestCancelledIntegrationCleanupRollsBackWhenEnvironmentIsReferenced(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	integrationID, environmentID := "blocked", "integration-blocked"
	if _, err := database.CreateEnvironment(ctx, Environment{ID: environmentID, NamespaceID: "namespace", DisplayName: "Pending", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: integrationID, AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: environmentID, DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare-blocked"})
	if err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{ID: "reference", IntegrationID: integrationID, EnvironmentID: environmentID, SocketPath: "/tmp/reference", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, integrationID, removing.Revision); err == nil {
		t.Fatal("referenced cancellation succeeded")
	}
	current, err := database.GetAgentIntegration(ctx, integrationID)
	if err != nil || current.ConfigState != 6 {
		t.Fatalf("integration did not roll back: %+v err=%v", current, err)
	}
	if _, err := database.GetEnvironment(ctx, environmentID); err != nil {
		t.Fatalf("environment was partially deleted: %v", err)
	}
}

func TestRemoteEnrollmentTransitionsAreRevisionAndStateFenced(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	pending, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", ProtocolVersion: "1",
		MaxEnvironments: 2, AggregateCPUMillis: 2000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 1 << 30, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 3600, DisconnectGraceSeconds: 30, StopGraceSeconds: 10,
		State: 1, ConnectionState: 1, EnrollmentState: 1, ConnectorPhase: 2, EnrollmentRequestID: "begin-registration",
	})
	if err != nil {
		t.Fatal(err)
	}
	byRequest, err := database.RemoteRegistrationByEnrollmentRequest(ctx, "begin-registration")
	if err != nil || byRequest.ID != pending.ID {
		t.Fatalf("enrollment request lookup=%+v err=%v", byRequest, err)
	}
	incomplete, err := database.SetRemoteEnrollment(ctx, pending.ID, pending.Revision, 1, 3, "KEYCHAIN_WRITE_FAILED", "Credential storage failed.")
	if err != nil || incomplete.EnrollmentState != 3 || incomplete.EnrollmentErrorCode != "KEYCHAIN_WRITE_FAILED" {
		t.Fatalf("incomplete=%+v err=%v", incomplete, err)
	}
	if _, err := database.SetRemoteEnrollment(ctx, pending.ID, pending.Revision, 1, 2, "", ""); err == nil {
		t.Fatal("stale enrollment revision was accepted")
	}
	retrying, err := database.RetryRemoteEnrollment(ctx, incomplete.ID, incomplete.Revision)
	if err != nil {
		t.Fatal(err)
	}
	retrying, err = database.RetryRemoteEnrollment(ctx, retrying.ID, retrying.Revision)
	if err != nil || retrying.EnrollmentState != 1 {
		t.Fatalf("pending enrollment was not recoverable: %+v err=%v", retrying, err)
	}
	ready, err := database.SetRemoteEnrollment(ctx, retrying.ID, retrying.Revision, 1, 2, "", "")
	if err != nil || ready.EnrollmentState != 2 {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if _, err := database.SetRemoteEnrollment(ctx, ready.ID, ready.Revision, 1, 3, "FAILED", "invalid transition"); err == nil {
		t.Fatal("invalid enrollment transition was accepted")
	}
	revoking, err := database.SetRemoteEnrollment(ctx, ready.ID, ready.Revision, 2, 4, "", "")
	if err != nil || revoking.State != 1 || revoking.EnrollmentState != 4 {
		t.Fatalf("revoking=%+v err=%v", revoking, err)
	}
	revoked, err := database.CompleteRemoteRevocation(ctx, revoking.ID, revoking.Revision)
	if err != nil || revoked.State != 3 || revoked.ConnectionState != 1 {
		t.Fatalf("revoked=%+v err=%v", revoked, err)
	}
}

func TestConnectorStatusRejectsStaleInstances(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", ProtocolVersion: "1", MaxEnvironments: 1,
		AggregateCPUMillis: 1, AggregateMemoryBytes: 1, AggregatePIDs: 1, AggregateDiskBytes: 1,
		PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 10,
		State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	connected, err := database.SetRemoteConnection(ctx, registration.ID, "current", 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "", 3, "test", time.Now(), time.Time{}, 0, "", ""); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("empty stale instance report error=%v", err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "old", 3, "test", time.Now(), time.Time{}, 0, "", ""); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("replaced instance report error=%v", err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "current", 3, "test", time.Now(), time.Time{}, 0, "", ""); err != nil {
		t.Fatalf("current instance report error=%v", err)
	}
	disconnected, err := database.SetRemoteConnection(ctx, connected.ID, "current", 1)
	if err != nil || disconnected.ConnectionInstance != "" {
		t.Fatalf("disconnect did not clear fence: %+v err=%v", disconnected, err)
	}
	if _, err := database.ReportConnectorStatus(ctx, disconnected.ID, "", 5, "test", time.Now(), time.Time{}, 1, "CONNECTION_FAILED", "retrying"); err != nil {
		t.Fatalf("disconnected status report error=%v", err)
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

func TestDiscardEnvironmentRemovesOnlyFullyRemovedIntegrationHistory(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "retained", NamespaceID: "retained-namespace", DisplayName: "Retained",
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "removed-integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "config", EnvironmentID: "retained", DesiredState: 1, ConfigState: 2,
		PrepareRequestID: "prepare-removed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{
		ID: "historical-session", IntegrationID: prepared.ID, EnvironmentID: "retained",
		SocketPath: "/tmp/historical-session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, removing.ID, removing.Revision); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "retained"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetEnvironment(ctx, "retained"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("discarded environment lookup = %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, prepared.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removed integration survived discard: %v", err)
	}
	if _, err := database.GetSession(ctx, "historical-session"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("historical session survived discard: %v", err)
	}
}

func TestDiscardEnvironmentRejectsActiveIntegrationWithoutMutation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "active", NamespaceID: "active-namespace", DisplayName: "Active",
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "active-integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "config", EnvironmentID: "active", DesiredState: 1, ConfigState: 1,
		PrepareRequestID: "prepare-active",
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "active"); !errors.Is(err, ErrEnvironmentHasAgentAccess) {
		t.Fatalf("discard active integration error = %v", err)
	}
	if _, err := database.GetEnvironment(ctx, "active"); err != nil {
		t.Fatalf("blocked discard removed environment: %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, "active-integration"); err != nil {
		t.Fatalf("blocked discard removed integration: %v", err)
	}
}

func TestDiscardEnvironmentRequiresRetainedRemoteAllocation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	states := []hostv1.AllocationState{
		hostv1.AllocationState_ALLOCATION_STATE_READY,
		hostv1.AllocationState_ALLOCATION_STATE_LEASED,
		hostv1.AllocationState_ALLOCATION_STATE_DRAINING,
	}
	for index, state := range states {
		id := fmt.Sprintf("remote-active-%d", index)
		if _, err := database.CreateEnvironment(ctx, Environment{
			ID: id, NamespaceID: id + "-namespace", DisplayName: id,
			CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1,
			OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: id,
			AllocationState: int32(state),
		}); err != nil {
			t.Fatal(err)
		}
		if err := database.ValidateEnvironmentDiscard(ctx, id); !errors.Is(err, ErrRemoteEnvironmentActive) {
			t.Fatalf("validate state %v error = %v", state, err)
		}
		if err := database.DiscardEnvironment(ctx, id); !errors.Is(err, ErrRemoteEnvironmentActive) {
			t.Fatalf("discard state %v error = %v", state, err)
		}
		if _, err := database.GetEnvironment(ctx, id); err != nil {
			t.Fatalf("blocked discard removed state %v environment: %v", state, err)
		}
	}
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "remote-retained", NamespaceID: "remote-retained-namespace", DisplayName: "retained",
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1,
		OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "retained",
		AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "remote-retained"); err != nil {
		t.Fatal(err)
	}
}

func TestDiscardEnvironmentRollsBackRelatedHistoryOnFailure(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "rollback", NamespaceID: "rollback-namespace", DisplayName: "Rollback",
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `INSERT INTO agent_integrations(
		id, adapter_id, display_name, scope, config_target, environment_id, desired_state, config_state,
		created_at, updated_at, prepare_request_id) VALUES ('removed', 'codex', 'Codex', 1, 'config', 'rollback', 2, 7, 1, 1, 'prepare')`); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{
		ID: "rollback-session", IntegrationID: "removed", EnvironmentID: "rollback",
		SocketPath: "/tmp/rollback-session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER reject_environment_delete BEFORE DELETE ON environments
		WHEN OLD.id='rollback' BEGIN SELECT RAISE(ABORT, 'injected discard failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "rollback"); err == nil {
		t.Fatal("discard unexpectedly succeeded")
	}
	if _, err := database.GetEnvironment(ctx, "rollback"); err != nil {
		t.Fatalf("rollback lost environment: %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, "removed"); err != nil {
		t.Fatalf("rollback lost integration: %v", err)
	}
	if _, err := database.GetSession(ctx, "rollback-session"); err != nil {
		t.Fatalf("rollback lost session: %v", err)
	}
}
