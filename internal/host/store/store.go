package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	_ "modernc.org/sqlite"
)

const schemaVersion = 24

const (
	auditRetention  = 30 * 24 * time.Hour
	auditMaxRows    = 10_000
	auditPruneBatch = 256
)

const (
	OwnerLocal              = 1
	OwnerRemoteRegistration = 2
)

type Store struct {
	db *sql.DB
}

var (
	ErrEnvironmentHasAgentAccess = errors.New("environment still has agent access")
	ErrRemoteEnvironmentActive   = errors.New("remote environment allocation is not retained")
)

type Environment struct {
	ID                   string
	NamespaceID          string
	DisplayName          string
	CPUMillis            uint32
	MemoryBytes          uint64
	PIDs                 uint32
	DiskBytes            uint64
	EgressMode           int32
	PolicyRevision       uint64
	OwnerKind            int32
	RegistrationID       string
	AllocationID         string
	AllocationState      int32
	AllocationGeneration uint64
	UpdatedAt            time.Time
}

type Session struct {
	ID             string
	IntegrationID  string
	EnvironmentID  string
	NamespaceID    string
	SocketPath     string
	Origin         int32
	CreatedAt      time.Time
	ExpiresAt      time.Time
	ConnectedAt    *time.Time
	LastActivity   *time.Time
	RevokedAt      *time.Time
	ClosedAt       *time.Time
	TerminalReason string
}

type Grant struct {
	ID                string
	Kind              string
	SourceNamespaceID string
	TargetNamespaceID string
	Payload           string
	Revoked           bool
}

type Operation struct {
	ID            string
	Kind          string
	EnvironmentID string
	State         int32
	Stage         string
	RequestID     string
	ErrorReason   string
	ErrorMessage  string
	OutputPath    string
	StartedAt     time.Time
	UpdatedAt     time.Time
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=FULL",
		"PRAGMA wal_autocheckpoint=1000",
		"PRAGMA journal_size_limit=67108864",
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure host database: %w", err)
		}
	}
	result := &Store{db: db}
	if err := result.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`UPDATE operations SET state=?, stage='interrupted', error_reason='SIDECAR_RESTARTED',
		error_message='the sidecar restarted before the operation completed', updated_at=? WHERE state=? AND kind != 'local-disposal'`,
		int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED), time.Now().UnixNano(), int32(hostv1.OperationState_OPERATION_STATE_RUNNING)); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover interrupted operations: %w", err)
	}
	if _, err := db.Exec(`UPDATE sessions SET revoked_at=?, terminal_reason='SIDECAR_RESTARTED'
		WHERE revoked_at IS NULL AND closed_at IS NULL`, time.Now().UnixNano()); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover agent sessions: %w", err)
	}
	if _, err := db.Exec(`UPDATE remote_registrations SET connection_state=1, connection_instance='',
		enrollment_state=CASE WHEN enrollment_state=1 THEN 3 ELSE enrollment_state END,
		enrollment_error_code=CASE WHEN enrollment_state=1 THEN 'ENROLLMENT_INTERRUPTED' ELSE enrollment_error_code END,
		enrollment_error_message=CASE WHEN enrollment_state=1 THEN 'Enrollment did not finish. Retry setup.' ELSE enrollment_error_message END,
		connector_phase=1, connector_observed_at=?, updated_at=?`, time.Now().UnixNano(), time.Now().UnixNano()); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover remote connection state: %w", err)
	}
	if _, err := db.Exec(`UPDATE service_listeners SET revoked=1, revision=revision+1, updated_at=? WHERE revoked=0`, time.Now().UnixNano()); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover service listeners: %w", err)
	}
	if err := result.maintainStorage(context.Background(), time.Now()); err != nil {
		db.Close()
		return nil, fmt.Errorf("prune mutation receipts: %w", err)
	}
	return result, nil
}

func (store *Store) Close() error {
	_, _ = store.db.Exec("PRAGMA wal_checkpoint(PASSIVE)")
	return store.db.Close()
}

func (store *Store) PutVMConfig(ctx context.Context, instanceID, bundleDirectory, instanceDirectory, stateDiskIdentity string, cpuCount uint, memoryBytes uint64, stateBytes int64) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO vm_config
		(id, instance_id, bundle_directory, instance_directory, state_disk_identity, cpu_count, memory_bytes, state_bytes)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET instance_id=excluded.instance_id,
		bundle_directory=excluded.bundle_directory, instance_directory=excluded.instance_directory,
		state_disk_identity=excluded.state_disk_identity, cpu_count=excluded.cpu_count,
		memory_bytes=excluded.memory_bytes, state_bytes=excluded.state_bytes`,
		instanceID, bundleDirectory, instanceDirectory, stateDiskIdentity, cpuCount, memoryBytes, stateBytes)
	return err
}

func (store *Store) CreateEnvironment(ctx context.Context, environment Environment) (Environment, error) {
	if environment.OwnerKind == 0 {
		return Environment{}, errors.New("environment owner is required")
	}
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO environments
		(id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, owner_kind, registration_id, allocation_id, allocation_state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0), ?, ?)`, environment.ID, environment.NamespaceID, environment.DisplayName,
		environment.CPUMillis, environment.MemoryBytes, environment.PIDs, environment.DiskBytes, environment.EgressMode,
		environment.OwnerKind, environment.RegistrationID, environment.AllocationID, environment.AllocationState, now, now)
	if err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
}

const environmentSelect = `SELECT id, namespace_id, display_name, cpu_millis,
	memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
	COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments`

func (store *Store) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, environmentSelect+" WHERE id = ?", id))
}

func (store *Store) GetRemoteEnvironment(ctx context.Context, registrationID, allocationID string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments
		WHERE owner_kind = 2 AND registration_id = ? AND allocation_id = ?`, registrationID, allocationID))
}

func (store *Store) EnvironmentByNamespace(ctx context.Context, namespaceID string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments WHERE namespace_id = ?`, namespaceID))
}

type rowScanner interface{ Scan(...any) error }

func scanEnvironment(row rowScanner) (Environment, error) {
	var environment Environment
	var updatedAt int64
	err := row.Scan(&environment.ID, &environment.NamespaceID, &environment.DisplayName, &environment.CPUMillis,
		&environment.MemoryBytes, &environment.PIDs, &environment.DiskBytes, &environment.EgressMode, &environment.PolicyRevision,
		&environment.OwnerKind, &environment.RegistrationID, &environment.AllocationID, &environment.AllocationState, &environment.AllocationGeneration, &updatedAt)
	environment.UpdatedAt = time.Unix(0, updatedAt)
	return environment, err
}

func (store *Store) ListEnvironments(ctx context.Context) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments ORDER BY display_name, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Environment
	for rows.Next() {
		environment, err := scanEnvironment(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, environment)
	}
	return result, rows.Err()
}

func (store *Store) ListEnvironmentsBounded(ctx context.Context, limit int) ([]Environment, error) {
	if limit <= 0 {
		return nil, errors.New("environment limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments ORDER BY display_name, id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Environment
	for rows.Next() {
		environment, scanErr := scanEnvironment(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, environment)
	}
	return result, rows.Err()
}

func (store *Store) UpdatePolicy(ctx context.Context, id string, expectedRevision uint64, limits Environment) (Environment, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET cpu_millis=?, memory_bytes=?, pids=?, disk_bytes=?,
		egress_mode=?, policy_revision=policy_revision+1, updated_at=? WHERE id=? AND owner_kind=? AND policy_revision=?`,
		limits.CPUMillis, limits.MemoryBytes, limits.PIDs, limits.DiskBytes, limits.EgressMode, time.Now().UnixNano(), id, OwnerLocal, expectedRevision)
	if err != nil {
		return Environment{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Environment{}, err
	}
	if changed != 1 {
		return Environment{}, errors.New("policy revision conflict")
	}
	return store.GetEnvironment(ctx, id)
}

func (store *Store) SetAllocationState(ctx context.Context, id string, expectedRevision uint64, state int32) (Environment, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET allocation_state=?, policy_revision=policy_revision+1,
		updated_at=? WHERE id=? AND owner_kind=2 AND policy_revision=?`, state, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return Environment{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Environment{}, err
	}
	if changed != 1 {
		return Environment{}, errors.New("allocation revision conflict")
	}
	return store.GetEnvironment(ctx, id)
}

func (store *Store) ValidateEnvironmentDiscard(ctx context.Context, id string) error {
	var ownerKind, allocationState int32
	if err := store.db.QueryRowContext(ctx, "SELECT owner_kind, COALESCE(allocation_state, 0) FROM environments WHERE id=?", id).Scan(&ownerKind, &allocationState); err != nil {
		return err
	}
	if ownerKind == OwnerRemoteRegistration && allocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return ErrRemoteEnvironmentActive
	}
	var count int
	err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_integrations
		WHERE environment_id=? AND (desired_state != 2 OR config_state != 7)`, id).Scan(&count)
	if err != nil {
		return err
	}
	if count != 0 {
		return ErrEnvironmentHasAgentAccess
	}
	return nil
}

func (store *Store) DiscardEnvironment(ctx context.Context, id string) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	var ownerKind, allocationState int32
	if err := transaction.QueryRowContext(ctx, "SELECT owner_kind, COALESCE(allocation_state, 0) FROM environments WHERE id=?", id).Scan(&ownerKind, &allocationState); err != nil {
		return err
	}
	if ownerKind == OwnerRemoteRegistration && allocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return ErrRemoteEnvironmentActive
	}
	var blockers int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_integrations
		WHERE environment_id=? AND (desired_state != 2 OR config_state != 7)`, id).Scan(&blockers); err != nil {
		return err
	}
	if blockers != 0 {
		return ErrEnvironmentHasAgentAccess
	}
	if _, err := transaction.ExecContext(ctx, "DELETE FROM sessions WHERE environment_id=?", id); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM agent_integrations
		WHERE environment_id=? AND desired_state=2 AND config_state=7`, id); err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, "DELETE FROM environments WHERE id=?", id)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return transaction.Commit()
}

func (store *Store) CreateSession(ctx context.Context, session Session) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO sessions(id, integration_id, environment_id, socket_path, origin, created_at, expires_at)
		VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?)`, session.ID, session.IntegrationID, session.EnvironmentID, session.SocketPath,
		session.Origin, session.CreatedAt.UnixNano(), session.ExpiresAt.UnixNano())
	return err
}

func (store *Store) CreateAuthorizedSession(ctx context.Context, session Session) error {
	result, err := store.db.ExecContext(ctx, `INSERT INTO sessions(id, integration_id, environment_id, socket_path, origin, created_at, expires_at)
		SELECT ?, id, environment_id, ?, ?, ?, ? FROM agent_integrations
		WHERE id=? AND environment_id=? AND desired_state=1 AND config_state IN (2,3)`,
		session.ID, session.SocketPath, session.Origin, session.CreatedAt.UnixNano(), session.ExpiresAt.UnixNano(),
		session.IntegrationID, session.EnvironmentID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("integration is not authorized")
	}
	return nil
}

func (store *Store) GetSession(ctx context.Context, id string) (Session, error) {
	var session Session
	var expires int64
	var revoked sql.NullInt64
	var created int64
	var connected, activity, closed sql.NullInt64
	err := store.db.QueryRowContext(ctx, `SELECT s.id, COALESCE(s.integration_id, ''), s.environment_id, e.namespace_id, s.socket_path,
		s.origin, s.created_at, s.expires_at, s.connected_at, s.last_activity_at, s.revoked_at, s.closed_at, s.terminal_reason
		FROM sessions s JOIN environments e ON e.id=s.environment_id WHERE s.id=?`, id).
		Scan(&session.ID, &session.IntegrationID, &session.EnvironmentID, &session.NamespaceID, &session.SocketPath,
			&session.Origin, &created, &expires, &connected, &activity, &revoked, &closed, &session.TerminalReason)
	if err != nil {
		return Session{}, err
	}
	session.ExpiresAt = time.Unix(0, expires)
	session.CreatedAt = time.Unix(0, created)
	if connected.Valid {
		value := time.Unix(0, connected.Int64)
		session.ConnectedAt = &value
	}
	if activity.Valid {
		value := time.Unix(0, activity.Int64)
		session.LastActivity = &value
	}
	if revoked.Valid {
		value := time.Unix(0, revoked.Int64)
		session.RevokedAt = &value
	}
	if closed.Valid {
		value := time.Unix(0, closed.Int64)
		session.ClosedAt = &value
	}
	return session, nil
}

func (store *Store) RevokeSession(ctx context.Context, id string) error {
	return store.EndSession(ctx, id, "REVOKED", false)
}

func (store *Store) EndSession(ctx context.Context, id, reason string, closed bool) error {
	now := time.Now().UnixNano()
	result, err := store.db.ExecContext(ctx, `UPDATE sessions SET revoked_at=CASE WHEN ? THEN revoked_at ELSE ? END,
		closed_at=CASE WHEN ? THEN ? ELSE closed_at END, terminal_reason=?
		WHERE id=? AND revoked_at IS NULL AND closed_at IS NULL`, closed, now, closed, now, reason, id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) RevokeActiveSessions(ctx context.Context) error {
	_, err := store.db.ExecContext(ctx, "UPDATE sessions SET revoked_at=?, terminal_reason='SIDECAR_RESTARTED' WHERE revoked_at IS NULL AND closed_at IS NULL", time.Now().UnixNano())
	return err
}

func (store *Store) PutGrant(ctx context.Context, grant Grant) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO grants(id, kind, source_namespace_id, target_namespace_id, methods)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,
		source_namespace_id=excluded.source_namespace_id, target_namespace_id=excluded.target_namespace_id,
		methods=excluded.methods, revoked_at=NULL, revision=grants.revision+1`,
		grant.ID, grant.Kind, grant.SourceNamespaceID, grant.TargetNamespaceID, grant.Payload)
	return err
}

func (store *Store) ActiveGrants(ctx context.Context) ([]Grant, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods
		FROM grants WHERE revoked_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []Grant
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// ActiveGrantsForNamespace returns at most limit+1 grants so callers can reject
// an inventory that exceeds their admitted work bound without scanning every
// grant in the Host database.
func (store *Store) ActiveGrantsForNamespace(ctx context.Context, namespaceID string, limit int) ([]Grant, error) {
	if limit < 0 {
		return nil, errors.New("grant result limit must not be negative")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods
		FROM grants WHERE revoked_at IS NULL AND (source_namespace_id=? OR target_namespace_id=?)
		ORDER BY id LIMIT ?`, namespaceID, namespaceID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants := make([]Grant, 0, min(limit+1, 16))
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// GrantsForNamespace returns active grants and tombstones within the same
// bounded owner scope. Cleanup uses tombstones to remove Guest policy that may
// remain after a Host-side revocation was persisted.
func (store *Store) GrantsForNamespace(ctx context.Context, namespaceID string, limit int) ([]Grant, error) {
	if limit < 0 {
		return nil, errors.New("grant result limit must not be negative")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants WHERE source_namespace_id=? OR target_namespace_id=?
		ORDER BY id LIMIT ?`, namespaceID, namespaceID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants := make([]Grant, 0, min(limit+1, 16))
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func (store *Store) AllGrants(ctx context.Context) ([]Grant, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []Grant
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func (store *Store) GetGrant(ctx context.Context, id string) (Grant, error) {
	var grant Grant
	err := store.db.QueryRowContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants WHERE id=?`, id).Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked)
	return grant, err
}

func (store *Store) RevokeGrant(ctx context.Context, id string) error {
	result, err := store.db.ExecContext(ctx, `UPDATE grants SET revoked_at=?, revision=revision+1 WHERE id=? AND revoked_at IS NULL`, time.Now().UnixNano(), id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) RevokeGrants(ctx context.Context, namespaceID string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE grants SET revoked_at=?, revision=revision+1 WHERE revoked_at IS NULL AND
		(source_namespace_id=? OR target_namespace_id=?)`, time.Now().UnixNano(), namespaceID, namespaceID)
	return err
}

// CompactRevokedGrant removes a reconciliation tombstone only after the Guest
// has confirmed the corresponding desired-state deletion.
func (store *Store) CompactRevokedGrant(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM grants WHERE id=? AND revoked_at IS NOT NULL`, id)
	return err
}

func (store *Store) PutVolumeOwner(ctx context.Context, volumeID, namespaceID string) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO volume_policies(volume_id, owner_namespace_id) VALUES (?, ?)
		ON CONFLICT(owner_namespace_id, volume_id) DO UPDATE SET revision=volume_policies.revision+1`, volumeID, namespaceID)
	return err
}

func (store *Store) DeleteVolumePolicy(ctx context.Context, volumeID, namespaceID string) error {
	result, err := store.db.ExecContext(ctx, "DELETE FROM volume_policies WHERE volume_id=? AND owner_namespace_id=?", volumeID, namespaceID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) HasVolumePolicy(ctx context.Context, volumeID, namespaceID string) (bool, error) {
	var exists bool
	err := store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM volume_policies WHERE volume_id=? AND owner_namespace_id=?)", volumeID, namespaceID).Scan(&exists)
	return exists, err
}

func (store *Store) PutOperation(ctx context.Context, operation Operation) (Operation, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, false, err
	}
	defer tx.Rollback()
	now := time.Now()
	result, err := tx.ExecContext(ctx, `INSERT INTO operations
		(id, kind, environment_id, state, stage, request_id, error_reason, error_message, output_path, started_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(request_id) DO NOTHING`, operation.ID, operation.Kind, operation.EnvironmentID, operation.State,
		operation.Stage, operation.RequestID, operation.ErrorReason, operation.ErrorMessage, operation.OutputPath, now.UnixNano(), now.UnixNano())
	if err != nil {
		return Operation{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return Operation{}, false, err
	}
	stored, err := scanOperation(tx.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at FROM operations WHERE request_id=?`, operation.RequestID))
	if err != nil {
		return Operation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, false, err
	}
	return stored, inserted == 1, nil
}

func (store *Store) OperationByRequestID(ctx context.Context, requestID string) (Operation, error) {
	return scanOperation(store.db.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at FROM operations WHERE request_id=?`, requestID))
}

func (store *Store) GetOperation(ctx context.Context, id string) (Operation, error) {
	return scanOperation(store.db.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at FROM operations WHERE id=?`, id))
}

func scanOperation(row rowScanner) (Operation, error) {
	var operation Operation
	var started, updated int64
	err := row.Scan(&operation.ID, &operation.Kind, &operation.EnvironmentID, &operation.State, &operation.Stage,
		&operation.RequestID, &operation.ErrorReason, &operation.ErrorMessage, &operation.OutputPath, &started, &updated)
	operation.StartedAt = time.Unix(0, started)
	operation.UpdatedAt = time.Unix(0, updated)
	return operation, err
}

func (store *Store) UpdateOperation(ctx context.Context, operation Operation) error {
	_, err := store.db.ExecContext(ctx, `UPDATE operations SET state=?, stage=?, error_reason=?, error_message=?, output_path=?, updated_at=? WHERE id=?`,
		operation.State, operation.Stage, operation.ErrorReason, operation.ErrorMessage, operation.OutputPath, time.Now().UnixNano(), operation.ID)
	return err
}

func (store *Store) Audit(ctx context.Context, actor, target, method, grantID, requestID string, resultCode int32, reason string) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO audit(actor, target, method, grant_id, request_id, result_code, reason, occurred_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, actor, target, method, grantID, requestID, resultCode, reason, time.Now().UnixNano())
	if err != nil {
		return err
	}
	return store.maintainStorage(ctx, time.Now())
}

func (store *Store) pruneAudit(ctx context.Context, now time.Time) error {
	if _, err := store.db.ExecContext(ctx, `DELETE FROM audit WHERE sequence IN (
		SELECT sequence FROM audit WHERE occurred_at < ? ORDER BY occurred_at,sequence LIMIT ?
	)`, now.Add(-auditRetention).UnixNano(), auditPruneBatch); err != nil {
		return err
	}
	var excess int
	if err := store.db.QueryRowContext(ctx, `SELECT MAX(COUNT(*)-?,0) FROM audit`, auditMaxRows).Scan(&excess); err != nil {
		return err
	}
	if excess == 0 {
		return nil
	}
	_, err := store.db.ExecContext(ctx, `DELETE FROM audit WHERE sequence IN (
		SELECT sequence FROM audit ORDER BY sequence LIMIT ?
	)`, min(excess, auditPruneBatch))
	return err
}

func (store *Store) maintainStorage(ctx context.Context, now time.Time) error {
	if err := store.pruneAudit(ctx, now); err != nil {
		return err
	}
	var busy, logFrames, checkpointed int
	if err := store.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 || logFrames != checkpointed || logFrames == 0 {
		return nil
	}
	if _, err := store.db.ExecContext(ctx, `PRAGMA busy_timeout=0`); err != nil {
		return err
	}
	defer store.db.ExecContext(context.Background(), `PRAGMA busy_timeout=5000`)
	return store.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed)
}
