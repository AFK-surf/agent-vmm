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

const schemaVersion = 1

type Store struct {
	db *sql.DB
}

type Environment struct {
	ID             string
	NamespaceID    string
	DisplayName    string
	CPUMillis      uint32
	MemoryBytes    uint64
	PIDs           uint32
	DiskBytes      uint64
	EgressMode     int32
	PolicyRevision uint64
}

type Session struct {
	ID            string
	EnvironmentID string
	NamespaceID   string
	SocketPath    string
	ExpiresAt     time.Time
	RevokedAt     *time.Time
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
		error_message='the sidecar restarted before the operation completed', updated_at=? WHERE state=?`,
		int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED), time.Now().UnixNano(), int32(hostv1.OperationState_OPERATION_STATE_RUNNING)); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover interrupted operations: %w", err)
	}
	return result, nil
}

func (store *Store) Close() error { return store.db.Close() }

func (store *Store) migrate(ctx context.Context) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	var version int
	err = tx.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version)
	if err != nil {
		return err
	}
	if version > schemaVersion {
		return fmt.Errorf("host database schema %d is newer than supported %d", version, schemaVersion)
	}
	if version == 0 {
		if err := migrateV1(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func migrateV1(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE vm_config (
			id INTEGER PRIMARY KEY CHECK (id = 1), instance_id TEXT NOT NULL,
			bundle_directory TEXT NOT NULL, instance_directory TEXT NOT NULL,
			state_disk_identity TEXT NOT NULL, cpu_count INTEGER NOT NULL,
			memory_bytes INTEGER NOT NULL, state_bytes INTEGER NOT NULL
		)`,
		`CREATE TABLE environments (
			id TEXT PRIMARY KEY, namespace_id TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
			cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL,
			disk_bytes INTEGER NOT NULL, egress_mode INTEGER NOT NULL,
			policy_revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE sessions (
			id TEXT PRIMARY KEY, environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			socket_path TEXT NOT NULL UNIQUE, expires_at INTEGER NOT NULL, revoked_at INTEGER
		)`,
		`CREATE TABLE grants (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, source_namespace_id TEXT NOT NULL,
			target_namespace_id TEXT NOT NULL, methods TEXT NOT NULL, expires_at INTEGER,
			revoked_at INTEGER, revision INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE volume_policies (
			volume_id TEXT NOT NULL, owner_namespace_id TEXT NOT NULL REFERENCES environments(namespace_id) ON DELETE CASCADE,
			revision INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (owner_namespace_id, volume_id)
		)`,
		`CREATE TABLE operations (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, environment_id TEXT NOT NULL,
			state INTEGER NOT NULL, stage TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
			error_reason TEXT NOT NULL DEFAULT '', error_message TEXT NOT NULL DEFAULT '', output_path TEXT NOT NULL DEFAULT '',
			started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE audit (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL, target TEXT NOT NULL,
			method TEXT NOT NULL, grant_id TEXT NOT NULL, request_id TEXT NOT NULL,
			result_code INTEGER NOT NULL, reason TEXT NOT NULL, occurred_at INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("create host schema: %w", err)
		}
	}
	return nil
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
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO environments
		(id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, environment.ID, environment.NamespaceID, environment.DisplayName,
		environment.CPUMillis, environment.MemoryBytes, environment.PIDs, environment.DiskBytes, environment.EgressMode, now, now)
	if err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
}

func (store *Store) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision FROM environments WHERE id = ?`, id))
}

func (store *Store) EnvironmentByNamespace(ctx context.Context, namespaceID string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision FROM environments WHERE namespace_id = ?`, namespaceID))
}

type rowScanner interface{ Scan(...any) error }

func scanEnvironment(row rowScanner) (Environment, error) {
	var environment Environment
	err := row.Scan(&environment.ID, &environment.NamespaceID, &environment.DisplayName, &environment.CPUMillis,
		&environment.MemoryBytes, &environment.PIDs, &environment.DiskBytes, &environment.EgressMode, &environment.PolicyRevision)
	return environment, err
}

func (store *Store) ListEnvironments(ctx context.Context) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision FROM environments ORDER BY display_name, id`)
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

func (store *Store) UpdatePolicy(ctx context.Context, id string, expectedRevision uint64, limits Environment) (Environment, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET cpu_millis=?, memory_bytes=?, pids=?, disk_bytes=?,
		egress_mode=?, policy_revision=policy_revision+1, updated_at=? WHERE id=? AND policy_revision=?`,
		limits.CPUMillis, limits.MemoryBytes, limits.PIDs, limits.DiskBytes, limits.EgressMode, time.Now().UnixNano(), id, expectedRevision)
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

func (store *Store) DeleteEnvironment(ctx context.Context, id string) error {
	result, err := store.db.ExecContext(ctx, "DELETE FROM environments WHERE id = ?", id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) CreateSession(ctx context.Context, session Session) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO sessions(id, environment_id, socket_path, expires_at)
		VALUES (?, ?, ?, ?)`, session.ID, session.EnvironmentID, session.SocketPath, session.ExpiresAt.UnixNano())
	return err
}

func (store *Store) GetSession(ctx context.Context, id string) (Session, error) {
	var session Session
	var expires int64
	var revoked sql.NullInt64
	err := store.db.QueryRowContext(ctx, `SELECT s.id, s.environment_id, e.namespace_id, s.socket_path,
		s.expires_at, s.revoked_at FROM sessions s JOIN environments e ON e.id=s.environment_id WHERE s.id=?`, id).
		Scan(&session.ID, &session.EnvironmentID, &session.NamespaceID, &session.SocketPath, &expires, &revoked)
	if err != nil {
		return Session{}, err
	}
	session.ExpiresAt = time.Unix(0, expires)
	if revoked.Valid {
		value := time.Unix(0, revoked.Int64)
		session.RevokedAt = &value
	}
	return session, nil
}

func (store *Store) RevokeSession(ctx context.Context, id string) error {
	result, err := store.db.ExecContext(ctx, "UPDATE sessions SET revoked_at=? WHERE id=? AND revoked_at IS NULL", time.Now().UnixNano(), id)
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
	_, err := store.db.ExecContext(ctx, "UPDATE sessions SET revoked_at=? WHERE revoked_at IS NULL", time.Now().UnixNano())
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
	now := time.Now()
	result, err := store.db.ExecContext(ctx, `INSERT INTO operations
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
	stored, err := store.OperationByRequestID(ctx, operation.RequestID)
	return stored, inserted == 1, err
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
	return err
}
