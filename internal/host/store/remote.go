package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	RemoteRegistrationExternal = 1
	RemoteRegistrationPrivate  = 2
)

type RemoteRegistration struct {
	ID                      string
	Endpoint                string
	DisplayName             string
	ProtocolVersion         string
	TrustBundle             []byte
	ScopeDigest             string
	SourcePolicyRevision    uint64
	PerEnvPIDs              uint32
	PerEnvDiskBytes         uint64
	MaxEgressMode           int32
	StopGraceSeconds        uint32
	Revision                uint64
	PoolPolicyRevision      uint64
	State                   int32
	ConnectionState         int32
	ConnectionInstance      string
	EnrollmentState         int32
	EnrollmentErrorCode     string
	EnrollmentErrorMessage  string
	ConnectorPhase          int32
	ConnectorRelease        string
	ConnectorObservedAt     time.Time
	LastConnectedAt         time.Time
	DisconnectedAt          time.Time
	NextRetryAt             time.Time
	ReconnectAttempt        uint32
	ConnectorFailureCode    string
	ConnectorFailureMessage string
	EnrollmentRequestID     string
	OwnerKind               int32
	InstallationID          string
	InventoryRevision       uint64
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (store *Store) ListRemoteEnvironments(ctx context.Context, registrationID string) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments
		WHERE owner_kind = 2 AND registration_id = ? ORDER BY allocation_id`, registrationID)
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

func (store *Store) ListRemoteActiveEnvironmentsBounded(ctx context.Context, registrationID string, limit int) ([]Environment, error) {
	if limit <= 0 {
		return nil, errors.New("remote environment limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments
		WHERE owner_kind = 2 AND registration_id = ? AND allocation_state IN (1, 3)
		ORDER BY allocation_id LIMIT ?`, registrationID, limit)
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

func (store *Store) ListRemoteEnvironmentPage(ctx context.Context, registrationID, afterAllocationID string, limit int) ([]Environment, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments
		WHERE owner_kind = 2 AND registration_id = ? AND allocation_id > ? ORDER BY allocation_id LIMIT ?`, registrationID, afterAllocationID, limit)
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

func (store *Store) CreateRemoteRegistration(ctx context.Context, registration RemoteRegistration) (RemoteRegistration, error) {
	if registration.OwnerKind == 0 {
		registration.OwnerKind = RemoteRegistrationExternal
	}
	if registration.InventoryRevision == 0 {
		registration.InventoryRevision = 1
	}
	if registration.TrustBundle == nil {
		registration.TrustBundle = []byte{}
	}
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO remote_registrations(
		id, endpoint, display_name, protocol_version, trust_bundle, scope_digest, source_policy_revision,
		per_env_pids, per_env_disk_bytes,
		max_egress_mode, stop_grace_seconds,
		state, connection_state, enrollment_state, connector_phase, connector_observed_at,
		enrollment_request_id, owner_kind, installation_id, inventory_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?)`,
		registration.ID, registration.Endpoint, registration.DisplayName, registration.ProtocolVersion,
		registration.TrustBundle, registration.ScopeDigest, registration.SourcePolicyRevision,
		registration.PerEnvPIDs, registration.PerEnvDiskBytes,
		registration.MaxEgressMode, registration.StopGraceSeconds, registration.State, registration.ConnectionState, registration.EnrollmentState,
		registration.ConnectorPhase, now, registration.EnrollmentRequestID, registration.OwnerKind,
		registration.InstallationID, registration.InventoryRevision, now, now)
	if err != nil {
		return RemoteRegistration{}, err
	}
	return store.GetRemoteRegistration(ctx, registration.ID)
}

func (store *Store) GetRemoteRegistration(ctx context.Context, id string) (RemoteRegistration, error) {
	return scanRemoteRegistration(store.db.QueryRowContext(ctx, remoteRegistrationSelect+" WHERE id = ?", id))
}

func (store *Store) RemoteRegistrationByEnrollmentRequest(ctx context.Context, requestID string) (RemoteRegistration, error) {
	return scanRemoteRegistration(store.db.QueryRowContext(ctx, remoteRegistrationSelect+" WHERE enrollment_request_id = ?", requestID))
}

const remoteRegistrationSelect = `SELECT id, endpoint, display_name, protocol_version, trust_bundle, scope_digest, source_policy_revision,
	per_env_pids, per_env_disk_bytes,
	max_egress_mode, stop_grace_seconds,
	revision, state, connection_state, connection_instance, enrollment_state, enrollment_error_code,
	enrollment_error_message, connector_phase, connector_release, connector_observed_at, last_connected_at,
	disconnected_at, next_retry_at, reconnect_attempt, connector_failure_code, connector_failure_message,
	enrollment_request_id, owner_kind, COALESCE(installation_id, ''), inventory_revision, pool_policy_revision, created_at, updated_at FROM remote_registrations`

func scanRemoteRegistration(row rowScanner) (RemoteRegistration, error) {
	var registration RemoteRegistration
	var connectorObserved, lastConnected, disconnected, nextRetry, created, updated int64
	err := row.Scan(&registration.ID, &registration.Endpoint, &registration.DisplayName, &registration.ProtocolVersion,
		&registration.TrustBundle, &registration.ScopeDigest, &registration.SourcePolicyRevision,
		&registration.PerEnvPIDs, &registration.PerEnvDiskBytes,
		&registration.MaxEgressMode, &registration.StopGraceSeconds, &registration.Revision, &registration.State, &registration.ConnectionState,
		&registration.ConnectionInstance, &registration.EnrollmentState, &registration.EnrollmentErrorCode,
		&registration.EnrollmentErrorMessage, &registration.ConnectorPhase, &registration.ConnectorRelease,
		&connectorObserved, &lastConnected, &disconnected, &nextRetry, &registration.ReconnectAttempt,
		&registration.ConnectorFailureCode, &registration.ConnectorFailureMessage, &registration.EnrollmentRequestID,
		&registration.OwnerKind, &registration.InstallationID, &registration.InventoryRevision, &registration.PoolPolicyRevision, &created, &updated)
	registration.ConnectorObservedAt = time.Unix(0, connectorObserved)
	registration.LastConnectedAt = time.Unix(0, lastConnected)
	registration.DisconnectedAt = time.Unix(0, disconnected)
	registration.NextRetryAt = time.Unix(0, nextRetry)
	registration.CreatedAt = time.Unix(0, created)
	registration.UpdatedAt = time.Unix(0, updated)
	return registration, err
}

func (store *Store) ListRemoteRegistrations(ctx context.Context) ([]RemoteRegistration, error) {
	rows, err := store.db.QueryContext(ctx, remoteRegistrationSelect+" ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var registrations []RemoteRegistration
	for rows.Next() {
		registration, err := scanRemoteRegistration(rows)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	return registrations, rows.Err()
}

func (store *Store) ListRemoteRegistrationPage(ctx context.Context, afterID string, limit int) ([]RemoteRegistration, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, remoteRegistrationSelect+" WHERE id > ? ORDER BY id LIMIT ?", afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var registrations []RemoteRegistration
	for rows.Next() {
		registration, scanErr := scanRemoteRegistration(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		registrations = append(registrations, registration)
	}
	return registrations, rows.Err()
}

func (store *Store) ListRemoteRegistrationsBounded(ctx context.Context, limit int) ([]RemoteRegistration, error) {
	if limit <= 0 {
		return nil, errors.New("remote registration limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, remoteRegistrationSelect+" ORDER BY created_at, id LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var registrations []RemoteRegistration
	for rows.Next() {
		registration, scanErr := scanRemoteRegistration(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		registrations = append(registrations, registration)
	}
	return registrations, rows.Err()
}

func (store *Store) UpdateRemotePool(ctx context.Context, registration RemoteRegistration, expectedRevision uint64) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET
		per_env_pids=?, per_env_disk_bytes=?, max_egress_mode=?,
		stop_grace_seconds=?, revision=revision+1, pool_policy_revision=pool_policy_revision+1, updated_at=?
		WHERE id=? AND revision=?`, registration.PerEnvPIDs,
		registration.PerEnvDiskBytes, registration.MaxEgressMode, registration.StopGraceSeconds, time.Now().UnixNano(),
		registration.ID, expectedRevision)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, registration.ID)
}

func (store *Store) SetRemoteRegistrationState(ctx context.Context, id string, expectedRevision uint64, state int32) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET state=?, revision=revision+1,
		connection_state=CASE WHEN ? = 3 THEN 1 ELSE connection_state END,
		connection_instance=CASE WHEN ? = 3 THEN '' ELSE connection_instance END, updated_at=?
		WHERE id=? AND revision=? AND state != 3 AND enrollment_state=2`, state, state, state, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) SetRemoteConnection(ctx context.Context, id, instance string, state int32) (RemoteRegistration, error) {
	now := time.Now().UnixNano()
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET connection_state=?, connection_instance=CASE WHEN ?=1 THEN '' ELSE ? END,
		last_connected_at=CASE WHEN ?=2 THEN ? ELSE last_connected_at END,
		disconnected_at=CASE WHEN ?=1 THEN ? ELSE disconnected_at END, updated_at=?
		WHERE id=? AND state != 3 AND enrollment_state=2`, state, state, instance, state, now, state, now, now, id)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, sql.ErrNoRows
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) SetRemoteEnrollment(ctx context.Context, id string, expectedRevision uint64, fromState, state int32, code, message string) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET enrollment_state=?,
		enrollment_error_code=?, enrollment_error_message=?, revision=revision+1, updated_at=?
		WHERE id=? AND revision=? AND state != 3 AND enrollment_state=?`, state, code, message, time.Now().UnixNano(), id, expectedRevision, fromState)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) DeleteIncompleteRemoteRegistration(ctx context.Context, id string, expectedRevision uint64) error {
	result, err := store.db.ExecContext(ctx, `DELETE FROM remote_registrations WHERE id=? AND revision=?
		AND enrollment_state IN (1,3)`, id, expectedRevision)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return errors.New("registration revision conflict")
	}
	return nil
}

func (store *Store) CompleteRemoteRevocation(ctx context.Context, id string, expectedRevision uint64) (RemoteRegistration, error) {
	current, err := store.GetRemoteRegistration(ctx, id)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if current.Revision == expectedRevision && current.State == 3 {
		return current, nil
	}
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET state=3, connection_state=1,
		connection_instance='', revision=revision+1, updated_at=? WHERE id=? AND revision=? AND enrollment_state=4
 AND NOT EXISTS (SELECT 1 FROM environments WHERE registration_id=remote_registrations.id AND allocation_state != 4)`,
		time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) BeginRemoteRevocation(ctx context.Context, id string, expectedRevision uint64) (RemoteRegistration, error) {
	current, err := store.GetRemoteRegistration(ctx, id)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if current.Revision == expectedRevision && current.EnrollmentState == 4 && (current.State == 2 || current.State == 3) {
		return current, nil
	}
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET enrollment_state=4, state=2,
		connection_state=1, connection_instance='', revision=revision+1, updated_at=?
		WHERE id=? AND revision=? AND state != 3 AND enrollment_state=2`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) RetryRemoteEnrollment(ctx context.Context, id string, expectedRevision uint64) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET enrollment_state=1,
		enrollment_error_code='', enrollment_error_message='', connector_phase=2, revision=revision+1, updated_at=?
		WHERE id=? AND revision=? AND enrollment_state IN (1,3)`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, errors.New("registration revision conflict")
	}
	return store.GetRemoteRegistration(ctx, id)
}

func (store *Store) ReportConnectorStatus(ctx context.Context, id, instance string, phase int32, release string,
	observedAt, nextRetry time.Time, attempt uint32, code, message string) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET connector_phase=?, connector_release=?,
		connector_observed_at=?, next_retry_at=?, reconnect_attempt=?, connector_failure_code=?,
		connector_failure_message=?, updated_at=? WHERE id=? AND state != 3
		AND ((? != '' AND connection_instance=?) OR (? = '' AND connection_instance='' AND connection_state=1))`, phase, release, observedAt.UnixNano(), nextRetry.UnixNano(),
		attempt, code, message, time.Now().UnixNano(), id, instance, instance, instance)
	if err != nil {
		return RemoteRegistration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return RemoteRegistration{}, err
		}
		return RemoteRegistration{}, sql.ErrNoRows
	}
	return store.GetRemoteRegistration(ctx, id)
}
