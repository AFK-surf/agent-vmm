package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type RemoteRegistration struct {
	ID                      string
	Endpoint                string
	DisplayName             string
	ProtocolVersion         string
	MaxEnvironments         uint32
	AggregateCPUMillis      uint32
	AggregateMemoryBytes    uint64
	AggregatePIDs           uint32
	AggregateDiskBytes      uint64
	PerEnvCPUMillis         uint32
	PerEnvMemoryBytes       uint64
	PerEnvPIDs              uint32
	PerEnvDiskBytes         uint64
	MaxEgressMode           int32
	MaxLeaseSeconds         uint32
	DisconnectGraceSeconds  uint32
	StopGraceSeconds        uint32
	Revision                uint64
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
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (store *Store) ListRemoteEnvironments(ctx context.Context, registrationID string) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0) FROM environments
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

func (store *Store) CreateRemoteRegistration(ctx context.Context, registration RemoteRegistration) (RemoteRegistration, error) {
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO remote_registrations(
		id, endpoint, display_name, protocol_version, max_environments,
		aggregate_cpu_millis, aggregate_memory_bytes, aggregate_pids, aggregate_disk_bytes,
		per_env_cpu_millis, per_env_memory_bytes, per_env_pids, per_env_disk_bytes,
		max_egress_mode, max_lease_seconds, disconnect_grace_seconds, stop_grace_seconds,
		state, connection_state, enrollment_state, connector_phase, connector_observed_at,
		enrollment_request_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		registration.ID, registration.Endpoint, registration.DisplayName, registration.ProtocolVersion,
		registration.MaxEnvironments, registration.AggregateCPUMillis, registration.AggregateMemoryBytes,
		registration.AggregatePIDs, registration.AggregateDiskBytes, registration.PerEnvCPUMillis,
		registration.PerEnvMemoryBytes, registration.PerEnvPIDs, registration.PerEnvDiskBytes,
		registration.MaxEgressMode, registration.MaxLeaseSeconds, registration.DisconnectGraceSeconds,
		registration.StopGraceSeconds, registration.State, registration.ConnectionState, registration.EnrollmentState,
		registration.ConnectorPhase, now, registration.EnrollmentRequestID, now, now)
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

func (store *Store) ActiveRemoteRegistration(ctx context.Context) (RemoteRegistration, error) {
	return scanRemoteRegistration(store.db.QueryRowContext(ctx, remoteRegistrationSelect+" WHERE state != 3"))
}

const remoteRegistrationSelect = `SELECT id, endpoint, display_name, protocol_version, max_environments,
	aggregate_cpu_millis, aggregate_memory_bytes, aggregate_pids, aggregate_disk_bytes,
	per_env_cpu_millis, per_env_memory_bytes, per_env_pids, per_env_disk_bytes,
	max_egress_mode, max_lease_seconds, disconnect_grace_seconds, stop_grace_seconds,
	revision, state, connection_state, connection_instance, enrollment_state, enrollment_error_code,
	enrollment_error_message, connector_phase, connector_release, connector_observed_at, last_connected_at,
	disconnected_at, next_retry_at, reconnect_attempt, connector_failure_code, connector_failure_message,
	enrollment_request_id, created_at, updated_at FROM remote_registrations`

func scanRemoteRegistration(row rowScanner) (RemoteRegistration, error) {
	var registration RemoteRegistration
	var connectorObserved, lastConnected, disconnected, nextRetry, created, updated int64
	err := row.Scan(&registration.ID, &registration.Endpoint, &registration.DisplayName, &registration.ProtocolVersion,
		&registration.MaxEnvironments, &registration.AggregateCPUMillis, &registration.AggregateMemoryBytes,
		&registration.AggregatePIDs, &registration.AggregateDiskBytes, &registration.PerEnvCPUMillis,
		&registration.PerEnvMemoryBytes, &registration.PerEnvPIDs, &registration.PerEnvDiskBytes,
		&registration.MaxEgressMode, &registration.MaxLeaseSeconds, &registration.DisconnectGraceSeconds,
		&registration.StopGraceSeconds, &registration.Revision, &registration.State, &registration.ConnectionState,
		&registration.ConnectionInstance, &registration.EnrollmentState, &registration.EnrollmentErrorCode,
		&registration.EnrollmentErrorMessage, &registration.ConnectorPhase, &registration.ConnectorRelease,
		&connectorObserved, &lastConnected, &disconnected, &nextRetry, &registration.ReconnectAttempt,
		&registration.ConnectorFailureCode, &registration.ConnectorFailureMessage, &registration.EnrollmentRequestID, &created, &updated)
	registration.ConnectorObservedAt = time.Unix(0, connectorObserved)
	registration.LastConnectedAt = time.Unix(0, lastConnected)
	registration.DisconnectedAt = time.Unix(0, disconnected)
	registration.NextRetryAt = time.Unix(0, nextRetry)
	registration.CreatedAt = time.Unix(0, created)
	registration.UpdatedAt = time.Unix(0, updated)
	return registration, err
}

func (store *Store) UpdateRemotePool(ctx context.Context, registration RemoteRegistration, expectedRevision uint64) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET
		max_environments=?, aggregate_cpu_millis=?, aggregate_memory_bytes=?, aggregate_pids=?, aggregate_disk_bytes=?,
		per_env_cpu_millis=?, per_env_memory_bytes=?, per_env_pids=?, per_env_disk_bytes=?, max_egress_mode=?,
		max_lease_seconds=?, disconnect_grace_seconds=?, stop_grace_seconds=?, revision=revision+1, updated_at=?
		WHERE id=? AND revision=?`, registration.MaxEnvironments, registration.AggregateCPUMillis,
		registration.AggregateMemoryBytes, registration.AggregatePIDs, registration.AggregateDiskBytes,
		registration.PerEnvCPUMillis, registration.PerEnvMemoryBytes, registration.PerEnvPIDs,
		registration.PerEnvDiskBytes, registration.MaxEgressMode, registration.MaxLeaseSeconds,
		registration.DisconnectGraceSeconds, registration.StopGraceSeconds, time.Now().UnixNano(),
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
		WHERE id=? AND revision=?`, state, state, state, time.Now().UnixNano(), id, expectedRevision)
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
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET state=3, connection_state=1,
		connection_instance='', revision=revision+1, updated_at=? WHERE id=? AND revision=? AND enrollment_state=4`,
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
