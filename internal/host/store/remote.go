package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type RemoteLeaseProjection struct {
	LeaseID, RegistrationID, AllocationID, EnvironmentID, WorkloadRef string
	Generation, AllocationGeneration                                  uint64
	ExpiresAt                                                         time.Time
	State                                                             int32
}

func (store *Store) AcquireRemoteLeaseProjection(ctx context.Context, lease RemoteLeaseProjection, expectedRevision uint64) (Environment, error) {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `UPDATE environments SET allocation_state=?, policy_revision=policy_revision+1,
		updated_at=? WHERE id=? AND owner_kind=? AND registration_id=? AND allocation_id=?
		AND allocation_state=? AND policy_revision=? AND EXISTS (
			SELECT 1 FROM workload_reservations_v2 r WHERE r.environment_id=environments.id
			AND r.allocation_id=? AND r.generation=? AND r.phase IN (?,?))`, int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED),
		time.Now().UnixNano(), lease.EnvironmentID, OwnerRemoteRegistration, lease.RegistrationID,
		lease.AllocationID, int32(hostv1.AllocationState_ALLOCATION_STATE_READY), expectedRevision,
		lease.AllocationID, lease.AllocationGeneration, WorkloadReservationLogical, WorkloadReservationAdmitted)
	if err != nil {
		return Environment{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Environment{}, err
		}
		return Environment{}, errors.New("allocation revision conflict")
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO remote_lease_projections(lease_id, registration_id,
		allocation_id, allocation_generation, environment_id, workload_ref, generation, expires_at, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		lease.LeaseID, lease.RegistrationID, lease.AllocationID, lease.AllocationGeneration, lease.EnvironmentID, lease.WorkloadRef,
		lease.Generation, lease.ExpiresAt.UnixNano(), lease.State); err != nil {
		return Environment{}, err
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE remote_registrations SET inventory_revision=inventory_revision+1,
		updated_at=? WHERE id=?`, time.Now().UnixNano(), lease.RegistrationID); err != nil {
		return Environment{}, err
	}
	environment, err := scanEnvironment(transaction.QueryRowContext(ctx, environmentSelect+" WHERE id = ?", lease.EnvironmentID))
	if err != nil {
		return Environment{}, err
	}
	if err := transaction.Commit(); err != nil {
		return Environment{}, err
	}
	return environment, nil
}

// FinishRemoteLeaseProjection atomically turns an automatic stop/revoke into
// the same exact durable release obligation used by explicit allocation
// retirement. Model anchor: ResourceAdmissionQueue Stop/Revoke.
func (store *Store) FinishRemoteLeaseProjection(ctx context.Context, registrationID, leaseID, environmentID string, expectedRevision uint64, allocationState int32) (Environment, error) {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, err
	}
	defer transaction.Rollback()
	var allocationID string
	var allocationGeneration uint64
	if err := transaction.QueryRowContext(ctx, `SELECT allocation_id,allocation_generation FROM remote_lease_projections
		WHERE lease_id=? AND registration_id=? AND environment_id=?`, leaseID, registrationID, environmentID).Scan(&allocationID, &allocationGeneration); err != nil {
		return Environment{}, err
	}
	result, err := transaction.ExecContext(ctx, `UPDATE environments SET allocation_state=?, policy_revision=policy_revision+1,
		updated_at=? WHERE id=? AND owner_kind=? AND registration_id=? AND allocation_state=? AND policy_revision=?`,
		allocationState, time.Now().UnixNano(), environmentID,
		OwnerRemoteRegistration, registrationID, int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED), expectedRevision)
	if err != nil {
		return Environment{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Environment{}, err
		}
		return Environment{}, errors.New("allocation revision conflict")
	}
	if allocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		result, err = transaction.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=CASE
			WHEN phase=? THEN ? WHEN phase=? THEN ? ELSE phase END,updated_at=?
			WHERE environment_id=? AND allocation_id=? AND generation=? AND phase IN (?,?,?,?)`, WorkloadReservationLogical,
			WorkloadReservationReleaseRequired, WorkloadReservationAdmitted, WorkloadReservationReleasing,
			time.Now().UnixNano(), environmentID, allocationID, allocationGeneration, WorkloadReservationLogical, WorkloadReservationReleaseRequired,
			WorkloadReservationAdmitted, WorkloadReservationReleasing)
		if err != nil {
			return Environment{}, err
		}
		if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
			if changeErr != nil {
				return Environment{}, changeErr
			}
			return Environment{}, errors.New("exact workload retirement obligation is missing")
		}
	}
	result, err = transaction.ExecContext(ctx, `DELETE FROM remote_lease_projections
		WHERE lease_id=? AND registration_id=? AND environment_id=? AND allocation_id=? AND allocation_generation=?`,
		leaseID, registrationID, environmentID, allocationID, allocationGeneration)
	if err != nil {
		return Environment{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Environment{}, err
		}
		return Environment{}, sql.ErrNoRows
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE remote_registrations SET inventory_revision=inventory_revision+1,
		updated_at=? WHERE id=?`, time.Now().UnixNano(), registrationID); err != nil {
		return Environment{}, err
	}
	environment, err := scanEnvironment(transaction.QueryRowContext(ctx, environmentSelect+" WHERE id = ?", environmentID))
	if err != nil {
		return Environment{}, err
	}
	if err := transaction.Commit(); err != nil {
		return Environment{}, err
	}
	return environment, nil
}

func (store *Store) PutRemoteLeaseProjection(ctx context.Context, lease RemoteLeaseProjection) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `INSERT INTO remote_lease_projections(lease_id, registration_id,
		allocation_id, allocation_generation, environment_id, workload_ref, generation, expires_at, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(lease_id) DO UPDATE SET expires_at=excluded.expires_at, state=excluded.state
		WHERE remote_lease_projections.registration_id=excluded.registration_id AND remote_lease_projections.generation=excluded.generation`,
		lease.LeaseID, lease.RegistrationID, lease.AllocationID, lease.AllocationGeneration, lease.EnvironmentID, lease.WorkloadRef,
		lease.Generation, lease.ExpiresAt.UnixNano(), lease.State); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE remote_registrations SET inventory_revision=inventory_revision+1,
		updated_at=? WHERE id=?`, time.Now().UnixNano(), lease.RegistrationID); err != nil {
		return err
	}
	return transaction.Commit()
}

func (store *Store) DeleteRemoteLeaseProjection(ctx context.Context, registrationID, leaseID string) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `DELETE FROM remote_lease_projections WHERE lease_id=? AND registration_id=?`, leaseID, registrationID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return sql.ErrNoRows
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE remote_registrations SET inventory_revision=inventory_revision+1,
		updated_at=? WHERE id=?`, time.Now().UnixNano(), registrationID); err != nil {
		return err
	}
	return transaction.Commit()
}

func (store *Store) RecoverRemoteLeaseProjections(ctx context.Context) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	var projectionCount int64
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM remote_lease_projections`).Scan(&projectionCount); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	result, err := transaction.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=CASE
		WHEN phase=? THEN ? WHEN phase=? THEN ? ELSE phase END,updated_at=?
		WHERE phase IN (?,?) AND EXISTS (SELECT 1 FROM remote_lease_projections p
			WHERE p.environment_id=workload_reservations_v2.environment_id
			AND p.allocation_id=workload_reservations_v2.allocation_id
			AND p.allocation_generation=workload_reservations_v2.generation)`,
		WorkloadReservationLogical, WorkloadReservationReleaseRequired,
		WorkloadReservationAdmitted, WorkloadReservationReleasing, now,
		WorkloadReservationLogical, WorkloadReservationAdmitted)
	if err != nil {
		return err
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != projectionCount {
		if changeErr != nil {
			return changeErr
		}
		return errors.New("remote lease recovery exact reservation mismatch")
	}
	result, err = transaction.ExecContext(ctx, `UPDATE environments SET allocation_state=?, policy_revision=policy_revision+1,
		updated_at=? WHERE owner_kind=? AND allocation_state=? AND EXISTS (
			SELECT 1 FROM remote_lease_projections p WHERE p.environment_id=environments.id
			AND p.registration_id=environments.registration_id AND p.allocation_id=environments.allocation_id)`,
		int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), now, OwnerRemoteRegistration,
		int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED))
	if err != nil {
		return err
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != projectionCount {
		if changeErr != nil {
			return changeErr
		}
		return errors.New("remote lease recovery exact environment mismatch")
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE remote_registrations SET inventory_revision=inventory_revision+1,
		updated_at=? WHERE id IN (SELECT DISTINCT registration_id FROM remote_lease_projections)`, now); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM remote_lease_projections`); err != nil {
		return err
	}
	return transaction.Commit()
}

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

func (store *Store) ListRemoteEnvironmentsBounded(ctx context.Context, registrationID string, limit int) ([]Environment, error) {
	if limit <= 0 {
		return nil, errors.New("remote environment limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
		memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0) FROM environments
		WHERE owner_kind = 2 AND registration_id = ? ORDER BY allocation_id LIMIT ?`, registrationID, limit)
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
		id, endpoint, display_name, protocol_version, trust_bundle, scope_digest, source_policy_revision, max_environments,
		aggregate_cpu_millis, aggregate_memory_bytes, aggregate_pids, aggregate_disk_bytes,
		per_env_cpu_millis, per_env_memory_bytes, per_env_pids, per_env_disk_bytes,
		max_egress_mode, max_lease_seconds, disconnect_grace_seconds, stop_grace_seconds,
		state, connection_state, enrollment_state, connector_phase, connector_observed_at,
		enrollment_request_id, owner_kind, installation_id, inventory_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?)`,
		registration.ID, registration.Endpoint, registration.DisplayName, registration.ProtocolVersion,
		registration.TrustBundle, registration.ScopeDigest, registration.SourcePolicyRevision,
		registration.MaxEnvironments, registration.AggregateCPUMillis, registration.AggregateMemoryBytes,
		registration.AggregatePIDs, registration.AggregateDiskBytes, registration.PerEnvCPUMillis,
		registration.PerEnvMemoryBytes, registration.PerEnvPIDs, registration.PerEnvDiskBytes,
		registration.MaxEgressMode, registration.MaxLeaseSeconds, registration.DisconnectGraceSeconds,
		registration.StopGraceSeconds, registration.State, registration.ConnectionState, registration.EnrollmentState,
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

const remoteRegistrationSelect = `SELECT id, endpoint, display_name, protocol_version, trust_bundle, scope_digest, source_policy_revision, max_environments,
	aggregate_cpu_millis, aggregate_memory_bytes, aggregate_pids, aggregate_disk_bytes,
	per_env_cpu_millis, per_env_memory_bytes, per_env_pids, per_env_disk_bytes,
	max_egress_mode, max_lease_seconds, disconnect_grace_seconds, stop_grace_seconds,
	revision, state, connection_state, connection_instance, enrollment_state, enrollment_error_code,
	enrollment_error_message, connector_phase, connector_release, connector_observed_at, last_connected_at,
	disconnected_at, next_retry_at, reconnect_attempt, connector_failure_code, connector_failure_message,
	enrollment_request_id, owner_kind, COALESCE(installation_id, ''), inventory_revision, pool_policy_revision, created_at, updated_at FROM remote_registrations`

func scanRemoteRegistration(row rowScanner) (RemoteRegistration, error) {
	var registration RemoteRegistration
	var connectorObserved, lastConnected, disconnected, nextRetry, created, updated int64
	err := row.Scan(&registration.ID, &registration.Endpoint, &registration.DisplayName, &registration.ProtocolVersion,
		&registration.TrustBundle, &registration.ScopeDigest, &registration.SourcePolicyRevision,
		&registration.MaxEnvironments, &registration.AggregateCPUMillis, &registration.AggregateMemoryBytes,
		&registration.AggregatePIDs, &registration.AggregateDiskBytes, &registration.PerEnvCPUMillis,
		&registration.PerEnvMemoryBytes, &registration.PerEnvPIDs, &registration.PerEnvDiskBytes,
		&registration.MaxEgressMode, &registration.MaxLeaseSeconds, &registration.DisconnectGraceSeconds,
		&registration.StopGraceSeconds, &registration.Revision, &registration.State, &registration.ConnectionState,
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

func (store *Store) UpdateRemotePool(ctx context.Context, registration RemoteRegistration, expectedRevision uint64) (RemoteRegistration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET
		max_environments=?, aggregate_cpu_millis=?, aggregate_memory_bytes=?, aggregate_pids=?, aggregate_disk_bytes=?,
		per_env_cpu_millis=?, per_env_memory_bytes=?, per_env_pids=?, per_env_disk_bytes=?, max_egress_mode=?,
		max_lease_seconds=?, disconnect_grace_seconds=?, stop_grace_seconds=?, revision=revision+1, pool_policy_revision=pool_policy_revision+1, updated_at=?
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

func (store *Store) BeginRemoteRevocation(ctx context.Context, id string, expectedRevision uint64) (RemoteRegistration, error) {
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
