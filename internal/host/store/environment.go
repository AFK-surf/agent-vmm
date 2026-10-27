package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

var (
	ErrEnvironmentHasAgentAccess = errors.New("environment still has agent access")
	ErrRemoteEnvironmentActive   = errors.New("remote environment allocation is not retained")
)

type Environment struct {
	ID                   string
	NamespaceID          string
	DisplayName          string
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
		(id, namespace_id, display_name, pids, disk_bytes, egress_mode, owner_kind, registration_id, allocation_id, allocation_state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0), ?, ?)`, environment.ID, environment.NamespaceID, environment.DisplayName,
		environment.PIDs, environment.DiskBytes, environment.EgressMode,
		environment.OwnerKind, environment.RegistrationID, environment.AllocationID, environment.AllocationState, now, now)
	if err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
}

const environmentSelect = `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
	COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments`

func (store *Store) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, environmentSelect+" WHERE id = ?", id))
}

func (store *Store) GetRemoteEnvironment(ctx context.Context, registrationID, allocationID string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments
		WHERE owner_kind = 2 AND registration_id = ? AND allocation_id = ?`, registrationID, allocationID))
}

func (store *Store) EnvironmentByNamespace(ctx context.Context, namespaceID string) (Environment, error) {
	return scanEnvironment(store.db.QueryRowContext(ctx, `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
		COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at FROM environments WHERE namespace_id = ?`, namespaceID))
}

type rowScanner interface{ Scan(...any) error }

func scanEnvironment(row rowScanner) (Environment, error) {
	var environment Environment
	var updatedAt int64
	err := row.Scan(&environment.ID, &environment.NamespaceID, &environment.DisplayName, &environment.PIDs, &environment.DiskBytes, &environment.EgressMode, &environment.PolicyRevision,
		&environment.OwnerKind, &environment.RegistrationID, &environment.AllocationID, &environment.AllocationState, &environment.AllocationGeneration, &updatedAt)
	environment.UpdatedAt = time.Unix(0, updatedAt)
	return environment, err
}

func (store *Store) ListEnvironments(ctx context.Context) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
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
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
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
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET pids=?, disk_bytes=?,
		egress_mode=?, policy_revision=policy_revision+1, updated_at=? WHERE id=? AND owner_kind=? AND policy_revision=?`,
		limits.PIDs, limits.DiskBytes, limits.EgressMode, time.Now().UnixNano(), id, OwnerLocal, expectedRevision)
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
