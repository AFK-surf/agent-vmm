package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

// BeginWorkloadAdmission validates one environment against the shared workload
// boundary. Stored environment count is not a resource reservation.
func (store *Store) BeginWorkloadAdmission(ctx context.Context, environment Environment, capacity resourcev2.Contract) (Environment, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, err
	}
	defer tx.Rollback()
	if err := environment.Resources().Fits(capacity); err != nil {
		return Environment{}, fmt.Errorf("%w: %w", ErrResourceCapacityExceeded, err)
	}
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO environments(id,namespace_id,display_name,cpu_millis,memory_bytes,pids,disk_bytes,egress_mode,owner_kind,registration_id,allocation_id,allocation_generation,allocation_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, environment.ID, environment.NamespaceID, environment.DisplayName, environment.CPUMillis, environment.MemoryBytes, environment.PIDs, environment.DiskBytes, environment.EgressMode, environment.OwnerKind, environment.RegistrationID, environment.AllocationID, environment.AllocationGeneration, environment.AllocationState, now, now); err != nil {
		return Environment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
}

func (environment Environment) Resources() resourcev2.Contract {
	return resourcev2.Contract{CPUMaxMillis: uint64(environment.CPUMillis), MemoryMaxBytes: environment.MemoryBytes, PIDMax: uint64(environment.PIDs), WritableQuotaBytes: environment.DiskBytes}
}

func (store *Store) CommitNamespaceProjection(ctx context.Context, environment Environment) error {
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET allocation_state=CASE WHEN allocation_state=? THEN ? ELSE allocation_state END
	WHERE id=? AND allocation_generation=? AND allocation_state IN (?,?)`, int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING), int32(hostv1.AllocationState_ALLOCATION_STATE_READY), environment.ID, environment.AllocationGeneration, int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING), int32(hostv1.AllocationState_ALLOCATION_STATE_READY))
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("allocation projection is no longer current")
	}
	return nil
}

func (store *Store) ListRetiringAllocations(ctx context.Context, since time.Time, limit int) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, environmentSelect+` WHERE owner_kind=? AND allocation_state=? AND updated_at>=? ORDER BY updated_at,id LIMIT ?`, OwnerRemoteRegistration, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), since.UnixNano(), limit)
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

// RequeueAllocationRetirement starts a new bounded retirement attempt window
// without changing the allocation fence. A retained allocation whose previous
// window expired otherwise remains a terminal, operator-visible fact.
func (store *Store) RequeueAllocationRetirement(ctx context.Context, id string, expectedRevision uint64) (Environment, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE environments SET updated_at=?
		WHERE id=? AND owner_kind=? AND allocation_state=? AND policy_revision=?`,
		time.Now().UnixNano(), id, OwnerRemoteRegistration,
		int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), expectedRevision)
	if err != nil {
		return Environment{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Environment{}, err
	}
	if changed != 1 {
		return Environment{}, errors.New("allocation retirement fence conflict")
	}
	return store.GetEnvironment(ctx, id)
}

func (store *Store) ListWorkloadEnvironmentPage(ctx context.Context, after string, limit int) ([]Environment, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
 memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
 COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at
 FROM environments WHERE id>? AND namespace_id NOT LIKE 'system.%' ORDER BY id LIMIT ?`, after, limit)
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

func (store *Store) ListLocalResourceEnvironmentPage(ctx context.Context, after string, limit int) ([]Environment, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, cpu_millis,
 memory_bytes, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
 COALESCE(registration_id, ''), COALESCE(allocation_id, ''), COALESCE(allocation_state, 0), allocation_generation, updated_at
 FROM environments e WHERE id>? AND namespace_id NOT LIKE 'system.%' AND (owner_kind=1 OR EXISTS(SELECT 1 FROM remote_registrations r WHERE r.id=e.registration_id AND r.owner_kind=1)) ORDER BY id LIMIT ?`, after, limit)
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
