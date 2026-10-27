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
	if _, err := tx.ExecContext(ctx, `INSERT INTO environments(id,namespace_id,display_name,pids,disk_bytes,egress_mode,owner_kind,registration_id,allocation_id,allocation_generation,allocation_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, environment.ID, environment.NamespaceID, environment.DisplayName, environment.PIDs, environment.DiskBytes, environment.EgressMode, environment.OwnerKind, environment.RegistrationID, environment.AllocationID, environment.AllocationGeneration, environment.AllocationState, now, now); err != nil {
		return Environment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
}

func (environment Environment) Resources() resourcev2.Contract {
	return resourcev2.Contract{PIDMax: uint64(environment.PIDs), WritableQuotaBytes: environment.DiskBytes}
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

func (store *Store) ListRetiringAllocations(ctx context.Context, after string, limit int) ([]Environment, error) {
	rows, err := store.db.QueryContext(ctx, environmentSelect+` WHERE owner_kind=? AND allocation_state=? AND id>? ORDER BY id LIMIT ?`, OwnerRemoteRegistration, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), after, limit)
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

// ListWorkloadEnvironmentPage lists environments that still own their running
// workloads. Retired allocations, revoking registrations, and local disposals
// are excluded because their own retirement or disposal stops the workload.
func (store *Store) ListWorkloadEnvironmentPage(ctx context.Context, after string, limit int) ([]Environment, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, environmentSelect+` e WHERE id>? AND namespace_id NOT LIKE 'system.%'
 AND COALESCE(allocation_state, 0) NOT IN (?, ?)
 AND NOT EXISTS(SELECT 1 FROM remote_registrations r WHERE r.id=e.registration_id AND r.enrollment_state=?)
 AND NOT EXISTS(SELECT 1 FROM local_disposals d WHERE (d.target_kind='environment' AND (d.target_id=e.id OR d.namespace_id=e.namespace_id))
  OR (d.target_kind='registration' AND d.target_id=e.registration_id))
 ORDER BY id LIMIT ?`, after, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), int32(hostv1.AllocationState_ALLOCATION_STATE_DISCARDED),
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING), limit)
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
	rows, err := store.db.QueryContext(ctx, `SELECT id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, owner_kind,
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
