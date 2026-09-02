package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func (store *Store) ReserveResources(ctx context.Context, reservation ResourceReservation, hostCapacity, domainCeiling ResourceAmount) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	total, err := sumReservations(ctx, tx, "", 0)
	if err != nil {
		return err
	}
	domain, err := sumReservations(ctx, tx, reservation.OwnerID, reservation.OwnerKind)
	if err != nil {
		return err
	}
	if !total.add(reservation.Amount).fits(hostCapacity) {
		return ErrResourceCapacityExceeded
	}
	if reservation.OwnerKind != ReservationOwnerHost && !domain.add(reservation.Amount).fits(domainCeiling) {
		return ErrDomainCapacityExceeded
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO resource_reservations(id, owner_kind, owner_id,
		resource_kind, resource_id, cpu_millis, memory_bytes, pids, disk_bytes, retained, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, reservation.ID, reservation.OwnerKind,
		reservation.OwnerID, reservation.ResourceKind, reservation.ResourceID, reservation.Amount.CPUMillis,
		reservation.Amount.MemoryBytes, reservation.Amount.PIDs, reservation.Amount.DiskBytes,
		reservation.Retained, time.Now().UnixNano())
	if err != nil {
		return err
	}
	return tx.Commit()
}

// BeginWorkloadAdmission atomically owns the Host logical reservation and the
// environment identity. Guest physical reservation is a later exact RPC and
// cannot be reported admitted until CommitGuestReservation persists its receipt.
// Model anchor: spec/tla/ResourceAdmissionQueue.tla Host logical/receipt phases.
func (store *Store) BeginWorkloadAdmission(ctx context.Context, environment Environment, reservation WorkloadReservation, capacity resourcev2.Capacity) (Environment, WorkloadReservation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	defer tx.Rollback()
	used, err := sumWorkloadReservations(ctx, tx, "")
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	if err := used.Admits(reservation.Resources, capacity); err != nil {
		return Environment{}, WorkloadReservation{}, fmt.Errorf("%w: %w", ErrResourceCapacityExceeded, err)
	}
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO environments(id, namespace_id, display_name,
		cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, owner_kind, registration_id,
		allocation_id, allocation_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		environment.ID, environment.NamespaceID, environment.DisplayName, reservation.Resources.CPUMaxMillis,
		reservation.Resources.MemoryMaxBytes, reservation.Resources.PIDMax, reservation.Resources.WritableQuotaBytes,
		environment.EgressMode, environment.OwnerKind, environment.RegistrationID, environment.AllocationID,
		environment.AllocationState, now, now); err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workload_reservations_v2(allocation_id,generation,registration_id,
		environment_id,cpu_guarantee_millis,cpu_max_millis,cpu_weight,memory_high_bytes,memory_max_bytes,pid_max,
		writable_quota_bytes,phase,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, reservation.AllocationID,
		reservation.Generation, reservation.RegistrationID, environment.ID, reservation.Resources.CPUGuaranteeMillis,
		reservation.Resources.CPUMaxMillis, reservation.Resources.CPUWeight, reservation.Resources.MemoryHighBytes,
		reservation.Resources.MemoryMaxBytes, reservation.Resources.PIDMax, reservation.Resources.WritableQuotaBytes,
		WorkloadReservationLogical, now, now); err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	env, err := store.GetEnvironment(ctx, environment.ID)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	stored, err := store.GetWorkloadReservation(ctx, reservation.AllocationID, reservation.Generation)
	return env, stored, err
}

func sumWorkloadReservations(ctx context.Context, tx *sql.Tx, registrationID string) (resourcev2.Capacity, error) {
	query := `SELECT COALESCE(SUM(cpu_guarantee_millis),0),COALESCE(SUM(cpu_max_millis),0),
		COALESCE(SUM(memory_max_bytes),0),COALESCE(SUM(pid_max),0),COALESCE(SUM(writable_quota_bytes),0)
		FROM workload_reservations_v2 WHERE phase IN (1,2,3,4)`
	var args []any
	if registrationID != "" {
		query += " AND registration_id=?"
		args = append(args, registrationID)
	}
	var value resourcev2.Capacity
	err := tx.QueryRowContext(ctx, query, args...).Scan(&value.CPUGuaranteeMillis, &value.CPUMaxMillis, &value.MemoryMaxBytes, &value.PIDMax, &value.WritableQuotaBytes)
	return value, err
}

func (store *Store) GetWorkloadReservation(ctx context.Context, allocationID string, generation uint64) (WorkloadReservation, error) {
	return scanWorkloadReservation(store.db.QueryRowContext(ctx, workloadReservationSelect+" WHERE allocation_id=? AND generation=?", allocationID, generation))
}

func (store *Store) GetWorkloadReservationByAllocation(ctx context.Context, registrationID, allocationID string) (WorkloadReservation, error) {
	return scanWorkloadReservation(store.db.QueryRowContext(ctx, workloadReservationSelect+" WHERE registration_id=? AND allocation_id=?", registrationID, allocationID))
}

func (store *Store) GetWorkloadReservationByEnvironment(ctx context.Context, environmentID string) (WorkloadReservation, error) {
	return scanWorkloadReservation(store.db.QueryRowContext(ctx, workloadReservationSelect+" WHERE environment_id=?", environmentID))
}

func (store *Store) WorkloadUsage(ctx context.Context, registrationID string) (resourcev2.Capacity, error) {
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return resourcev2.Capacity{}, err
	}
	defer tx.Rollback()
	return sumWorkloadReservations(ctx, tx, registrationID)
}

const workloadReservationSelect = `SELECT allocation_id,generation,registration_id,environment_id,
	cpu_guarantee_millis,cpu_max_millis,cpu_weight,memory_high_bytes,memory_max_bytes,pid_max,writable_quota_bytes,
	phase,guest_receipt,created_at,updated_at FROM workload_reservations_v2`

func scanWorkloadReservation(row rowScanner) (WorkloadReservation, error) {
	var value WorkloadReservation
	var created, updated int64
	err := row.Scan(&value.AllocationID, &value.Generation, &value.RegistrationID, &value.EnvironmentID,
		&value.Resources.CPUGuaranteeMillis, &value.Resources.CPUMaxMillis, &value.Resources.CPUWeight,
		&value.Resources.MemoryHighBytes, &value.Resources.MemoryMaxBytes, &value.Resources.PIDMax, &value.Resources.WritableQuotaBytes,
		&value.Phase, &value.GuestReceipt, &created, &updated)
	value.CreatedAt, value.UpdatedAt = time.Unix(0, created), time.Unix(0, updated)
	return value, err
}

func (store *Store) CommitGuestReservation(ctx context.Context, allocationID string, generation uint64, receipt []byte) (WorkloadReservation, error) {
	if len(receipt) == 0 {
		return WorkloadReservation{}, errors.New("guest reservation receipt is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkloadReservation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=?,guest_receipt=?,updated_at=?
		WHERE allocation_id=? AND generation=? AND phase IN (?,?) AND (length(guest_receipt)=0 OR guest_receipt=?)`,
		WorkloadReservationAdmitted, receipt, now, allocationID, generation,
		WorkloadReservationLogical, WorkloadReservationAdmitted, receipt)
	if err != nil {
		return WorkloadReservation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return WorkloadReservation{}, err
		}
		return WorkloadReservation{}, errors.New("guest reservation receipt conflict")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE environments SET allocation_state=?,updated_at=? WHERE allocation_state=? AND id=(
		SELECT environment_id FROM workload_reservations_v2 WHERE allocation_id=? AND generation=?)`,
		int32(hostv1.AllocationState_ALLOCATION_STATE_READY), now, int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING), allocationID, generation); err != nil {
		return WorkloadReservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkloadReservation{}, err
	}
	return store.GetWorkloadReservation(ctx, allocationID, generation)
}

// BeginWorkloadRetirement atomically makes a remote allocation terminal and
// creates its exact Host release obligation. A reserve response that was lost
// before admission remains release-required; an admitted row can immediately
// enter releasing because its exact Guest receipt is already durable.
func (store *Store) BeginWorkloadRetirement(ctx context.Context, environmentID, allocationID string, generation, expectedRevision uint64, retainedState int32) (Environment, WorkloadReservation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE environments SET allocation_state=?,policy_revision=policy_revision+1,updated_at=?
		WHERE id=? AND owner_kind=2 AND allocation_id=? AND policy_revision=?`, retainedState, now, environmentID, allocationID, expectedRevision)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
		if changeErr != nil {
			return Environment{}, WorkloadReservation{}, changeErr
		}
		return Environment{}, WorkloadReservation{}, errors.New("allocation revision conflict")
	}
	result, err = tx.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=CASE
		WHEN phase=? THEN ? WHEN phase=? THEN ? ELSE phase END,updated_at=?
		WHERE environment_id=? AND allocation_id=? AND generation=? AND phase IN (?,?,?,?)`,
		WorkloadReservationLogical, WorkloadReservationReleaseRequired,
		WorkloadReservationAdmitted, WorkloadReservationReleasing, now,
		environmentID, allocationID, generation, WorkloadReservationLogical,
		WorkloadReservationReleaseRequired, WorkloadReservationAdmitted, WorkloadReservationReleasing)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
		if changeErr != nil {
			return Environment{}, WorkloadReservation{}, changeErr
		}
		return Environment{}, WorkloadReservation{}, errors.New("exact workload reservation is not retireable")
	}
	if err := tx.Commit(); err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	environment, err := store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return Environment{}, WorkloadReservation{}, err
	}
	reservation, err := store.GetWorkloadReservation(ctx, allocationID, generation)
	return environment, reservation, err
}

// CommitGuestReservationForRelease persists a receipt recovered after a lost
// reserve response directly into the existing terminal obligation. It never
// makes the environment ready or exposes the reservation as admitted.
func (store *Store) CommitGuestReservationForRelease(ctx context.Context, allocationID string, generation uint64, receipt []byte) (WorkloadReservation, error) {
	if len(receipt) == 0 {
		return WorkloadReservation{}, errors.New("guest reservation receipt is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=?,guest_receipt=?,updated_at=?
		WHERE allocation_id=? AND generation=? AND phase IN (?,?) AND (length(guest_receipt)=0 OR guest_receipt=?)`,
		WorkloadReservationReleasing, receipt, time.Now().UnixNano(), allocationID, generation,
		WorkloadReservationReleaseRequired, WorkloadReservationReleasing, receipt)
	if err != nil {
		return WorkloadReservation{}, err
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
		if changeErr != nil {
			return WorkloadReservation{}, changeErr
		}
		return WorkloadReservation{}, errors.New("terminal Guest reservation receipt conflict")
	}
	return store.GetWorkloadReservation(ctx, allocationID, generation)
}

// BeginWorkloadRelease durably fences reconciliation before the exact Guest
// release crosses the Host/Guest failure boundary. Retrying the same identity
// is idempotent; another generation or receipt cannot adopt the reservation.
func (store *Store) BeginWorkloadRelease(ctx context.Context, allocationID string, generation uint64, receipt []byte) (WorkloadReservation, error) {
	if len(receipt) == 0 {
		return WorkloadReservation{}, errors.New("guest reservation receipt is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=?,updated_at=?
		WHERE allocation_id=? AND generation=? AND phase IN (?,?) AND guest_receipt=?`, WorkloadReservationReleasing,
		time.Now().UnixNano(), allocationID, generation, WorkloadReservationAdmitted, WorkloadReservationReleasing, receipt)
	if err != nil {
		return WorkloadReservation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return WorkloadReservation{}, err
		}
		return WorkloadReservation{}, errors.New("exact workload reservation is not releasable")
	}
	return store.GetWorkloadReservation(ctx, allocationID, generation)
}

// CommitGuestRelease durably records that Guest returned the exact released
// tombstone. The Host keeps this row until a later exact confirmation removes
// the Guest tombstone, so response loss at either boundary is restart-safe.
func (store *Store) CommitGuestRelease(ctx context.Context, allocationID string, generation uint64, receipt []byte) (WorkloadReservation, error) {
	if len(receipt) == 0 {
		return WorkloadReservation{}, errors.New("guest reservation receipt is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workload_reservations_v2 SET phase=?,updated_at=?
		WHERE allocation_id=? AND generation=? AND phase IN (?,?) AND guest_receipt=?`, WorkloadReservationReleasedPendingConfirm,
		time.Now().UnixNano(), allocationID, generation, WorkloadReservationReleasing, WorkloadReservationReleasedPendingConfirm, receipt)
	if err != nil {
		return WorkloadReservation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return WorkloadReservation{}, err
		}
		return WorkloadReservation{}, errors.New("exact workload release is not confirmable")
	}
	return store.GetWorkloadReservation(ctx, allocationID, generation)
}

func sumReservations(ctx context.Context, tx *sql.Tx, ownerID string, ownerKind int32) (ResourceAmount, error) {
	query := `SELECT COALESCE(SUM(cpu_millis), 0), COALESCE(SUM(memory_bytes), 0),
		COALESCE(SUM(pids), 0), COALESCE(SUM(disk_bytes), 0) FROM resource_reservations`
	var args []any
	if ownerID != "" {
		query += " WHERE owner_kind=? AND owner_id=?"
		args = []any{ownerKind, ownerID}
	}
	var amount ResourceAmount
	err := tx.QueryRowContext(ctx, query, args...).Scan(&amount.CPUMillis, &amount.MemoryBytes, &amount.PIDs, &amount.DiskBytes)
	return amount, err
}

func (store *Store) ReleaseResources(ctx context.Context, id string) error {
	result, err := store.db.ExecContext(ctx, `DELETE FROM resource_reservations WHERE id=?`, id)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return nil
}
