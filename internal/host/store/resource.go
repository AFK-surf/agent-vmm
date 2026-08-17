package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
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

func (store *Store) AdmitRemoteEnvironment(ctx context.Context, environment Environment,
	reservation ResourceReservation, hostCapacity, domainCeiling ResourceAmount) (Environment, error) {
	if environment.OwnerKind != OwnerRemoteRegistration || environment.RegistrationID == "" || environment.AllocationID == "" {
		return Environment{}, errors.New("remote environment ownership is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Environment{}, err
	}
	defer tx.Rollback()
	total, err := sumReservations(ctx, tx, "", 0)
	if err != nil {
		return Environment{}, err
	}
	domain, err := sumReservations(ctx, tx, reservation.OwnerID, reservation.OwnerKind)
	if err != nil {
		return Environment{}, err
	}
	if !total.add(reservation.Amount).fits(hostCapacity) {
		return Environment{}, ErrResourceCapacityExceeded
	}
	if !domain.add(reservation.Amount).fits(domainCeiling) {
		return Environment{}, ErrDomainCapacityExceeded
	}
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_reservations(id, owner_kind, owner_id,
		resource_kind, resource_id, cpu_millis, memory_bytes, pids, disk_bytes, retained, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, reservation.ID, reservation.OwnerKind,
		reservation.OwnerID, reservation.ResourceKind, environment.ID, reservation.Amount.CPUMillis,
		reservation.Amount.MemoryBytes, reservation.Amount.PIDs, reservation.Amount.DiskBytes,
		reservation.Retained, now); err != nil {
		return Environment{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO environments(id, namespace_id, display_name,
		cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, owner_kind, registration_id,
		allocation_id, allocation_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		environment.ID, environment.NamespaceID, environment.DisplayName, environment.CPUMillis,
		environment.MemoryBytes, environment.PIDs, environment.DiskBytes, environment.EgressMode,
		environment.OwnerKind, environment.RegistrationID, environment.AllocationID,
		environment.AllocationState, now, now); err != nil {
		return Environment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Environment{}, err
	}
	return store.GetEnvironment(ctx, environment.ID)
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
