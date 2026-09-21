package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

const applianceOperationInsert = `INSERT INTO appliance_operations(id, installation_id, request_id,
	kind, state, expected_revision, deadline_unix_millis, stage, error_reason, error_message,
	operation_phase, terminal_reason, receipt, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func applianceOperationArgs(operation ApplianceOperation, now int64) []any {
	return []any{operation.ID, operation.InstallationID, operation.RequestID, operation.Kind, operation.State,
		operation.ExpectedRevision, operation.DeadlineUnixMillis, operation.Stage, operation.ErrorReason,
		operation.ErrorMessage, operation.Phase, operation.TerminalReason, applianceBlob(operation.Receipt), now, now}
}

func applianceBlob(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func insertApplianceOperation(ctx context.Context, tx *sql.Tx, operation ApplianceOperation, now int64) error {
	_, err := tx.ExecContext(ctx, applianceOperationInsert, applianceOperationArgs(operation, now)...)
	return err
}

func (store *Store) CreateApplianceOperation(ctx context.Context, operation ApplianceOperation) (ApplianceOperation, error) {
	now := time.Now().UnixNano()
	if _, err := store.db.ExecContext(ctx, applianceOperationInsert, applianceOperationArgs(operation, now)...); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, operation.ID)
}

// BeginApplianceStateMutation commits the installation fence and its operation
// receipt together. A failed receipt insert must not leave a revision change
// that cannot be retried or observed.
func (store *Store) BeginApplianceStateMutation(ctx context.Context, installation ApplianceInstallation, expectedRevision uint64, operation ApplianceOperation) (ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceOperation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE appliance_installations SET desired_enabled=?, state=?,
		registration_id=NULLIF(?, ''), bootstrap_id=?, current_boot_id=?, current_container_instance=?, revision=revision+1,
		updated_at=? WHERE id=? AND revision=?`, installation.DesiredEnabled, installation.State, installation.RegistrationID,
		installation.BootstrapID, installation.CurrentBootID, installation.CurrentContainerInstance, now, installation.ID, expectedRevision)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	if err := insertApplianceOperation(ctx, tx, operation, now); err != nil {
		return ApplianceOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, operation.ID)
}

const applianceOperationSelect = `SELECT id, installation_id, request_id, kind, state,
	expected_revision, deadline_unix_millis, stage, error_reason, error_message, operation_phase,
	terminal_reason, receipt, created_at, updated_at FROM appliance_operations`

func (store *Store) GetApplianceOperation(ctx context.Context, id string) (ApplianceOperation, error) {
	return scanApplianceOperation(store.db.QueryRowContext(ctx, applianceOperationSelect+" WHERE id=?", id))
}

func (store *Store) ApplianceOperationByRequest(ctx context.Context, requestID string) (ApplianceOperation, error) {
	return scanApplianceOperation(store.db.QueryRowContext(ctx, applianceOperationSelect+" WHERE request_id=?", requestID))
}

func (store *Store) ListRunningApplianceOperations(ctx context.Context) ([]ApplianceOperation, error) {
	return store.listApplianceOperations(ctx, " WHERE state=1 ORDER BY created_at, id")
}

func (store *Store) HasRunningApplianceOperation(ctx context.Context, installationID string) (bool, error) {
	var exists bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM appliance_operations WHERE installation_id=? AND state=1
	)`, installationID).Scan(&exists)
	return exists, err
}

func (store *Store) ListPendingApplianceRemovals(ctx context.Context) ([]ApplianceOperation, error) {
	return store.listApplianceOperations(ctx, " WHERE state=1 AND operation_phase=4 ORDER BY created_at, id")
}

func (store *Store) listApplianceOperations(ctx context.Context, suffix string) ([]ApplianceOperation, error) {
	rows, err := store.db.QueryContext(ctx, applianceOperationSelect+suffix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []ApplianceOperation
	for rows.Next() {
		value, err := scanApplianceOperation(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanApplianceOperation(row rowScanner) (ApplianceOperation, error) {
	var value ApplianceOperation
	var created, updated int64
	err := row.Scan(&value.ID, &value.InstallationID, &value.RequestID, &value.Kind, &value.State,
		&value.ExpectedRevision, &value.DeadlineUnixMillis, &value.Stage, &value.ErrorReason,
		&value.ErrorMessage, &value.Phase, &value.TerminalReason, &value.Receipt, &created, &updated)
	value.CreatedAt, value.UpdatedAt = time.Unix(0, created), time.Unix(0, updated)
	return value, err
}

func (store *Store) TransitionApplianceOperation(ctx context.Context, id string, transition ApplianceOperationTransition) (ApplianceOperation, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE appliance_operations SET state=?, operation_phase=?,
		terminal_reason=?, stage=?, error_reason=?, error_message=?, receipt=?, updated_at=?
		WHERE id=? AND state=? AND operation_phase=?`, transition.State, transition.Phase,
		transition.TerminalReason, transition.Stage, transition.ErrorReason, transition.ErrorMessage,
		applianceBlob(transition.Receipt), time.Now().UnixNano(), id, transition.ExpectedState, transition.ExpectedPhase)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	return store.GetApplianceOperation(ctx, id)
}

func (store *Store) BeginApplianceRemove(ctx context.Context, remove ApplianceRemove) (ApplianceInstallation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	if err := insertApplianceOperation(ctx, tx, remove.Operation, now); err != nil {
		return ApplianceInstallation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE appliance_installations SET desired_enabled=0, state=?,
		revision=revision+1, current_boot_id='', current_container_instance='', updated_at=?
		WHERE id=? AND revision=? AND state=?`, remove.PendingState, now, remove.InstallationID,
		remove.ExpectedRevision, remove.ExpectedState)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceInstallation{}, err
		}
		return ApplianceInstallation{}, ErrInstallationRevisionConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE appliance_operations SET operation_phase=4,
		stage='pending-destroy', receipt=?, updated_at=? WHERE id=? AND installation_id=?
		AND state=1 AND operation_phase=?`, applianceBlob(remove.Receipt), now, remove.OperationID,
		remove.InstallationID, remove.ExpectedPhase)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceInstallation{}, err
		}
		return ApplianceInstallation{}, ErrOperationReceiptConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceInstallation{}, err
	}
	return store.GetApplianceInstallation(ctx, remove.InstallationID)
}

// CommitSealedRepair records the post-key-replacement installation fence and
// terminal receipt together. If the process dies before this transaction, the
// installation remains SEALED and the planned operation is safely retryable.
func (store *Store) CommitSealedRepair(ctx context.Context, installationID string, expectedRevision uint64, operationID string, receipt []byte) (ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceOperation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE appliance_installations SET desired_enabled=0, state=?,
		revision=revision+1, current_boot_id='', current_container_instance='', updated_at=?
		WHERE id=? AND revision=? AND state=?`, appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED, now, installationID, expectedRevision,
		appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE appliance_operations SET state=?, operation_phase=?,
		terminal_reason=?, stage='sealed-repaired', receipt=?, updated_at=?
		WHERE id=? AND installation_id=? AND expected_revision=? AND state=? AND operation_phase=?`,
		appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED,
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL,
		appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED,
		applianceBlob(receipt), now, operationID, installationID, expectedRevision,
		appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING,
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, operationID)
}

func (store *Store) CommitApplianceRemove(ctx context.Context, remove ApplianceRemove) (ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceOperation{}, err
	}
	defer tx.Rollback()
	var blockers int
	if remove.RegistrationID != "" {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM environments WHERE registration_id=?`, remove.RegistrationID).Scan(&blockers); err != nil {
			return ApplianceOperation{}, err
		}
	}
	if blockers != 0 {
		return ApplianceOperation{}, ErrApplianceDomainNotEmpty
	}
	var authorityID string
	if err := tx.QueryRowContext(ctx, `SELECT authority_id FROM appliance_trust_authorities WHERE installation_id=?`, remove.InstallationID).Scan(&authorityID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ApplianceOperation{}, err
	}
	if authorityID != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO revoked_appliance_authorities(authority_id, installation_id, revoked_at)
			VALUES(?, ?, ?) ON CONFLICT(authority_id) DO NOTHING`, authorityID, remove.InstallationID, time.Now().UnixNano()); err != nil {
			return ApplianceOperation{}, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM membership_credentials WHERE authority_id=?`, authorityID); err != nil {
			return ApplianceOperation{}, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM trust_anchors WHERE authority_id=?`, authorityID); err != nil {
			return ApplianceOperation{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM appliance_installations WHERE id=? AND revision=?`, remove.InstallationID, remove.CommitRevision)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	if remove.RegistrationID != "" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM remote_registrations WHERE id=?`, remove.RegistrationID); err != nil {
			return ApplianceOperation{}, err
		}
	}
	result, err = tx.ExecContext(ctx, `UPDATE appliance_operations SET state=2, operation_phase=5,
		terminal_reason=7, stage='removed', receipt=?, updated_at=? WHERE id=? AND installation_id=?
		AND state=1 AND operation_phase=4`, applianceBlob(remove.Receipt), time.Now().UnixNano(),
		remove.OperationID, remove.InstallationID)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, remove.OperationID)
}
