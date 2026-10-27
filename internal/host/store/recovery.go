package store

import (
	"context"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

const resumableOperationKinds = "'local-disposal','host-share-create','host-share-rebind','host-share-revoke'"

func (store *Store) RecoverAfterRestart(ctx context.Context, now time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=?, stage='interrupted', error_reason='SIDECAR_RESTARTED',
		error_message='the sidecar restarted before the operation completed', updated_at=? WHERE state=? AND kind NOT IN (`+resumableOperationKinds+`)`,
		int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED), now.UnixNano(), int32(hostv1.OperationState_OPERATION_STATE_RUNNING)); err != nil {
		return fmt.Errorf("recover interrupted operations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=?, terminal_reason='SIDECAR_RESTARTED'
		WHERE revoked_at IS NULL AND closed_at IS NULL`, now.UnixNano()); err != nil {
		return fmt.Errorf("recover agent sessions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE remote_registrations SET connection_state=1, connection_instance='',
		enrollment_state=CASE WHEN enrollment_state=1 AND owner_kind=1 THEN 3 ELSE enrollment_state END,
		enrollment_error_code=CASE WHEN enrollment_state=1 AND owner_kind=1 THEN 'ENROLLMENT_INTERRUPTED' ELSE enrollment_error_code END,
		enrollment_error_message=CASE WHEN enrollment_state=1 AND owner_kind=1 THEN 'Enrollment did not finish. Retry setup.' ELSE enrollment_error_message END,
		connector_phase=1, connector_observed_at=?, updated_at=?`, now.UnixNano(), now.UnixNano()); err != nil {
		return fmt.Errorf("recover remote connection state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_listeners SET revoked=1, revision=revision+1, updated_at=? WHERE revoked=0 AND local=0`, now.UnixNano()); err != nil {
		return fmt.Errorf("recover service listeners: %w", err)
	}
	return tx.Commit()
}
