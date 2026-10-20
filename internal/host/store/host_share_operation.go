package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

// Accept the authority fence and its resumable operation in one transaction.
func (store *Store) BeginHostShareMutation(ctx context.Context, operation Operation, candidate HostShare, expected uint64) (Operation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback()
	current, err := scanOperation(tx.QueryRowContext(ctx, `SELECT id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at,request_sha256,receipt FROM operations WHERE request_id=?`, operation.RequestID))
	if err == nil {
		if current.Kind != operation.Kind || current.EnvironmentID != operation.EnvironmentID || !bytes.Equal(current.RequestSHA256, operation.RequestSHA256) {
			return Operation{}, ErrOperationReceiptConflict
		}
		return current, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	var approved bool
	if err := tx.QueryRowContext(ctx, hostShareEnvironmentApprovedQuery, candidate.EnvironmentID).Scan(&approved); err != nil {
		return Operation{}, err
	}
	if !approved && operation.Kind != "host-share-revoke" {
		return Operation{}, ErrOperationReceiptConflict
	}
	if expected == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO host_shares(`+hostShareColumns+`) VALUES(?,?,?,?,?,?,?,?,?,?)`, candidate.ID, candidate.EnvironmentID, candidate.Source.Path, candidate.Source.FileSystemUUID, strconv.FormatUint(candidate.Source.FileID, 10), candidate.ReadOnly, candidate.Revision, hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE, "waiting for runtime projection", operation.ID)
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,stage='superseded',error_reason='HOST_SHARE_AUTHORITY_CHANGED',error_message='a newer operator intent superseded this operation',updated_at=? WHERE id=(SELECT operation_id FROM host_shares WHERE id=? AND revision=?) AND state=?`, hostv1.OperationState_OPERATION_STATE_INTERRUPTED, time.Now().UnixNano(), candidate.ID, expected, hostv1.OperationState_OPERATION_STATE_RUNNING); err != nil {
			return Operation{}, err
		}
		result, updateErr := tx.ExecContext(ctx, `UPDATE host_shares SET state=?,revision=revision+1,operation_id=?,unavailable_reason='' WHERE id=? AND environment_id=? AND revision=? AND state<>?`, hostv1.HostShareState_HOST_SHARE_STATE_REVOKING, operation.ID, candidate.ID, candidate.EnvironmentID, expected, hostv1.HostShareState_HOST_SHARE_STATE_REVOKED)
		err = updateErr
		if err == nil {
			count, countErr := result.RowsAffected()
			err = countErr
			if count == 0 && err == nil {
				err = ErrOperationReceiptConflict
			}
		}
	}
	if err != nil {
		return Operation{}, err
	}
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(ctx, `INSERT INTO operations(id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at,request_sha256,receipt) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, operation.ID, operation.Kind, operation.EnvironmentID, operation.State, operation.Stage, operation.RequestID, "", "", "", now, now, operation.RequestSHA256, operation.Receipt)
	if err != nil {
		return Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, err
	}
	return operation, nil
}

func (store *Store) FinishHostShareMutation(ctx context.Context, operation Operation, value HostShare) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE host_shares SET source_path=?,filesystem_uuid=?,file_id=?,read_only=?,state=?,unavailable_reason=? WHERE id=? AND revision=? AND operation_id=?`, value.Source.Path, value.Source.FileSystemUUID, strconv.FormatUint(value.Source.FileID, 10), value.ReadOnly, value.State, value.UnavailableReason, value.ID, value.Revision, operation.ID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrOperationReceiptConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE operations SET state=?,stage=?,receipt=?,error_reason='',error_message='',updated_at=? WHERE id=?`, operation.State, operation.Stage, operation.Receipt, time.Now().UnixNano(), operation.ID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) CompleteEnvironmentHostShareRevocation(ctx context.Context, environment string) ([]string, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,stage='superseded',error_reason='HOST_SHARE_AUTHORITY_CHANGED',error_message='service environment share authority was revoked',updated_at=? WHERE state=? AND id IN(SELECT operation_id FROM host_shares WHERE environment_id=? AND state<>?) AND NOT EXISTS(SELECT 1 FROM agent_integrations WHERE environment_id=? AND desired_state=1 AND config_state NOT IN(6,7))`, hostv1.OperationState_OPERATION_STATE_INTERRUPTED, time.Now().UnixNano(), hostv1.OperationState_OPERATION_STATE_RUNNING, environment, hostv1.HostShareState_HOST_SHARE_STATE_REVOKED, environment); err != nil {
		return nil, err
	}
	var approved bool
	if err := tx.QueryRowContext(ctx, hostShareEnvironmentApprovedQuery, environment).Scan(&approved); err != nil {
		return nil, err
	}
	if approved {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM host_shares WHERE environment_id=? AND state<>?`, environment, hostv1.HostShareState_HOST_SHARE_STATE_REVOKED)
	if err != nil {
		return nil, err
	}
	var revoked []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		revoked = append(revoked, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE host_shares SET state=?,revision=revision+1,operation_id='',unavailable_reason='' WHERE environment_id=? AND state<>?`, hostv1.HostShareState_HOST_SHARE_STATE_REVOKED, environment, hostv1.HostShareState_HOST_SHARE_STATE_REVOKED); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return revoked, nil
}
