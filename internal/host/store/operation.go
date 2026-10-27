package store

import (
	"context"
	"time"
)

type Operation struct {
	RequestSHA256 []byte
	Receipt       []byte
	ID            string
	Kind          string
	EnvironmentID string
	State         int32
	Stage         string
	RequestID     string
	ErrorReason   string
	ErrorMessage  string
	OutputPath    string
	StartedAt     time.Time
	UpdatedAt     time.Time
}

func (store *Store) PutOperation(ctx context.Context, operation Operation) (Operation, bool, error) {
	return store.putOperation(ctx, operation, nil)
}

func (store *Store) putOperation(ctx context.Context, operation Operation, start *ManagedContainer) (Operation, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, false, err
	}
	defer tx.Rollback()
	now := time.Now()
	result, err := tx.ExecContext(ctx, `INSERT INTO operations
		(id, kind, environment_id, state, stage, request_id, error_reason, error_message, output_path, started_at, updated_at, request_sha256, receipt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(request_id) DO NOTHING`, operation.ID, operation.Kind, operation.EnvironmentID, operation.State,
		operation.Stage, operation.RequestID, operation.ErrorReason, operation.ErrorMessage, operation.OutputPath, now.UnixNano(), now.UnixNano(), operation.RequestSHA256, operation.Receipt)
	if err != nil {
		return Operation{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return Operation{}, false, err
	}
	stored, err := scanOperation(tx.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at, request_sha256, receipt FROM operations WHERE request_id=?`, operation.RequestID))
	if err != nil {
		return Operation{}, false, err
	}
	if inserted == 1 && start != nil {
		if _, err := tx.ExecContext(ctx, managedContainerRunningQuery, start.ContainerID, start.ContainerGenerationID, true, start.EnvironmentID, true); err != nil {
			return Operation{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, false, err
	}
	return stored, inserted == 1, nil
}

func (store *Store) OperationByRequestID(ctx context.Context, requestID string) (Operation, error) {
	return scanOperation(store.db.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at, request_sha256, receipt FROM operations WHERE request_id=?`, requestID))
}

func (store *Store) GetOperation(ctx context.Context, id string) (Operation, error) {
	return scanOperation(store.db.QueryRowContext(ctx, `SELECT id, kind, environment_id, state, stage,
		request_id, error_reason, error_message, output_path, started_at, updated_at, request_sha256, receipt FROM operations WHERE id=?`, id))
}

func scanOperation(row rowScanner) (Operation, error) {
	var operation Operation
	var started, updated int64
	err := row.Scan(&operation.ID, &operation.Kind, &operation.EnvironmentID, &operation.State, &operation.Stage,
		&operation.RequestID, &operation.ErrorReason, &operation.ErrorMessage, &operation.OutputPath, &started, &updated, &operation.RequestSHA256, &operation.Receipt)
	operation.StartedAt = time.Unix(0, started)
	operation.UpdatedAt = time.Unix(0, updated)
	return operation, err
}

func (store *Store) UpdateOperation(ctx context.Context, operation Operation) error {
	_, err := store.db.ExecContext(ctx, `UPDATE operations SET state=?, stage=?, error_reason=?, error_message=?, output_path=?, updated_at=?,receipt=? WHERE id=?`,
		operation.State, operation.Stage, operation.ErrorReason, operation.ErrorMessage, operation.OutputPath, time.Now().UnixNano(), operation.Receipt, operation.ID)
	return err
}
