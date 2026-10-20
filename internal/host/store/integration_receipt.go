package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func integrationCommandReplay(ctx context.Context, tx *sql.Tx, operation Operation) (AgentIntegration, bool, error) {
	prior, err := scanOperation(tx.QueryRowContext(ctx, `SELECT id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at,request_sha256,receipt FROM operations WHERE request_id=?`, operation.RequestID))
	if errors.Is(err, sql.ErrNoRows) {
		return AgentIntegration{}, false, nil
	}
	if err != nil {
		return AgentIntegration{}, false, err
	}
	if prior.Kind != operation.Kind || !bytes.Equal(prior.RequestSHA256, operation.RequestSHA256) || prior.State != 2 {
		return AgentIntegration{}, false, ErrOperationReceiptConflict
	}
	var receipt AgentIntegration
	if err := json.Unmarshal(prior.Receipt, &receipt); err != nil {
		return AgentIntegration{}, false, err
	}
	return receipt, true, nil
}

func recordIntegrationCommand(ctx context.Context, tx *sql.Tx, operation Operation, receipt AgentIntegration) error {
	body, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(ctx, `INSERT INTO operations(id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at,request_sha256,receipt) VALUES(?,?,?,2,'applied',?,'','','',?,?,?,?)`, operation.ID, operation.Kind, receipt.EnvironmentID, operation.RequestID, now, now, operation.RequestSHA256, body)
	return err
}
