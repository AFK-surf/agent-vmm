package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

func integrationCommitOperation(requestID, id string, revision uint64, fingerprint string, configState int32) Operation {
	key := sha256.Sum256([]byte("integration-commit\x00" + requestID))
	body, _ := json.Marshal(struct {
		RequestID        string
		IntegrationID    string
		ExpectedRevision uint64
		Fingerprint      string
		ConfigState      int32
	}{requestID, id, revision, fingerprint, configState})
	digest := sha256.Sum256(body)
	return Operation{ID: "integration-" + hex.EncodeToString(key[:]), RequestID: "integration-" + hex.EncodeToString(key[:]), Kind: "integration-commit", RequestSHA256: digest[:], State: 2, Stage: "applied"}
}

func (store *Store) CheckAgentIntegrationCommit(ctx context.Context, requestID, id string, revision uint64, fingerprint string, configState int32) (AgentIntegration, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, false, err
	}
	defer tx.Rollback()
	operation := integrationCommitOperation(requestID, id, revision, fingerprint, configState)
	if receipt, replay, err := integrationCommandReplay(ctx, tx, operation); err != nil || replay {
		return receipt, replay, err
	}
	current, err := scanAgentIntegration(tx.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil {
		return AgentIntegration{}, false, err
	}
	if current.Revision != revision || current.DesiredState != 1 || current.ConfigState == 6 || current.ConfigState == 7 {
		return AgentIntegration{}, false, ErrOperationReceiptConflict
	}
	return current, false, nil
}

func (store *Store) CommitAgentIntegration(ctx context.Context, requestID, id string, expectedRevision uint64, fingerprint string, configState int32) (AgentIntegration, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, err
	}
	defer tx.Rollback()
	operation := integrationCommitOperation(requestID, id, expectedRevision, fingerprint, configState)
	if receipt, replay, err := integrationCommandReplay(ctx, tx, operation); err != nil || replay {
		return receipt, err
	}
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE agent_integrations SET config_state=?, config_fingerprint=?,
		last_error_code='', last_error_message='', last_verified_at=?, updated_at=?, revision=revision+1
		WHERE id=? AND revision=? AND desired_state=1 AND config_state IN (1,2,3,4,5,8)`, configState, fingerprint, now, now, id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return AgentIntegration{}, errors.Join(ErrOperationReceiptConflict, err)
	}
	current, err := scanAgentIntegration(tx.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil {
		return AgentIntegration{}, err
	}
	if err := recordIntegrationCommand(ctx, tx, operation, current); err != nil {
		return AgentIntegration{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentIntegration{}, err
	}
	return current, nil
}
