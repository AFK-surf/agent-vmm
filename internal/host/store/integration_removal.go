package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

func (store *Store) BeginAgentIntegrationRemoval(ctx context.Context, requestID, id string, expectedRevision uint64) (AgentIntegration, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, err
	}
	defer tx.Rollback()
	operation := integrationRemovalOperation("integration-removal-begin", requestID, id, expectedRevision)
	if receipt, found, err := integrationCommandReplay(ctx, tx, operation); err != nil || found {
		return receipt, err
	}
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE agent_integrations SET config_state=6,updated_at=?,revision=revision+1 WHERE id=? AND revision=? AND desired_state=1 AND config_state NOT IN(6,7,8)`, now, id, expectedRevision)
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
	var environmentInUse bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_integrations WHERE environment_id=? AND desired_state=1 AND config_state NOT IN(6,7))`, current.EnvironmentID).Scan(&environmentInUse); err != nil {
		return AgentIntegration{}, err
	}
	if !environmentInUse {
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=4,stage='superseded',error_reason='HOST_SHARE_AUTHORITY_CHANGED',error_message='service environment is being removed',updated_at=? WHERE state=1 AND id IN(SELECT operation_id FROM host_shares WHERE environment_id=? AND state<>4)`, now, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE host_shares SET state=3,revision=revision+1,operation_id='',unavailable_reason='service environment is being removed' WHERE environment_id=? AND state<>4`, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE managed_containers SET desired_running=0,revision=revision+1 WHERE environment_id=? AND desired_running=1`, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_listeners SET revoked=1,revision=revision+1,updated_at=? WHERE local=1 AND environment_id=? AND revoked=0`, now, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_exports SET revoked=1,revision=revision+1,updated_at=? WHERE environment_id=? AND revoked=0`, now, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1,revision=revision+1,updated_at=? WHERE revoked=0 AND (source_environment_id=? OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id=?))`, now, current.EnvironmentID, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=4,revision=revision+1,generation=generation+1,updated_at=? WHERE import_id IN (SELECT import_id FROM service_imports WHERE source_environment_id=? OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id=?)) AND state NOT IN (4,5)`, now, current.EnvironmentID, current.EnvironmentID); err != nil {
			return AgentIntegration{}, err
		}
	}
	if err := recordIntegrationCommand(ctx, tx, operation, current); err != nil {
		return AgentIntegration{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentIntegration{}, err
	}
	return current, nil
}

func (store *Store) CheckAgentIntegrationRemoval(ctx context.Context, requestID, id string, expectedRevision uint64) (AgentIntegration, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, false, err
	}
	defer tx.Rollback()
	operation := integrationRemovalOperation("integration-removal-complete", requestID, id, expectedRevision)
	if receipt, found, err := integrationCommandReplay(ctx, tx, operation); err != nil || found {
		return receipt, found, err
	}
	current, err := scanAgentIntegration(tx.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil {
		return AgentIntegration{}, false, err
	}
	if current.Revision != expectedRevision || current.ConfigState != 6 {
		return AgentIntegration{}, false, ErrOperationReceiptConflict
	}
	return current, false, nil
}

func (store *Store) CompleteAgentIntegrationRemoval(ctx context.Context, requestID, id string, expectedRevision uint64) (AgentIntegration, error) {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, err
	}
	defer transaction.Rollback()
	operation := integrationRemovalOperation("integration-removal-complete", requestID, id, expectedRevision)
	if receipt, found, err := integrationCommandReplay(ctx, transaction, operation); err != nil || found {
		return receipt, err
	}
	current, err := scanAgentIntegration(transaction.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil || current.Revision != expectedRevision || current.ConfigState != 6 {
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	var active int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions WHERE integration_id=? AND revoked_at IS NULL AND closed_at IS NULL`, id).Scan(&active); err != nil {
		return AgentIntegration{}, err
	}
	if active != 0 {
		return AgentIntegration{}, errors.New("integration sessions are not revoked")
	}
	result, err := transaction.ExecContext(ctx, `UPDATE agent_integrations SET desired_state=2, config_state=7,
		config_fingerprint='', updated_at=?, revision=revision+1 WHERE id=? AND revision=? AND config_state=6`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	current, err = scanAgentIntegration(transaction.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil {
		return AgentIntegration{}, err
	}
	if err := recordIntegrationCommand(ctx, transaction, operation, current); err != nil {
		return AgentIntegration{}, err
	}
	if err := transaction.Commit(); err != nil {
		return AgentIntegration{}, err
	}
	return current, nil
}

func integrationRemovalOperation(kind, requestID, id string, revision uint64) Operation {
	key := sha256.Sum256([]byte(kind + "\x00" + requestID))
	body, _ := json.Marshal(struct {
		RequestID        string
		IntegrationID    string
		ExpectedRevision uint64
	}{requestID, id, revision})
	digest := sha256.Sum256(body)
	return Operation{ID: "integration-" + hex.EncodeToString(key[:]), RequestID: "integration-" + hex.EncodeToString(key[:]), Kind: kind, RequestSHA256: digest[:], State: 2, Stage: "applied"}
}
