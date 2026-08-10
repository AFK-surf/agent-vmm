package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type AgentIntegration struct {
	ID                string
	AdapterID         string
	DisplayName       string
	Scope             int32
	ConfigTarget      string
	EnvironmentID     string
	DesiredState      int32
	ConfigState       int32
	ConfigFingerprint string
	Revision          uint64
	LastErrorCode     string
	LastErrorMessage  string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	LastVerifiedAt    *time.Time
	PrepareRequestID  string
}

func (store *Store) CreateAgentIntegration(ctx context.Context, integration AgentIntegration) (AgentIntegration, error) {
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO agent_integrations(
		id, adapter_id, display_name, scope, config_target, environment_id, desired_state, config_state,
		config_fingerprint, last_error_code, last_error_message, created_at, updated_at, prepare_request_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', '', ?, ?, ?)`, integration.ID, integration.AdapterID,
		integration.DisplayName, integration.Scope, integration.ConfigTarget, integration.EnvironmentID,
		integration.DesiredState, integration.ConfigState, now, now, integration.PrepareRequestID)
	if err != nil {
		return AgentIntegration{}, err
	}
	return store.GetAgentIntegration(ctx, integration.ID)
}

func (store *Store) GetAgentIntegration(ctx context.Context, id string) (AgentIntegration, error) {
	return scanAgentIntegration(store.db.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
}

func (store *Store) AgentIntegrationByPrepareRequest(ctx context.Context, requestID string) (AgentIntegration, error) {
	return scanAgentIntegration(store.db.QueryRowContext(ctx, agentIntegrationSelect+" WHERE prepare_request_id=?", requestID))
}

func (store *Store) ListAgentIntegrations(ctx context.Context, includeRemoved bool) ([]AgentIntegration, error) {
	query := agentIntegrationSelect
	if !includeRemoved {
		query += " WHERE desired_state != 2"
	}
	query += " ORDER BY display_name, id"
	rows, err := store.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []AgentIntegration
	for rows.Next() {
		integration, err := scanAgentIntegration(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, integration)
	}
	return result, rows.Err()
}

const agentIntegrationSelect = `SELECT id, adapter_id, display_name, scope, config_target, environment_id,
	desired_state, config_state, config_fingerprint, revision, last_error_code, last_error_message,
	created_at, updated_at, last_verified_at, prepare_request_id FROM agent_integrations`

func scanAgentIntegration(row rowScanner) (AgentIntegration, error) {
	var integration AgentIntegration
	var created, updated int64
	var verified sql.NullInt64
	err := row.Scan(&integration.ID, &integration.AdapterID, &integration.DisplayName, &integration.Scope,
		&integration.ConfigTarget, &integration.EnvironmentID, &integration.DesiredState, &integration.ConfigState,
		&integration.ConfigFingerprint, &integration.Revision, &integration.LastErrorCode, &integration.LastErrorMessage,
		&created, &updated, &verified, &integration.PrepareRequestID)
	if err != nil {
		return AgentIntegration{}, err
	}
	integration.CreatedAt = time.Unix(0, created)
	integration.UpdatedAt = time.Unix(0, updated)
	if verified.Valid {
		value := time.Unix(0, verified.Int64)
		integration.LastVerifiedAt = &value
	}
	return integration, nil
}

func (store *Store) CommitAgentIntegration(ctx context.Context, id string, expectedRevision uint64, fingerprint string, configState int32) (AgentIntegration, error) {
	now := time.Now().UnixNano()
	result, err := store.db.ExecContext(ctx, `UPDATE agent_integrations SET config_state=?, config_fingerprint=?,
		last_error_code='', last_error_message='', last_verified_at=?, updated_at=?, revision=revision+1
		WHERE id=? AND revision=? AND desired_state=1 AND config_state IN (1,3,4,5,6,8)`, configState, fingerprint, now, now, id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AgentIntegration{}, err
		}
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	return store.GetAgentIntegration(ctx, id)
}

func (store *Store) BeginAgentIntegrationConfiguration(ctx context.Context, id string, expectedRevision uint64) (AgentIntegration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE agent_integrations SET config_state=8,
		last_error_code='CONFIGURATION_STARTED', last_error_message='', updated_at=?, revision=revision+1
		WHERE id=? AND revision=? AND desired_state=1 AND config_state IN (1,5,8)`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AgentIntegration{}, err
		}
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	return store.GetAgentIntegration(ctx, id)
}

func (store *Store) FailAgentIntegration(ctx context.Context, id string, expectedRevision uint64, code, message string) (AgentIntegration, error) {
	state := 5
	if code == "CONFIG_CONFLICT" {
		state = 4
	}
	result, err := store.db.ExecContext(ctx, `UPDATE agent_integrations SET config_state=CASE WHEN config_state=6 THEN 6 ELSE ? END, last_error_code=?,
		last_error_message=?, updated_at=?, revision=revision+1 WHERE id=? AND revision=? AND desired_state=1`,
		state, code, message, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AgentIntegration{}, err
		}
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	return store.GetAgentIntegration(ctx, id)
}

func (store *Store) BeginAgentIntegrationRemoval(ctx context.Context, id string, expectedRevision uint64) (AgentIntegration, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE agent_integrations SET config_state=6, updated_at=?, revision=revision+1
		WHERE id=? AND revision=? AND desired_state=1 AND config_state != 8`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AgentIntegration{}, err
		}
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	return store.GetAgentIntegration(ctx, id)
}

func (store *Store) CompleteAgentIntegrationRemoval(ctx context.Context, id string, expectedRevision uint64) (AgentIntegration, error) {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentIntegration{}, err
	}
	defer transaction.Rollback()
	current, err := scanAgentIntegration(transaction.QueryRowContext(ctx, agentIntegrationSelect+" WHERE id=?", id))
	if err != nil || current.Revision != expectedRevision || current.ConfigState != 6 {
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	if current.ConfigFingerprint == "" && current.LastErrorCode == "" && current.EnvironmentID == "integration-"+current.ID {
		result, err := transaction.ExecContext(ctx, `DELETE FROM agent_integrations WHERE id=? AND revision=?`, id, expectedRevision)
		if err != nil {
			return AgentIntegration{}, err
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return AgentIntegration{}, errors.New("integration revision conflict")
		}
		result, err = transaction.ExecContext(ctx, `DELETE FROM environments WHERE id=?
			AND NOT EXISTS (SELECT 1 FROM agent_integrations WHERE environment_id=?)
			AND NOT EXISTS (SELECT 1 FROM sessions WHERE environment_id=?)`, current.EnvironmentID, current.EnvironmentID, current.EnvironmentID)
		if err != nil {
			return AgentIntegration{}, err
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return AgentIntegration{}, errors.New("cancelled integration environment is not empty")
		}
		if err := transaction.Commit(); err != nil {
			return AgentIntegration{}, err
		}
		current.DesiredState, current.ConfigState, current.Revision = 2, 7, current.Revision+1
		return current, nil
	}
	result, err := transaction.ExecContext(ctx, `UPDATE agent_integrations SET desired_state=2, config_state=7,
		config_fingerprint='', updated_at=?, revision=revision+1 WHERE id=? AND revision=? AND config_state=6`, time.Now().UnixNano(), id, expectedRevision)
	if err != nil {
		return AgentIntegration{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return AgentIntegration{}, errors.New("integration revision conflict")
	}
	if err := transaction.Commit(); err != nil {
		return AgentIntegration{}, err
	}
	return store.GetAgentIntegration(ctx, id)
}

func (store *Store) VerifyAgentIntegration(ctx context.Context, id string) error {
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `UPDATE agent_integrations SET config_state=2,
		last_verified_at=?, updated_at=?, revision=revision+1
		WHERE id=? AND desired_state=1 AND config_state=3`, now, now, id)
	return err
}

func (store *Store) ListSessions(ctx context.Context, integrationID string, includeTerminal bool) ([]Session, error) {
	query := `SELECT s.id, COALESCE(s.integration_id, ''), s.environment_id, e.namespace_id, s.socket_path,
		s.origin, s.created_at, s.expires_at, s.connected_at, s.last_activity_at, s.revoked_at, s.closed_at, s.terminal_reason
		FROM sessions s JOIN environments e ON e.id=s.environment_id WHERE 1=1`
	var arguments []any
	if integrationID != "" {
		query += " AND s.integration_id=?"
		arguments = append(arguments, integrationID)
	}
	if !includeTerminal {
		query += " AND s.revoked_at IS NULL AND s.closed_at IS NULL AND s.expires_at>?"
		arguments = append(arguments, time.Now().UnixNano())
	}
	query += " ORDER BY s.created_at DESC"
	rows, err := store.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Session
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, session)
	}
	return result, rows.Err()
}

func scanSession(row rowScanner) (Session, error) {
	var session Session
	var created, expires int64
	var connected, activity, revoked, closed sql.NullInt64
	err := row.Scan(&session.ID, &session.IntegrationID, &session.EnvironmentID, &session.NamespaceID, &session.SocketPath,
		&session.Origin, &created, &expires, &connected, &activity, &revoked, &closed, &session.TerminalReason)
	if err != nil {
		return Session{}, err
	}
	session.CreatedAt = time.Unix(0, created)
	session.ExpiresAt = time.Unix(0, expires)
	if connected.Valid {
		value := time.Unix(0, connected.Int64)
		session.ConnectedAt = &value
	}
	if activity.Valid {
		value := time.Unix(0, activity.Int64)
		session.LastActivity = &value
	}
	if revoked.Valid {
		value := time.Unix(0, revoked.Int64)
		session.RevokedAt = &value
	}
	if closed.Valid {
		value := time.Unix(0, closed.Int64)
		session.ClosedAt = &value
	}
	return session, nil
}

func (store *Store) MarkSessionConnected(ctx context.Context, id string) error {
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `UPDATE sessions SET connected_at=COALESCE(connected_at, ?), last_activity_at=?
		WHERE id=? AND revoked_at IS NULL AND closed_at IS NULL`, now, now, id)
	return err
}

func (store *Store) MarkSessionActivity(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE sessions SET last_activity_at=?
		WHERE id=? AND revoked_at IS NULL AND closed_at IS NULL`, time.Now().UnixNano(), id)
	return err
}
