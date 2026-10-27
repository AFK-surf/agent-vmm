package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Session struct {
	ID             string
	IntegrationID  string
	EnvironmentID  string
	NamespaceID    string
	SocketPath     string
	Origin         int32
	CreatedAt      time.Time
	ExpiresAt      time.Time
	ConnectedAt    *time.Time
	LastActivity   *time.Time
	RevokedAt      *time.Time
	ClosedAt       *time.Time
	TerminalReason string
}

func (store *Store) CreateSession(ctx context.Context, session Session) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO sessions(id, integration_id, environment_id, socket_path, origin, created_at, expires_at)
		VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?)`, session.ID, session.IntegrationID, session.EnvironmentID, session.SocketPath,
		session.Origin, session.CreatedAt.UnixNano(), session.ExpiresAt.UnixNano())
	return err
}

func (store *Store) CreateAuthorizedSession(ctx context.Context, session Session, integrationRevision uint64) error {
	result, err := store.db.ExecContext(ctx, `INSERT INTO sessions(id, integration_id, environment_id, socket_path, origin, created_at, expires_at)
		SELECT ?, id, environment_id, ?, ?, ?, ? FROM agent_integrations
		WHERE id=? AND environment_id=? AND revision=? AND desired_state=1 AND config_state IN (2,3)`,
		session.ID, session.SocketPath, session.Origin, session.CreatedAt.UnixNano(), session.ExpiresAt.UnixNano(),
		session.IntegrationID, session.EnvironmentID, integrationRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("integration is not authorized")
	}
	return nil
}

func (store *Store) GetSession(ctx context.Context, id string) (Session, error) {
	var session Session
	var expires int64
	var revoked sql.NullInt64
	var created int64
	var connected, activity, closed sql.NullInt64
	err := store.db.QueryRowContext(ctx, `SELECT s.id, COALESCE(s.integration_id, ''), s.environment_id, e.namespace_id, s.socket_path,
		s.origin, s.created_at, s.expires_at, s.connected_at, s.last_activity_at, s.revoked_at, s.closed_at, s.terminal_reason
		FROM sessions s JOIN environments e ON e.id=s.environment_id WHERE s.id=?`, id).
		Scan(&session.ID, &session.IntegrationID, &session.EnvironmentID, &session.NamespaceID, &session.SocketPath,
			&session.Origin, &created, &expires, &connected, &activity, &revoked, &closed, &session.TerminalReason)
	if err != nil {
		return Session{}, err
	}
	session.ExpiresAt = time.Unix(0, expires)
	session.CreatedAt = time.Unix(0, created)
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

func (store *Store) RevokeSession(ctx context.Context, id string) error {
	return store.EndSession(ctx, id, "REVOKED", false)
}

func (store *Store) EndSession(ctx context.Context, id, reason string, closed bool) error {
	now := time.Now().UnixNano()
	result, err := store.db.ExecContext(ctx, `UPDATE sessions SET revoked_at=CASE WHEN ? THEN revoked_at ELSE ? END,
		closed_at=CASE WHEN ? THEN ? ELSE closed_at END, terminal_reason=?
		WHERE id=? AND revoked_at IS NULL AND closed_at IS NULL`, closed, now, closed, now, reason, id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		var terminal bool
		err = store.db.QueryRowContext(ctx, "SELECT revoked_at IS NOT NULL OR closed_at IS NOT NULL FROM sessions WHERE id=?", id).Scan(&terminal)
		if err != nil {
			return err
		}
		if !terminal {
			return sql.ErrNoRows
		}
	}
	return err
}
