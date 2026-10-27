package store

import (
	"context"
	"time"
)

func (store *Store) Audit(ctx context.Context, actor, target, method, grantID, requestID string, resultCode int32, reason string) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO audit(actor, target, method, grant_id, request_id, result_code, reason, occurred_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, actor, target, method, grantID, requestID, resultCode, reason, time.Now().UnixNano())
	if err != nil {
		return err
	}
	return store.maintainStorage(ctx, time.Now())
}

func (store *Store) pruneAudit(ctx context.Context, now time.Time) error {
	if _, err := store.db.ExecContext(ctx, `DELETE FROM audit WHERE sequence IN (
		SELECT sequence FROM audit WHERE occurred_at < ? ORDER BY occurred_at,sequence LIMIT ?
	)`, now.Add(-auditRetention).UnixNano(), auditPruneBatch); err != nil {
		return err
	}
	var excess int
	if err := store.db.QueryRowContext(ctx, `SELECT MAX(COUNT(*)-?,0) FROM audit`, auditMaxRows).Scan(&excess); err != nil {
		return err
	}
	if excess == 0 {
		return nil
	}
	_, err := store.db.ExecContext(ctx, `DELETE FROM audit WHERE sequence IN (
		SELECT sequence FROM audit ORDER BY sequence LIMIT ?
	)`, min(excess, auditPruneBatch))
	return err
}

func (store *Store) maintainStorage(ctx context.Context, now time.Time) error {
	if err := store.pruneAudit(ctx, now); err != nil {
		return err
	}
	var busy, logFrames, checkpointed int
	if err := store.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 || logFrames != checkpointed || logFrames == 0 {
		return nil
	}
	if _, err := store.db.ExecContext(ctx, `PRAGMA busy_timeout=0`); err != nil {
		return err
	}
	defer store.db.ExecContext(context.Background(), `PRAGMA busy_timeout=5000`)
	return store.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed)
}
