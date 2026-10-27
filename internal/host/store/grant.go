package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Grant struct {
	ID                string
	Kind              string
	SourceNamespaceID string
	TargetNamespaceID string
	Payload           string
	Revoked           bool
}

func (store *Store) PutGrant(ctx context.Context, grant Grant) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO grants(id, kind, source_namespace_id, target_namespace_id, methods)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,
		source_namespace_id=excluded.source_namespace_id, target_namespace_id=excluded.target_namespace_id,
		methods=excluded.methods, revoked_at=NULL, revision=grants.revision+1`,
		grant.ID, grant.Kind, grant.SourceNamespaceID, grant.TargetNamespaceID, grant.Payload)
	return err
}

func (store *Store) ActiveGrants(ctx context.Context) ([]Grant, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods
		FROM grants WHERE revoked_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []Grant
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// ActiveGrantsForNamespace returns at most limit+1 grants so callers can reject
// an inventory that exceeds their admitted work bound without scanning every
// grant in the Host database.
func (store *Store) ActiveGrantsForNamespace(ctx context.Context, namespaceID string, limit int) ([]Grant, error) {
	if limit < 0 {
		return nil, errors.New("grant result limit must not be negative")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods
		FROM grants WHERE revoked_at IS NULL AND (source_namespace_id=? OR target_namespace_id=?)
		ORDER BY id LIMIT ?`, namespaceID, namespaceID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants := make([]Grant, 0, min(limit+1, 16))
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// GrantsForNamespace returns active grants and tombstones within the same
// bounded owner scope. Cleanup uses tombstones to remove Guest policy that may
// remain after a Host-side revocation was persisted.
func (store *Store) GrantsForNamespace(ctx context.Context, namespaceID string, limit int) ([]Grant, error) {
	if limit < 0 {
		return nil, errors.New("grant result limit must not be negative")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants WHERE source_namespace_id=? OR target_namespace_id=?
		ORDER BY id LIMIT ?`, namespaceID, namespaceID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants := make([]Grant, 0, min(limit+1, 16))
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func (store *Store) AllGrants(ctx context.Context) ([]Grant, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []Grant
	for rows.Next() {
		var grant Grant
		if err := rows.Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func (store *Store) GetGrant(ctx context.Context, id string) (Grant, error) {
	var grant Grant
	err := store.db.QueryRowContext(ctx, `SELECT id, kind, source_namespace_id, target_namespace_id, methods, revoked_at IS NOT NULL
		FROM grants WHERE id=?`, id).Scan(&grant.ID, &grant.Kind, &grant.SourceNamespaceID, &grant.TargetNamespaceID, &grant.Payload, &grant.Revoked)
	return grant, err
}

func (store *Store) RevokeGrant(ctx context.Context, id string) error {
	result, err := store.db.ExecContext(ctx, `UPDATE grants SET revoked_at=?, revision=revision+1 WHERE id=? AND revoked_at IS NULL`, time.Now().UnixNano(), id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) RevokeGrants(ctx context.Context, namespaceID string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE grants SET revoked_at=?, revision=revision+1 WHERE revoked_at IS NULL AND
		(source_namespace_id=? OR target_namespace_id=?)`, time.Now().UnixNano(), namespaceID, namespaceID)
	return err
}

// CompactRevokedGrant removes a reconciliation tombstone only after the Guest
// has confirmed the corresponding desired-state deletion.
func (store *Store) CompactRevokedGrant(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM grants WHERE id=? AND revoked_at IS NOT NULL`, id)
	return err
}
