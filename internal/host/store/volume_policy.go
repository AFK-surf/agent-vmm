package store

import (
	"context"
	"database/sql"
)

func (store *Store) PutVolumeOwner(ctx context.Context, volumeID, namespaceID string) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO volume_policies(volume_id, owner_namespace_id) VALUES (?, ?)
		ON CONFLICT(owner_namespace_id, volume_id) DO UPDATE SET revision=volume_policies.revision+1`, volumeID, namespaceID)
	return err
}

func (store *Store) DeleteVolumePolicy(ctx context.Context, volumeID, namespaceID string) error {
	result, err := store.db.ExecContext(ctx, "DELETE FROM volume_policies WHERE volume_id=? AND owner_namespace_id=?", volumeID, namespaceID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return sql.ErrNoRows
	}
	return err
}

func (store *Store) HasVolumePolicy(ctx context.Context, volumeID, namespaceID string) (bool, error) {
	var exists bool
	err := store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM volume_policies WHERE volume_id=? AND owner_namespace_id=?)", volumeID, namespaceID).Scan(&exists)
	return exists, err
}
