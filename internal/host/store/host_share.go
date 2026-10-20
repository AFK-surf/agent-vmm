package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type HostShareSource struct {
	Path           string
	FileSystemUUID string
	FileID         uint64
}

type HostShare struct {
	ID                string
	EnvironmentID     string
	Source            HostShareSource
	ReadOnly          bool
	Revision          uint64
	State             hostv1.HostShareState
	UnavailableReason string
	OperationID       string
}

const hostShareColumns = `id,environment_id,source_path,filesystem_uuid,file_id,read_only,revision,state,unavailable_reason,operation_id`

func scanHostShare(row rowScanner) (HostShare, error) {
	var value HostShare
	var fileID string
	err := row.Scan(&value.ID, &value.EnvironmentID, &value.Source.Path, &value.Source.FileSystemUUID, &fileID, &value.ReadOnly, &value.Revision, &value.State, &value.UnavailableReason, &value.OperationID)
	if err != nil {
		return value, err
	}
	value.Source.FileID, err = strconv.ParseUint(fileID, 10, 64)
	return value, err
}

func (store *Store) GetHostShare(ctx context.Context, id string) (HostShare, error) {
	return scanHostShare(store.db.QueryRowContext(ctx, `SELECT `+hostShareColumns+` FROM host_shares WHERE id=?`, id))
}

func (store *Store) ListHostShares(ctx context.Context, environment string) ([]HostShare, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT `+hostShareColumns+` FROM host_shares WHERE (?='' OR environment_id=?) ORDER BY id`, environment, environment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []HostShare
	for rows.Next() {
		value, err := scanHostShare(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

const hostShareEnvironmentApprovedQuery = `SELECT EXISTS(SELECT 1 FROM environments e JOIN agent_integrations i ON i.environment_id=e.id WHERE e.id=? AND e.owner_kind=1 AND e.registration_id IS NULL AND i.desired_state=1 AND i.config_state NOT IN(6,7))`

func (store *Store) HostShareEnvironmentApproved(ctx context.Context, environment string) (bool, error) {
	var approved bool
	err := store.db.QueryRowContext(ctx, hostShareEnvironmentApprovedQuery, environment).Scan(&approved)
	return approved, err
}

func (store *Store) UpdateHostShare(ctx context.Context, value HostShare, expectedRevision uint64) error {
	result, err := store.db.ExecContext(ctx, `UPDATE host_shares SET source_path=?,filesystem_uuid=?,file_id=?,read_only=?,revision=?,state=?,unavailable_reason=?,operation_id=? WHERE id=? AND revision=?`, value.Source.Path, value.Source.FileSystemUUID, strconv.FormatUint(value.Source.FileID, 10), value.ReadOnly, value.Revision, value.State, value.UnavailableReason, value.OperationID, value.ID, expectedRevision)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count == 0 {
		return fmt.Errorf("host share revision changed: %w", sql.ErrNoRows)
	}
	return err
}
