package store

import (
	"context"
	"database/sql"
	"time"
)

func (store *Store) ApplianceContentRetentionFacts(ctx context.Context) ([]ApplianceArtifact, []ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, applianceArtifactSelect+` WHERE artifact_id IN (SELECT artifact_id FROM appliance_installations)`)
	if err != nil {
		return nil, nil, err
	}
	var artifacts []ApplianceArtifact
	for rows.Next() {
		artifact, err := scanApplianceArtifact(rows)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	rows, err = tx.QueryContext(ctx, applianceOperationSelect+` WHERE (kind=9 AND state IN (1,3) AND operation_phase!=5) OR (kind IN (1,10) AND deadline_unix_millis>?)`, time.Now().UnixMilli())
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var operations []ApplianceOperation
	for rows.Next() {
		operation, err := scanApplianceOperation(rows)
		if err != nil {
			return nil, nil, err
		}
		operations = append(operations, operation)
	}
	return artifacts, operations, rows.Err()
}
