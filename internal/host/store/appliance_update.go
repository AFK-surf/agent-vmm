package store

import (
	"context"
	"database/sql"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func (store *Store) ResumeApplianceUpdate(ctx context.Context, operation ApplianceOperation, expectedUpdated time.Time) (ApplianceOperation, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE appliance_operations SET state=1, deadline_unix_millis=?, error_reason='', error_message='', receipt=?, updated_at=? WHERE id=? AND updated_at=? AND state=3 AND kind=9 AND operation_phase!=5`, operation.DeadlineUnixMillis, applianceBlob(operation.Receipt), time.Now().UnixNano(), operation.ID, expectedUpdated.UnixNano())
	if err != nil {
		return ApplianceOperation{}, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	return store.GetApplianceOperation(ctx, operation.ID)
}

func (store *Store) CommitApplianceUpdateSwitch(ctx context.Context, operation ApplianceOperation, sourceArtifact string, installationRevision uint64, targetArtifact, bootstrapID string, grant ApplianceGrant, frontends []ApplianceFrontend) (ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceOperation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE appliance_installations SET artifact_id=?,bootstrap_id=COALESCE(NULLIF(?,''),bootstrap_id),desired_enabled=1,state=2,revision=revision+1,current_boot_id='',current_container_instance='',updated_at=? WHERE id=? AND revision=? AND artifact_id=? AND desired_enabled=0 AND state=4`, targetArtifact, bootstrapID, now, operation.InstallationID, installationRevision, sourceArtifact)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	if _, err := tx.ExecContext(ctx, `UPDATE appliance_grants SET revision=?,system_pids=?,system_disk_bytes=?,per_env_pids=?,per_env_disk_bytes=?,egress_mode=?,max_egress_mode=?,lan_egress=?,host_tcp_endpoints=?,updated_at=? WHERE installation_id=?`, grant.Revision, grant.System.PIDs, grant.System.DiskBytes, grant.PerEnvironment.PIDs, grant.PerEnvironment.DiskBytes, grant.EgressMode, grant.MaxEgressMode, grant.LANEgress, grant.HostTCPEndpoints, now, operation.InstallationID); err != nil {
		return ApplianceOperation{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM appliance_frontends WHERE installation_id=?`, operation.InstallationID); err != nil {
		return ApplianceOperation{}, err
	}
	for _, frontend := range frontends {
		if _, err := tx.ExecContext(ctx, `INSERT INTO appliance_frontends(installation_id,name,transport,exposure,max_frame_bytes,max_total_bytes,deadline_seconds,idle_seconds,max_concurrent_streams) VALUES(?,?,?,?,?,?,?,?,?)`, operation.InstallationID, frontend.Name, frontend.Transport, frontend.Exposure, frontend.MaxFrameBytes, frontend.MaxTotalBytes, frontend.DeadlineSeconds, frontend.IdleSeconds, frontend.MaxConcurrentStreams); err != nil {
			return ApplianceOperation{}, err
		}
	}
	if err := revokeMissingApplianceFrontends(ctx, tx, operation.InstallationID, now); err != nil {
		return ApplianceOperation{}, err
	}
	result, err = tx.ExecContext(ctx, `UPDATE remote_registrations SET per_env_pids=?,per_env_disk_bytes=?,max_egress_mode=?,revision=revision+1,pool_policy_revision=pool_policy_revision+1,updated_at=? WHERE installation_id=? AND owner_kind=2 AND state=2`, grant.PerEnvironment.PIDs, grant.PerEnvironment.DiskBytes, grant.MaxEgressMode, now, operation.InstallationID)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ApplianceOperation{}, ErrRemoteRegistrationRevisionConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE appliance_operations SET operation_phase=10,stage='activating',receipt=?,updated_at=? WHERE id=? AND state=1 AND operation_phase=9`, applianceBlob(operation.Receipt), now, operation.ID)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, operation.ID)
}

func (store *Store) HasPendingApplianceUpdate(ctx context.Context, installationID string) (bool, error) {
	var found bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM appliance_operations WHERE installation_id=? AND kind=9 AND state IN(1,3) AND operation_phase!=5)`, installationID).Scan(&found)
	return found, err
}

func (store *Store) HasRunningApplianceUpdate(ctx context.Context, installationID string) (bool, error) {
	var found bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM appliance_operations WHERE installation_id=? AND kind=9 AND state=1 AND operation_phase!=5)`, installationID).Scan(&found)
	return found, err
}

func interruptApplianceUpdate(ctx context.Context, tx *sql.Tx, installationID string, kind int32, now int64) error {
	var running, failed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM appliance_operations WHERE installation_id=? AND kind=9 AND state=1 AND operation_phase!=5), EXISTS(SELECT 1 FROM appliance_operations WHERE installation_id=? AND kind=9 AND state=3 AND operation_phase!=5)`, installationID, installationID).Scan(&running, &failed); err != nil {
		return err
	}
	if !running && !failed {
		return nil
	}
	var reason appliancev1.ApplianceTerminalReason
	switch appliancev1.ApplianceOperationKind(kind) {
	case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP:
		reason = appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_OPERATOR_STOPPED
	case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE:
		reason = appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_OPERATOR_REVOKED
	case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE:
		reason = appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_REMOVED
	default:
		return ErrApplianceOperationInProgress
	}
	_, err := tx.ExecContext(ctx, `UPDATE appliance_operations SET state=4,terminal_reason=?,updated_at=? WHERE installation_id=? AND kind=9 AND state IN(1,3) AND operation_phase!=5`, int32(reason), now, installationID)
	return err
}

func (store *Store) ApplianceImageRetentionFacts(ctx context.Context, installationID string) (ApplianceInstallation, []ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ApplianceInstallation{}, nil, err
	}
	defer tx.Rollback()
	installation, err := scanApplianceInstallation(tx.QueryRowContext(ctx, applianceInstallationSelect+" WHERE id=?", installationID))
	if err != nil {
		return ApplianceInstallation{}, nil, err
	}
	rows, err := tx.QueryContext(ctx, applianceOperationSelect+" WHERE installation_id=? AND kind=9 AND state IN(1,3) AND operation_phase!=5", installationID)
	if err != nil {
		return ApplianceInstallation{}, nil, err
	}
	defer rows.Close()
	var operations []ApplianceOperation
	for rows.Next() {
		operation, err := scanApplianceOperation(rows)
		if err != nil {
			return ApplianceInstallation{}, nil, err
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		return ApplianceInstallation{}, nil, err
	}
	return installation, operations, nil
}

func (store *Store) CommitApplianceUpdatePreparation(ctx context.Context, operation ApplianceOperation, sourceArtifact string, installationRevision uint64) (ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceOperation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE appliance_operations SET operation_phase=?,stage='draining',error_reason='',error_message='',receipt=?,updated_at=? WHERE id=? AND state=? AND operation_phase=?`, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_DRAINING, applianceBlob(operation.Receipt), now, operation.ID, appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrOperationReceiptConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE appliance_installations SET desired_enabled=0,state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=? AND artifact_id=?`, appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING, now, operation.InstallationID, installationRevision, sourceArtifact)
	if err != nil {
		return ApplianceOperation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceOperation{}, err
		}
		return ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceOperation{}, err
	}
	return store.GetApplianceOperation(ctx, operation.ID)
}
