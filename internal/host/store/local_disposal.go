package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

var ErrLocalDisposed = errors.New("local target was disposed by its operator")

type LocalDisposal struct {
	OperationID          string
	Kind                 string
	TargetID             string
	ParentID             string
	RegistrationID       string
	EnrollmentRequestID  string
	NamespaceID          string
	Revision             uint64
	Completed            bool
	AllocationID         string
	AllocationGeneration uint64
}

func migrateV24(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE local_disposals (
   target_kind TEXT NOT NULL CHECK(target_kind IN ('registration','environment')),
   target_id TEXT NOT NULL, operation_id TEXT NOT NULL REFERENCES operations(id),
   parent_id TEXT NOT NULL DEFAULT '', registration_id TEXT NOT NULL DEFAULT '',
   enrollment_request_id TEXT NOT NULL DEFAULT '', namespace_id TEXT NOT NULL DEFAULT '',
   target_revision INTEGER NOT NULL, completed INTEGER NOT NULL DEFAULT 0,
   PRIMARY KEY(target_kind,target_id))`,
		`CREATE INDEX disposal_sessions_pending ON sessions(environment_id,id) WHERE closed_at IS NULL`,
		`CREATE INDEX local_disposals_pending ON local_disposals(completed,parent_id,target_id)`,
		`CREATE INDEX local_disposals_namespace ON local_disposals(namespace_id) WHERE namespace_id != ''`,
		`CREATE INDEX local_disposals_enrollment ON local_disposals(enrollment_request_id) WHERE enrollment_request_id != ''`,
		`CREATE TRIGGER disposed_registration_insert BEFORE INSERT ON remote_registrations WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE target_kind='registration' AND
   (target_id=NEW.id OR (enrollment_request_id != '' AND enrollment_request_id=NEW.enrollment_request_id)))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_registration_update BEFORE UPDATE ON remote_registrations WHEN
   (NEW.state != 3 OR NEW.enrollment_state != 4 OR NEW.connection_instance != '') AND EXISTS (
   SELECT 1 FROM local_disposals WHERE target_kind='registration' AND target_id=NEW.id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_environment_insert BEFORE INSERT ON environments WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE (target_kind='environment' AND (target_id=NEW.id OR namespace_id=NEW.namespace_id))
    OR (target_kind='registration' AND target_id=NEW.registration_id))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_environment_update BEFORE UPDATE ON environments WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE (target_kind='environment' AND target_id=NEW.id)
    OR (target_kind='registration' AND target_id=NEW.registration_id))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_session_insert BEFORE INSERT ON sessions WHEN EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_integration_update BEFORE UPDATE ON agent_integrations WHEN NEW.desired_state=1 AND EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
		`CREATE TRIGGER disposed_integration_insert BEFORE INSERT ON agent_integrations WHEN EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
	}
	// Capabilities cannot be recreated by calls accepted before the disposal fence.
	for _, event := range []string{"INSERT", "UPDATE"} {
		statements = append(statements, `CREATE TRIGGER disposed_service_exports_`+event+` BEFORE `+event+` ON service_exports WHEN NEW.revoked=0 AND EXISTS (
 SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.environment_id) BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`)
		statements = append(statements, `CREATE TRIGGER disposed_service_imports_`+event+` BEFORE `+event+` ON service_imports WHEN NEW.revoked=0 AND (
 EXISTS (SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.source_environment_id) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`)
		statements = append(statements, `CREATE TRIGGER disposed_access_routes_`+event+` BEFORE `+event+` ON access_routes WHEN NEW.state NOT IN (4,5) AND (
 EXISTS (SELECT 1 FROM service_imports WHERE import_id=NEW.import_id AND revoked=1) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`)
	}
	statements = append(statements, `CREATE TRIGGER disposed_grant_insert BEFORE INSERT ON grants WHEN EXISTS (
 SELECT 1 FROM local_disposals WHERE namespace_id != '' AND (namespace_id=NEW.source_namespace_id OR namespace_id=NEW.target_namespace_id)
 UNION ALL SELECT 1 FROM environments e JOIN local_disposals d ON d.target_kind='registration' AND d.target_id=e.registration_id
 WHERE e.namespace_id=NEW.source_namespace_id OR e.namespace_id=NEW.target_namespace_id)
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`)
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func migrateV25(tx *sql.Tx) error {
	for _, statement := range []string{
		`ALTER TABLE local_disposals ADD COLUMN allocation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE local_disposals ADD COLUMN allocation_generation INTEGER NOT NULL DEFAULT 0`,
		`CREATE UNIQUE INDEX allocation_disposal_scope ON local_disposals(registration_id,allocation_id,allocation_generation) WHERE allocation_id != ''`,
		`CREATE TRIGGER disposed_allocation_insert BEFORE INSERT ON environments WHEN EXISTS (
          SELECT 1 FROM local_disposals WHERE allocation_id != '' AND registration_id=NEW.registration_id
            AND allocation_id=NEW.allocation_id AND allocation_generation=NEW.allocation_generation)
          BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

const disposalSelect = `SELECT operation_id,target_kind,target_id,parent_id,registration_id,enrollment_request_id,namespace_id,target_revision,completed,allocation_id,allocation_generation FROM local_disposals`

func scanDisposal(row rowScanner) (LocalDisposal, error) {
	var result LocalDisposal
	err := row.Scan(&result.OperationID, &result.Kind, &result.TargetID, &result.ParentID, &result.RegistrationID, &result.EnrollmentRequestID, &result.NamespaceID, &result.Revision, &result.Completed, &result.AllocationID, &result.AllocationGeneration)
	return result, err
}

func (store *Store) GetLocalDisposal(ctx context.Context, kind, id string) (LocalDisposal, error) {
	return scanDisposal(store.db.QueryRowContext(ctx, disposalSelect+` WHERE target_kind=? AND target_id=?`, kind, id))
}

func (store *Store) EnvironmentDisposed(ctx context.Context, environment Environment) (bool, error) {
	var disposed bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM local_disposals WHERE
  (target_kind='environment' AND (target_id=? OR namespace_id=?)) OR (target_kind='registration' AND target_id=?))`, environment.ID, environment.NamespaceID, environment.RegistrationID).Scan(&disposed)
	return disposed, err
}

// AcceptLocalDisposal commits the receipt and replay fence before any Guest work.
// The caller holds inventory transition, remote admission, and the exact environment lock.
func (store *Store) AcceptLocalDisposal(ctx context.Context, operation Operation, requested LocalDisposal) (Operation, error) {
	return store.acceptDisposal(ctx, operation, requested, false)
}

// AcceptAllocationDisposal uses cloud authority for one generation, never a registration-wide scope.
func (store *Store) AcceptAllocationDisposal(ctx context.Context, operation Operation, requested LocalDisposal) (Operation, error) {
	if requested.Kind != "environment" || requested.RegistrationID == "" || requested.AllocationID == "" || requested.AllocationGeneration == 0 {
		return Operation{}, errors.New("invalid allocation disposal scope")
	}
	return store.acceptDisposal(ctx, operation, requested, true)
}

func (store *Store) AllocationDisposal(ctx context.Context, registration, allocation string, generation uint64) (LocalDisposal, error) {
	return scanDisposal(store.db.QueryRowContext(ctx, disposalSelect+` WHERE registration_id=? AND allocation_id=? AND allocation_generation=?`, registration, allocation, generation))
}

func (store *Store) acceptDisposal(ctx context.Context, operation Operation, requested LocalDisposal, remote bool) (Operation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback()
	prior, err := scanOperation(tx.QueryRowContext(ctx, `SELECT id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at FROM operations WHERE request_id=?`, operation.RequestID))
	if err == nil {
		target, loadErr := scanDisposal(tx.QueryRowContext(ctx, disposalSelect+` WHERE operation_id=? AND parent_id=''`, prior.ID))
		if loadErr != nil {
			return Operation{}, loadErr
		}
		if target.Kind != requested.Kind || target.TargetID != requested.TargetID || target.Revision != requested.Revision || target.RegistrationID != requested.RegistrationID && remote || target.AllocationID != requested.AllocationID || target.AllocationGeneration != requested.AllocationGeneration {
			return Operation{}, errors.New("disposal request identity conflict")
		}
		return prior, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	if requested.Kind == "registration" {
		var revision uint64
		var enrollment string
		if err := tx.QueryRowContext(ctx, `SELECT revision,enrollment_request_id FROM remote_registrations WHERE id=? AND owner_kind=?`, requested.TargetID, RemoteRegistrationExternal).Scan(&revision, &enrollment); err != nil {
			return Operation{}, err
		}
		if revision != requested.Revision {
			return Operation{}, errors.New("disposal target revision changed")
		}
		requested.RegistrationID, requested.EnrollmentRequestID = requested.TargetID, enrollment
	} else if requested.Kind == "environment" {
		var revision uint64
		var namespace string
		if remote {
			var registration, allocation string
			var generation uint64
			if err := tx.QueryRowContext(ctx, `SELECT policy_revision,namespace_id,registration_id,allocation_id,allocation_generation FROM environments WHERE id=? AND owner_kind=?`, requested.TargetID, OwnerRemoteRegistration).Scan(&revision, &namespace, &registration, &allocation, &generation); err != nil {
				return Operation{}, err
			}
			if registration != requested.RegistrationID || allocation != requested.AllocationID || generation != requested.AllocationGeneration {
				return Operation{}, errors.New("allocation disposal fence changed")
			}
		} else if err := tx.QueryRowContext(ctx, `SELECT policy_revision,namespace_id FROM environments WHERE id=? AND owner_kind=?`, requested.TargetID, OwnerLocal).Scan(&revision, &namespace); err != nil {
			return Operation{}, err
		}
		if revision != requested.Revision || namespace != requested.NamespaceID {
			return Operation{}, errors.New("disposal target revision changed")
		}
	} else {
		return Operation{}, errors.New("invalid local disposal scope")
	}
	now := time.Now()
	operation.State, operation.Stage = int32(hostv1.OperationState_OPERATION_STATE_RUNNING), "isolated"
	operation.StartedAt, operation.UpdatedAt = now, now
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations(id,kind,environment_id,state,stage,request_id,error_reason,error_message,output_path,started_at,updated_at) VALUES(?,?,?,?,?,?, '', '', '',?,?)`, operation.ID, "local-disposal", requested.TargetID, operation.State, operation.Stage, operation.RequestID, now.UnixNano(), now.UnixNano()); err != nil {
		return Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO local_disposals(target_kind,target_id,operation_id,registration_id,enrollment_request_id,namespace_id,target_revision,allocation_id,allocation_generation) VALUES(?,?,?,?,?,?,?,?,?)`, requested.Kind, requested.TargetID, operation.ID, requested.RegistrationID, requested.EnrollmentRequestID, requested.NamespaceID, requested.Revision, requested.AllocationID, requested.AllocationGeneration); err != nil {
		return Operation{}, err
	}
	if requested.Kind == "registration" {
		if _, err := tx.ExecContext(ctx, `UPDATE remote_registrations SET state=3,enrollment_state=4,connection_state=1,connection_instance='',revision=revision+1,updated_at=? WHERE id=?`, now.UnixNano(), requested.TargetID); err != nil {
			return Operation{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=?,terminal_reason='LOCAL_DISPOSED' WHERE environment_id IN (SELECT id FROM environments WHERE (?='environment' AND id=?) OR (?='registration' AND registration_id=?)) AND revoked_at IS NULL`, now.UnixNano(), requested.Kind, requested.TargetID, requested.Kind, requested.TargetID); err != nil {
		return Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_integrations SET desired_state=2,updated_at=?,revision=revision+1 WHERE environment_id IN (SELECT id FROM environments WHERE (?='environment' AND id=?) OR (?='registration' AND registration_id=?))`, now.UnixNano(), requested.Kind, requested.TargetID, requested.Kind, requested.TargetID); err != nil {
		return Operation{}, err
	}
	scope := `SELECT id FROM environments WHERE (?='environment' AND id=?) OR (?='registration' AND registration_id=?)`
	args := []any{requested.Kind, requested.TargetID, requested.Kind, requested.TargetID}
	if _, err := tx.ExecContext(ctx, `UPDATE grants SET revoked_at=? WHERE revoked_at IS NULL AND
 (source_namespace_id IN (SELECT namespace_id FROM environments WHERE id IN (`+scope+`)) OR target_namespace_id IN (SELECT namespace_id FROM environments WHERE id IN (`+scope+`)))`, append(append([]any{now.UnixNano()}, args...), args...)...); err != nil {
		return Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_exports SET revoked=1,revision=revision+1,updated_at=? WHERE revoked=0 AND environment_id IN (`+scope+`)`, append([]any{now.UnixNano()}, args...)...); err != nil {
		return Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1,revision=revision+1,updated_at=? WHERE revoked=0 AND
 (source_environment_id IN (`+scope+`) OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id IN (`+scope+`)))`, append(append([]any{now.UnixNano()}, args...), args...)...); err != nil {
		return Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=4,revision=revision+1,generation=generation+1,updated_at=? WHERE state NOT IN (4,5) AND import_id IN (SELECT import_id FROM service_imports WHERE source_environment_id IN (`+scope+`) OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id IN (`+scope+`)))`, append(append([]any{now.UnixNano()}, args...), args...)...); err != nil {
		return Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, err
	}
	operation.Kind, operation.EnvironmentID = "local-disposal", requested.TargetID
	return operation, nil
}

func (store *Store) PendingLocalDisposals(ctx context.Context, after string, limit int) ([]LocalDisposal, error) {
	if limit < 1 || limit > 32 {
		return nil, errors.New("invalid disposal page size")
	}
	rows, err := store.db.QueryContext(ctx, disposalSelect+` WHERE completed=0 AND parent_id='' AND target_id>? ORDER BY target_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LocalDisposal
	for rows.Next() {
		value, err := scanDisposal(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// The Environment mapping is durable before Guest deletion and survives its resource row.
func (store *Store) CaptureDisposalEnvironment(ctx context.Context, root LocalDisposal, environment Environment) (LocalDisposal, error) {
	if root.Kind == "environment" {
		if root.TargetID != environment.ID || root.NamespaceID != environment.NamespaceID {
			return LocalDisposal{}, errors.New("disposal scope mismatch")
		}
		return root, nil
	}
	if root.Kind != "registration" || root.TargetID != environment.RegistrationID {
		return LocalDisposal{}, errors.New("disposal scope mismatch")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO local_disposals(target_kind,target_id,parent_id,operation_id,registration_id,namespace_id,target_revision,allocation_id,allocation_generation)
 SELECT 'environment',?,target_id,operation_id,registration_id,?,?,?,? FROM local_disposals WHERE target_kind='registration' AND target_id=? AND operation_id=?
 ON CONFLICT(target_kind,target_id) DO NOTHING`, environment.ID, environment.NamespaceID, environment.PolicyRevision, environment.AllocationID, environment.AllocationGeneration, root.TargetID, root.OperationID)
	if err != nil {
		return LocalDisposal{}, err
	}
	child, err := store.GetLocalDisposal(ctx, "environment", environment.ID)
	if err == nil && child.OperationID != root.OperationID {
		return LocalDisposal{}, errors.New("disposal target belongs to another operation")
	}
	return child, err
}

func (store *Store) FinishDisposalEnvironment(ctx context.Context, child LocalDisposal) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stored, err := scanDisposal(tx.QueryRowContext(ctx, disposalSelect+` WHERE target_kind='environment' AND target_id=? AND operation_id=?`, child.TargetID, child.OperationID))
	if err != nil {
		return err
	}
	if stored.NamespaceID != child.NamespaceID {
		return errors.New("disposal namespace changed")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE environment_id=?`, child.TargetID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_integrations WHERE environment_id=?`, child.TargetID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM grants WHERE source_namespace_id=? OR target_namespace_id=?`, child.NamespaceID, child.NamespaceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM environments WHERE id=? AND namespace_id=?`, child.TargetID, child.NamespaceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE local_disposals SET completed=CASE WHEN parent_id='' THEN 0 ELSE 1 END WHERE target_kind='environment' AND target_id=? AND operation_id=?`, child.TargetID, child.OperationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) FinishLocalDisposal(ctx context.Context, root LocalDisposal) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var remaining bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM environments WHERE (?='environment' AND id=?) OR (?='registration' AND registration_id=?)) OR EXISTS(SELECT 1 FROM local_disposals WHERE operation_id=? AND parent_id != '' AND completed=0)`, root.Kind, root.TargetID, root.Kind, root.TargetID, root.OperationID).Scan(&remaining); err != nil {
		return err
	}
	if remaining {
		return errors.New("local disposal still has pending environments")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE local_disposals SET completed=1 WHERE target_kind=? AND target_id=? AND operation_id=?`, root.Kind, root.TargetID, root.OperationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,stage='complete',error_reason='',error_message='',updated_at=? WHERE id=?`, hostv1.OperationState_OPERATION_STATE_SUCCEEDED, time.Now().UnixNano(), root.OperationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) RemoteEnvironmentCount(ctx context.Context, registrationID string) (uint64, error) {
	var count uint64
	err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM environments WHERE registration_id=?`, registrationID).Scan(&count)
	return count, err
}

// DisposalSessionPage reads only the exact target's open listeners; acceptance has already revoked authority.
func (store *Store) DisposalSessionPage(ctx context.Context, environmentID string) ([]string, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id FROM sessions WHERE environment_id=? AND closed_at IS NULL ORDER BY id LIMIT 32`, environmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 32)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (store *Store) CloseDisposedSession(ctx context.Context, environmentID, id string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE sessions SET closed_at=?, terminal_reason='LOCAL_DISPOSED' WHERE id=? AND environment_id=? AND revoked_at IS NOT NULL AND closed_at IS NULL`, time.Now().UnixNano(), id, environmentID)
	return err
}

func (store *Store) RegistrationDisposed(ctx context.Context, id, enrollmentRequestID string) (bool, error) {
	var disposed bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM local_disposals WHERE target_kind='registration' AND (target_id=? OR (enrollment_request_id != '' AND enrollment_request_id=?)))`, id, enrollmentRequestID).Scan(&disposed)
	return disposed, err
}

func (store *Store) DisposalServicePage(ctx context.Context, environmentID string) ([]string, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT import_id FROM service_imports WHERE source_environment_id=? OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id=?) ORDER BY import_id LIMIT 32`, environmentID, environmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 32)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (store *Store) FinishDisposalService(ctx context.Context, environmentID, id string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM service_imports WHERE import_id=? AND revoked=1 AND (source_environment_id=? OR destination_export_id IN (SELECT export_id FROM service_exports WHERE environment_id=?))`, id, environmentID, environmentID)
	return err
}
