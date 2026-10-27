package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

func (store *Store) CreatePersonalMesh(ctx context.Context, descriptor *trustv1.PersonalMeshDescriptor, snapshot *trustv1.MeshSnapshot, operation *trustv1.RegistryOperation) error {
	descriptorPayload, err := marshalTrust(descriptor)
	if err != nil {
		return err
	}
	snapshotPayload, err := marshalTrust(snapshot)
	if err != nil {
		return err
	}
	operationPayload, err := marshalTrust(operation)
	if err != nil {
		return err
	}
	if len(operationPayload) > maxRegistryOperationBytes {
		return errors.New("registry operation exceeds size limit")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO mesh_descriptors(mesh_id, owner_kind, descriptor, created_at) VALUES(?, ?, ?, ?)`,
		descriptor.GetMeshId(), TrustOwnerPersonalMesh, descriptorPayload, time.Now().UnixNano()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mesh_snapshots(mesh_id, owner_kind, revision, policy_epoch, expires_at, snapshot, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, snapshot.GetMeshId(), TrustOwnerPersonalMesh, snapshot.GetRevision(), snapshot.GetPolicyEpoch(),
		snapshot.GetExpiresAt().AsTime().UnixNano(), snapshotPayload, time.Now().UnixNano()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO registry_operations(operation_id, mesh_id, expected_revision, kind,
		state, operation, created_at) VALUES(?, ?, ?, ?, 1, ?, ?)`, operation.GetOperationId(), operation.GetMeshId(),
		operation.GetExpectedRevision(), operation.GetKind(), operationPayload, time.Now().UnixNano()); err != nil {
		return err
	}
	if err := replaceTombstones(ctx, tx, snapshot); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) GetPersonalMesh(ctx context.Context, meshID string) (*trustv1.PersonalMeshDescriptor, *trustv1.MeshSnapshot, error) {
	var descriptorPayload, snapshotPayload []byte
	err := store.db.QueryRowContext(ctx, `SELECT d.descriptor, s.snapshot FROM mesh_descriptors d JOIN mesh_snapshots s USING(mesh_id) WHERE d.mesh_id=?`, meshID).
		Scan(&descriptorPayload, &snapshotPayload)
	if err != nil {
		return nil, nil, err
	}
	descriptor := new(trustv1.PersonalMeshDescriptor)
	snapshot := new(trustv1.MeshSnapshot)
	if err := proto.Unmarshal(descriptorPayload, descriptor); err != nil {
		return nil, nil, err
	}
	if err := proto.Unmarshal(snapshotPayload, snapshot); err != nil {
		return nil, nil, err
	}
	return descriptor, snapshot, nil
}

func (store *Store) ListPersonalMeshes(ctx context.Context) ([]PersonalMeshRecord, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT d.descriptor, s.snapshot FROM mesh_descriptors d
		JOIN mesh_snapshots s USING(mesh_id) WHERE d.owner_kind=? AND s.owner_kind=? ORDER BY d.mesh_id LIMIT 32`,
		TrustOwnerPersonalMesh, TrustOwnerPersonalMesh)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []PersonalMeshRecord
	for rows.Next() {
		var descriptorPayload, snapshotPayload []byte
		if err := rows.Scan(&descriptorPayload, &snapshotPayload); err != nil {
			return nil, err
		}
		record := PersonalMeshRecord{Descriptor: new(trustv1.PersonalMeshDescriptor), Snapshot: new(trustv1.MeshSnapshot)}
		if err := proto.Unmarshal(descriptorPayload, record.Descriptor); err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(snapshotPayload, record.Snapshot); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (store *Store) ApplyPersonalMeshSnapshot(ctx context.Context, snapshot *trustv1.MeshSnapshot, expectedRevision uint64) error {
	payload, err := marshalTrust(snapshot)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE mesh_snapshots SET revision=?, policy_epoch=?, expires_at=?, snapshot=?, updated_at=?
		WHERE mesh_id=? AND owner_kind=? AND revision=? AND policy_epoch<=?`, snapshot.GetRevision(), snapshot.GetPolicyEpoch(),
		snapshot.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano(), snapshot.GetMeshId(), TrustOwnerPersonalMesh,
		expectedRevision, snapshot.GetPolicyEpoch())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 || snapshot.GetRevision() <= expectedRevision {
		return ErrTrustRevisionConflict
	}
	if err := replaceTombstones(ctx, tx, snapshot); err != nil {
		return err
	}
	return tx.Commit()
}

func replaceTombstones(ctx context.Context, tx *sql.Tx, snapshot *trustv1.MeshSnapshot) error {
	for _, tombstone := range snapshot.GetTombstones() {
		if _, err := tx.ExecContext(ctx, `INSERT INTO mesh_tombstones(mesh_id, device_id, root_key_revision,
			revoked_at_revision, policy_epoch, operation_digest) VALUES(?, ?, ?, ?, ?, ?)
			ON CONFLICT(mesh_id, device_id) DO UPDATE SET root_key_revision=excluded.root_key_revision,
			revoked_at_revision=excluded.revoked_at_revision, policy_epoch=excluded.policy_epoch,
			operation_digest=excluded.operation_digest WHERE excluded.revoked_at_revision > mesh_tombstones.revoked_at_revision`,
			snapshot.GetMeshId(), tombstone.GetDeviceId(), tombstone.GetRootKeyRevision(), tombstone.GetRevokedAtRevision(),
			tombstone.GetPolicyEpoch(), tombstone.GetOperationDigest()); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) QueueRegistryOperation(ctx context.Context, operation *trustv1.RegistryOperation) error {
	payload, err := marshalTrust(operation)
	if err != nil {
		return err
	}
	if len(payload) > maxRegistryOperationBytes {
		return errors.New("registry operation exceeds size limit")
	}
	if operation.GetKind() == trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_UNSPECIFIED || operation.GetMembership() == nil {
		return errors.New("complete registry operation is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO registry_operations(operation_id, mesh_id, expected_revision, kind,
		state, operation, created_at) VALUES(?, ?, ?, ?, 1, ?, ?)`, operation.GetOperationId(), operation.GetMeshId(),
		operation.GetExpectedRevision(), operation.GetKind(), payload, time.Now().UnixNano()); err != nil {
		return err
	}
	if operation.GetKind() == trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE {
		if _, err := tx.ExecContext(ctx, `INSERT INTO mesh_member_denials(mesh_id, device_id, root_key_revision,
		operation_id, denied_at) VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(mesh_id, device_id) DO UPDATE SET root_key_revision=excluded.root_key_revision,
		operation_id=excluded.operation_id, denied_at=excluded.denied_at`, operation.GetMeshId(), operation.GetMembership().GetDeviceId(),
			operation.GetMembership().GetRootKeyRevision(), operation.GetOperationId(), time.Now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (store *Store) ListPendingRegistryOperations(ctx context.Context, afterSequence uint64, limit uint32) ([]PendingRegistryOperation, error) {
	if limit == 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, `SELECT o.sequence, o.operation, COALESCE(d.descriptor, X'')
		FROM registry_operations o LEFT JOIN mesh_descriptors d ON d.mesh_id=o.mesh_id
		WHERE o.state=1 AND o.sequence>? ORDER BY o.sequence LIMIT ?`, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []PendingRegistryOperation
	for rows.Next() {
		var item PendingRegistryOperation
		var payload, descriptorPayload []byte
		if err := rows.Scan(&item.Sequence, &payload, &descriptorPayload); err != nil {
			return nil, err
		}
		item.Operation = new(trustv1.RegistryOperation)
		if err := proto.Unmarshal(payload, item.Operation); err != nil {
			return nil, err
		}
		if len(descriptorPayload) != 0 {
			item.Descriptor = new(trustv1.PersonalMeshDescriptor)
			if err := proto.Unmarshal(descriptorPayload, item.Descriptor); err != nil {
				return nil, err
			}
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) GetPendingRegistryOperation(ctx context.Context, operationID string) (*trustv1.RegistryOperation, error) {
	return store.getRegistryOperation(ctx, operationID, true)
}

func (store *Store) GetRegistryOperation(ctx context.Context, operationID string) (*trustv1.RegistryOperation, error) {
	return store.getRegistryOperation(ctx, operationID, false)
}

func (store *Store) getRegistryOperation(ctx context.Context, operationID string, pendingOnly bool) (*trustv1.RegistryOperation, error) {
	var payload []byte
	query := `SELECT operation FROM registry_operations WHERE operation_id=?`
	if pendingOnly {
		query += ` AND state=1`
	}
	if err := store.db.QueryRowContext(ctx, query, operationID).Scan(&payload); err != nil {
		return nil, err
	}
	operation := new(trustv1.RegistryOperation)
	if err := proto.Unmarshal(payload, operation); err != nil {
		return nil, err
	}
	return operation, nil
}

func (store *Store) CommitRegistryOperation(ctx context.Context, operationID string, snapshot *trustv1.MeshSnapshot) error {
	payload, err := marshalTrust(snapshot)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var meshID string
	var operationPayload []byte
	var expectedRevision uint64
	if err := tx.QueryRowContext(ctx, `SELECT mesh_id, expected_revision, operation FROM registry_operations
		WHERE operation_id=? AND state=1`, operationID).Scan(&meshID, &expectedRevision, &operationPayload); err != nil {
		return err
	}
	operation := new(trustv1.RegistryOperation)
	if err := proto.Unmarshal(operationPayload, operation); err != nil {
		return err
	}
	localExpectedRevision := expectedRevision
	if operation.GetKind() == trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS {
		localExpectedRevision = 1
	}
	if snapshot.GetMeshId() != meshID || (operation.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS && snapshot.GetRevision() <= expectedRevision) {
		return ErrTrustRevisionConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE mesh_snapshots SET revision=?, policy_epoch=?, expires_at=?, snapshot=?, updated_at=?
		WHERE mesh_id=? AND owner_kind=? AND revision=? AND policy_epoch<=?`, snapshot.GetRevision(), snapshot.GetPolicyEpoch(),
		snapshot.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano(), snapshot.GetMeshId(), TrustOwnerPersonalMesh,
		localExpectedRevision, snapshot.GetPolicyEpoch())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if err := replaceTombstones(ctx, tx, snapshot); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE registry_operations SET state=2, completed_at=? WHERE operation_id=? AND state=1`,
		time.Now().UnixNano(), operationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mesh_member_denials WHERE mesh_id=? AND device_id=? AND operation_id=?`,
		operation.GetMeshId(), operation.GetMembership().GetDeviceId(), operationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) IsMeshMemberDenied(ctx context.Context, meshID, deviceID string, rootRevision uint64) (bool, error) {
	var deniedRevision uint64
	err := store.db.QueryRowContext(ctx, `SELECT root_key_revision FROM mesh_member_denials WHERE mesh_id=? AND device_id=?`, meshID, deviceID).Scan(&deniedRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && rootRevision <= deniedRevision, err
}
