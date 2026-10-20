package store

import (
	"context"
	"database/sql"
	"errors"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"time"
)

const (
	DeviceIdentityReady       = 1
	DeviceIdentityNeedsRepair = 2
	TrustOwnerManaged         = 1
	TrustOwnerPersonalMesh    = 2
	maxRegistryOperationBytes = 64 << 10
)

var ErrTrustRevisionConflict = errors.New("trust revision conflict")

const (
	ApplianceTrustActive  = 1
	ApplianceTrustRevoked = 2
)

type ApplianceTrustAuthority struct {
	AuthorityID, InstallationID, RegistrationID, BootID, ContainerInstance string
	ConfigRevision                                                         uint64
	State                                                                  int
}

type DeviceIdentityRecord struct {
	Identity           *trustv1.DeviceIdentity
	Custody            string
	State              int32
	EndpointGeneration uint64
}

type PendingRegistryOperation struct {
	Sequence   uint64
	Operation  *trustv1.RegistryOperation
	Descriptor *trustv1.PersonalMeshDescriptor
}

type PersonalMeshRecord struct {
	Descriptor *trustv1.PersonalMeshDescriptor
	Snapshot   *trustv1.MeshSnapshot
}

func (store *Store) GetDeviceIdentity(ctx context.Context) (DeviceIdentityRecord, error) {
	var record DeviceIdentityRecord
	var deviceID string
	var publicKey []byte
	var revision uint64
	err := store.db.QueryRowContext(ctx, `SELECT device_id, root_public_key, root_key_revision, custody, state,
		endpoint_generation FROM device_identity WHERE id=1`).Scan(&deviceID, &publicKey, &revision, &record.Custody, &record.State, &record.EndpointGeneration)
	if err != nil {
		return DeviceIdentityRecord{}, err
	}
	record.Identity = &trustv1.DeviceIdentity{DeviceId: deviceID, RootPublicKey: publicKey,
		RootKeyRevision: revision, SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}
	return record, nil
}

func (store *Store) CreateDeviceIdentity(ctx context.Context, record DeviceIdentityRecord) error {
	now := time.Now().UnixNano()
	_, err := store.db.ExecContext(ctx, `INSERT INTO device_identity
		(id, device_id, root_public_key, root_key_revision, custody, state, endpoint_generation, created_at, updated_at)
		VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?)`, record.Identity.GetDeviceId(), record.Identity.GetRootPublicKey(),
		record.Identity.GetRootKeyRevision(), record.Custody, record.State, record.EndpointGeneration, now, now)
	return err
}

func (store *Store) MarkDeviceIdentityNeedsRepair(ctx context.Context, expectedRevision uint64) error {
	result, err := store.db.ExecContext(ctx, `UPDATE device_identity SET state=?, updated_at=?
		WHERE id=1 AND root_key_revision=?`, DeviceIdentityNeedsRepair, time.Now().UnixNano(), expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	return nil
}

func (store *Store) RotateDeviceIdentity(ctx context.Context, requestID string, expectedRevision uint64, record DeviceIdentityRecord, rotation *trustv1.DeviceRootRotation) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(rotation)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE device_identity SET root_public_key=?, root_key_revision=?, custody=?, state=?, updated_at=?
		WHERE id=1 AND root_key_revision=?`, record.Identity.GetRootPublicKey(), record.Identity.GetRootKeyRevision(),
		record.Custody, record.State, time.Now().UnixNano(), expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO device_root_rotations(new_revision, request_id, rotation, created_at) VALUES(?, ?, ?, ?)`,
		record.Identity.GetRootKeyRevision(), requestID, payload, time.Now().UnixNano()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM endpoint_observations WHERE device_id=?`, record.Identity.GetDeviceId()); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) GetDeviceRootRotation(ctx context.Context, requestID string) (*trustv1.DeviceRootRotation, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT rotation FROM device_root_rotations WHERE request_id=?`, requestID).Scan(&payload); err != nil {
		return nil, err
	}
	rotation := new(trustv1.DeviceRootRotation)
	if err := proto.Unmarshal(payload, rotation); err != nil {
		return nil, err
	}
	return rotation, nil
}

func (store *Store) RepairDeviceIdentity(ctx context.Context, requestID string, record DeviceIdentityRecord) error {
	identityPayload, err := marshalTrust(record.Identity)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE device_identity SET device_id=?, root_public_key=?, root_key_revision=?,
		custody=?, state=?, endpoint_generation=0, updated_at=? WHERE id=1 AND state=?`, record.Identity.GetDeviceId(),
		record.Identity.GetRootPublicKey(), record.Identity.GetRootKeyRevision(), record.Custody, record.State,
		time.Now().UnixNano(), DeviceIdentityNeedsRepair)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	for _, statement := range []string{"DELETE FROM registry_operations", "DELETE FROM mesh_member_denials", "DELETE FROM mesh_tombstones",
		"DELETE FROM mesh_snapshots", "DELETE FROM mesh_descriptors", "DELETE FROM membership_credentials", "DELETE FROM endpoint_observations"} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO device_repairs(request_id, identity, repaired_at) VALUES(?, ?, ?)`,
		requestID, identityPayload, time.Now().UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) HasDeviceRepair(ctx context.Context, requestID string) (bool, error) {
	var ignored int
	err := store.db.QueryRowContext(ctx, `SELECT 1 FROM device_repairs WHERE request_id=?`, requestID).Scan(&ignored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (store *Store) GetDeviceRepair(ctx context.Context, requestID string) (*trustv1.DeviceIdentity, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT identity FROM device_repairs WHERE request_id=?`, requestID).Scan(&payload); err != nil {
		return nil, err
	}
	identity := new(trustv1.DeviceIdentity)
	if err := proto.Unmarshal(payload, identity); err != nil {
		return nil, err
	}
	return identity, nil
}

func marshalTrust(message proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func (store *Store) PutTrustAnchor(ctx context.Context, ownerKind int, anchor *trustv1.TrustAnchor, expectedRevision uint64) error {
	if anchor.GetAuthority() == nil || anchor.GetAuthority().GetAuthorityId() == "" || anchor.GetRevision() == 0 || anchor.GetNotAfter() == nil {
		return errors.New("complete trust anchor is required")
	}
	if anchor.GetAuthority().GetRevision() != anchor.GetRevision() || anchor.GetNotBefore() == nil ||
		!anchor.GetNotAfter().AsTime().After(anchor.GetNotBefore().AsTime()) || len(anchor.GetVerificationKeys()) == 0 || len(anchor.GetVerificationKeys()) > 8 ||
		(ownerKind == TrustOwnerManaged && anchor.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER) ||
		(ownerKind == TrustOwnerPersonalMesh && anchor.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH) {
		return errors.New("trust anchor owner, revision, keys, or validity window is invalid")
	}
	keyIDs := make(map[string]struct{}, len(anchor.GetVerificationKeys()))
	for _, key := range anchor.GetVerificationKeys() {
		if key.GetKeyId() == "" || len(key.GetPublicKey()) != 33 || key.GetSignatureSuite() != trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
			return errors.New("trust anchor verification key is invalid")
		}
		if _, duplicate := keyIDs[key.GetKeyId()]; duplicate {
			return errors.New("trust anchor verification key is duplicated")
		}
		keyIDs[key.GetKeyId()] = struct{}{}
	}
	payload, err := marshalTrust(anchor)
	if err != nil {
		return err
	}
	if expectedRevision == 0 {
		_, err = store.db.ExecContext(ctx, `INSERT INTO trust_anchors(authority_id, owner_kind, revision, expires_at, anchor, updated_at)
			VALUES(?, ?, ?, ?, ?, ?)`, anchor.GetAuthority().GetAuthorityId(), ownerKind, anchor.GetRevision(),
			anchor.GetNotAfter().AsTime().UnixNano(), payload, time.Now().UnixNano())
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE trust_anchors SET revision=?, expires_at=?, anchor=?, updated_at=?
		WHERE authority_id=? AND owner_kind=? AND revision=?`, anchor.GetRevision(), anchor.GetNotAfter().AsTime().UnixNano(),
		payload, time.Now().UnixNano(), anchor.GetAuthority().GetAuthorityId(), ownerKind, expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	return nil
}

func (store *Store) GetTrustAnchor(ctx context.Context, authorityID string) (int, *trustv1.TrustAnchor, error) {
	var owner int
	var payload []byte
	err := store.db.QueryRowContext(ctx, `SELECT owner_kind, anchor FROM trust_anchors WHERE authority_id=?`, authorityID).Scan(&owner, &payload)
	if err != nil {
		return 0, nil, err
	}
	anchor := new(trustv1.TrustAnchor)
	if err := proto.Unmarshal(payload, anchor); err != nil {
		return 0, nil, err
	}
	return owner, anchor, nil
}

func (store *Store) PutMembershipCredential(ctx context.Context, credential *trustv1.MembershipCredential, expectedPolicyRevision uint64) error {
	if credential.GetAuthority() == nil || credential.GetAuthority().GetAuthorityId() == "" || credential.GetSubjectDeviceId() == "" ||
		credential.GetPolicyRevision() == 0 || credential.GetSubjectRootKeyRevision() == 0 || len(credential.GetOpaqueClaimsDigest()) != 32 ||
		credential.GetExpiresAt() == nil || credential.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER {
		return errors.New("complete membership credential is required")
	}
	payload, err := marshalTrust(credential)
	if err != nil {
		return err
	}
	if expectedPolicyRevision == 0 {
		_, err = store.db.ExecContext(ctx, `INSERT INTO membership_credentials(authority_id, subject_device_id, policy_revision,
			expires_at, credential, updated_at) VALUES(?, ?, ?, ?, ?, ?)`, credential.GetAuthority().GetAuthorityId(),
			credential.GetSubjectDeviceId(), credential.GetPolicyRevision(), credential.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano())
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE membership_credentials SET policy_revision=?, expires_at=?, credential=?, updated_at=?
		WHERE authority_id=? AND subject_device_id=? AND policy_revision=?`, credential.GetPolicyRevision(), credential.GetExpiresAt().AsTime().UnixNano(),
		payload, time.Now().UnixNano(), credential.GetAuthority().GetAuthorityId(), credential.GetSubjectDeviceId(), expectedPolicyRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 || credential.GetPolicyRevision() <= expectedPolicyRevision {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	return nil
}

func (store *Store) GetMembershipCredential(ctx context.Context, authorityID, subjectDeviceID string) (*trustv1.MembershipCredential, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT credential FROM membership_credentials WHERE authority_id=? AND subject_device_id=?`,
		authorityID, subjectDeviceID).Scan(&payload); err != nil {
		return nil, err
	}
	credential := new(trustv1.MembershipCredential)
	if err := proto.Unmarshal(payload, credential); err != nil {
		return nil, err
	}
	return credential, nil
}

func (store *Store) IsRouteDenied(ctx context.Context, capabilityID string) (bool, error) {
	var ignored int
	err := store.db.QueryRowContext(ctx, `SELECT 1 FROM route_denials WHERE capability_id=?`, capabilityID).Scan(&ignored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (store *Store) DenyRoute(ctx context.Context, capabilityID, reason string) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO route_denials(capability_id, denied_at, reason) VALUES(?, ?, ?)
		ON CONFLICT(capability_id) DO UPDATE SET denied_at=excluded.denied_at, reason=excluded.reason`, capabilityID, time.Now().UnixNano(), reason)
	return err
}

func (store *Store) PutEndpointObservation(ctx context.Context, observation *trustv1.EndpointObservation, expectedGeneration uint64) error {
	payload, err := marshalTrust(observation)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE device_identity SET endpoint_generation=?, updated_at=?
		WHERE id=1 AND device_id=? AND root_key_revision=? AND state=? AND endpoint_generation=?`, observation.GetEndpointGeneration(),
		time.Now().UnixNano(), observation.GetDeviceId(), observation.GetRootKeyRevision(), DeviceIdentityReady, expectedGeneration)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 || observation.GetEndpointGeneration() < expectedGeneration {
		return ErrTrustRevisionConflict
	}
	endpointResult, err := tx.ExecContext(ctx, `INSERT INTO endpoint_observations(device_id, root_key_revision, endpoint_generation,
		expires_at, observation, updated_at) VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET root_key_revision=excluded.root_key_revision,
		endpoint_generation=excluded.endpoint_generation, expires_at=excluded.expires_at,
		observation=excluded.observation, updated_at=excluded.updated_at
		WHERE excluded.endpoint_generation > endpoint_observations.endpoint_generation OR
			(excluded.endpoint_generation = endpoint_observations.endpoint_generation AND excluded.expires_at > endpoint_observations.expires_at)`,
		observation.GetDeviceId(), observation.GetRootKeyRevision(), observation.GetEndpointGeneration(),
		observation.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano())
	if err != nil {
		return err
	}
	endpointChanged, err := endpointResult.RowsAffected()
	if err != nil || endpointChanged != 1 {
		return ErrTrustRevisionConflict
	}
	return tx.Commit()
}

func (store *Store) PutRemoteEndpointObservation(ctx context.Context, observation *trustv1.EndpointObservation) error {
	payload, err := marshalTrust(observation)
	if err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `INSERT INTO endpoint_observations(device_id, root_key_revision, endpoint_generation,
		expires_at, observation, updated_at) VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET root_key_revision=excluded.root_key_revision,
		endpoint_generation=excluded.endpoint_generation, expires_at=excluded.expires_at,
		observation=excluded.observation, updated_at=excluded.updated_at
		WHERE excluded.endpoint_generation > endpoint_observations.endpoint_generation OR
			(excluded.endpoint_generation = endpoint_observations.endpoint_generation AND excluded.expires_at > endpoint_observations.expires_at)`, observation.GetDeviceId(), observation.GetRootKeyRevision(),
		observation.GetEndpointGeneration(), observation.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	return nil
}

func (store *Store) GetEndpointObservation(ctx context.Context, deviceID string) (*trustv1.EndpointObservation, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT observation FROM endpoint_observations WHERE device_id=? AND expires_at>?`, deviceID, time.Now().UnixNano()).Scan(&payload); err != nil {
		return nil, err
	}
	value := new(trustv1.EndpointObservation)
	if err := proto.Unmarshal(payload, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (store *Store) GetEndpointObservationAny(ctx context.Context, deviceID string) (*trustv1.EndpointObservation, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT observation FROM endpoint_observations WHERE device_id=?`, deviceID).Scan(&payload); err != nil {
		return nil, err
	}
	value := new(trustv1.EndpointObservation)
	if err := proto.Unmarshal(payload, value); err != nil {
		return nil, err
	}
	return value, nil
}
