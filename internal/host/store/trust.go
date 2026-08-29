package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
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

// InstallManagedTrust atomically advances the controller anchor and the local
// device credential. Equal revisions are idempotent for byte-identical
// artifacts. A different, already-validated credential at the same revision
// may replace only an expired credential; rollback and unexpired
// same-revision substitution fail closed.
func (store *Store) InstallManagedTrust(ctx context.Context, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential, validatedAt time.Time) error {
	anchorPayload, err := marshalTrust(anchor)
	if err != nil {
		return err
	}
	credentialPayload, err := marshalTrust(credential)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := validatedAt.UnixNano()
	if err := installManagedTrustTx(ctx, tx, anchor, credential, anchorPayload, credentialPayload, now); err != nil {
		return err
	}
	return tx.Commit()
}

func installManagedTrustTx(ctx context.Context, tx *sql.Tx, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential, anchorPayload, credentialPayload []byte, now int64) error {
	var err error
	var currentAnchorRevision uint64
	var currentAnchorPayload []byte
	err = tx.QueryRowContext(ctx, `SELECT revision, anchor FROM trust_anchors WHERE authority_id=?`, anchor.GetAuthority().GetAuthorityId()).Scan(&currentAnchorRevision, &currentAnchorPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO trust_anchors(authority_id, owner_kind, revision, expires_at, anchor, updated_at)
			VALUES(?, ?, ?, ?, ?, ?)`, anchor.GetAuthority().GetAuthorityId(), TrustOwnerManaged, anchor.GetRevision(),
			anchor.GetNotAfter().AsTime().UnixNano(), anchorPayload, now)
	case err != nil:
		return err
	case anchor.GetRevision() == currentAnchorRevision && bytes.Equal(anchorPayload, currentAnchorPayload):
	case anchor.GetRevision() > currentAnchorRevision:
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE trust_anchors SET owner_kind=?, revision=?, expires_at=?, anchor=?, updated_at=?
			WHERE authority_id=? AND owner_kind=? AND revision=?`, TrustOwnerManaged, anchor.GetRevision(), anchor.GetNotAfter().AsTime().UnixNano(),
			anchorPayload, now, anchor.GetAuthority().GetAuthorityId(), TrustOwnerManaged, currentAnchorRevision)
		if err == nil {
			if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
				return errors.Join(ErrTrustRevisionConflict, changeErr)
			}
		}
	default:
		return ErrTrustRevisionConflict
	}
	if err != nil {
		return err
	}
	var currentPolicyRevision uint64
	var currentCredentialExpiry int64
	var currentCredentialPayload []byte
	err = tx.QueryRowContext(ctx, `SELECT policy_revision, expires_at, credential FROM membership_credentials
		WHERE authority_id=? AND subject_device_id=?`, credential.GetAuthority().GetAuthorityId(), credential.GetSubjectDeviceId()).Scan(&currentPolicyRevision, &currentCredentialExpiry, &currentCredentialPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO membership_credentials(authority_id, subject_device_id, policy_revision,
			expires_at, credential, updated_at) VALUES(?, ?, ?, ?, ?, ?)`, credential.GetAuthority().GetAuthorityId(),
			credential.GetSubjectDeviceId(), credential.GetPolicyRevision(), credential.GetExpiresAt().AsTime().UnixNano(), credentialPayload, now)
	case err != nil:
		return err
	case credential.GetPolicyRevision() == currentPolicyRevision && bytes.Equal(credentialPayload, currentCredentialPayload):
	case credential.GetPolicyRevision() == currentPolicyRevision && currentCredentialExpiry <= now:
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE membership_credentials SET expires_at=?, credential=?, updated_at=?
			WHERE authority_id=? AND subject_device_id=? AND policy_revision=? AND expires_at<=?`, credential.GetExpiresAt().AsTime().UnixNano(),
			credentialPayload, now, credential.GetAuthority().GetAuthorityId(), credential.GetSubjectDeviceId(), currentPolicyRevision, now)
		if err == nil {
			if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
				return errors.Join(ErrTrustRevisionConflict, changeErr)
			}
		}
	case credential.GetPolicyRevision() > currentPolicyRevision:
		_, err = tx.ExecContext(ctx, `UPDATE membership_credentials SET policy_revision=?, expires_at=?, credential=?, updated_at=?
			WHERE authority_id=? AND subject_device_id=? AND policy_revision=?`, credential.GetPolicyRevision(),
			credential.GetExpiresAt().AsTime().UnixNano(), credentialPayload, now, credential.GetAuthority().GetAuthorityId(),
			credential.GetSubjectDeviceId(), currentPolicyRevision)
	default:
		return ErrTrustRevisionConflict
	}
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) InstallApplianceManagedTrust(ctx context.Context, binding ApplianceTrustAuthority, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	anchorPayload, err := marshalTrust(anchor)
	if err != nil {
		return err
	}
	credentialPayload, err := marshalTrust(credential)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var retired int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM revoked_appliance_authorities WHERE authority_id=?`, binding.AuthorityID).Scan(&retired); err != nil {
		return err
	}
	if retired != 0 {
		return ErrTrustRevisionConflict
	}
	var revision uint64
	var registration, bootID, containerInstance string
	if err := tx.QueryRowContext(ctx, `SELECT revision, registration_id, current_boot_id, current_container_instance
		FROM appliance_installations WHERE id=?`, binding.InstallationID).Scan(&revision, &registration, &bootID, &containerInstance); err != nil {
		return err
	}
	if binding.AuthorityID != anchor.GetAuthority().GetAuthorityId() || binding.RegistrationID == "" || binding.RegistrationID != registration ||
		binding.BootID == "" || binding.BootID != bootID || binding.ContainerInstance == "" || binding.ContainerInstance != containerInstance ||
		binding.ConfigRevision != revision {
		return ErrTrustRevisionConflict
	}
	var existing ApplianceTrustAuthority
	err = tx.QueryRowContext(ctx, `SELECT authority_id, installation_id, registration_id, boot_id, container_instance, config_revision, state
		FROM appliance_trust_authorities WHERE authority_id=? OR installation_id=?`, binding.AuthorityID, binding.InstallationID).
		Scan(&existing.AuthorityID, &existing.InstallationID, &existing.RegistrationID, &existing.BootID, &existing.ContainerInstance, &existing.ConfigRevision, &existing.State)
	now := time.Now().UnixNano()
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := installManagedTrustTx(ctx, tx, anchor, credential, anchorPayload, credentialPayload, now); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO appliance_trust_authorities(authority_id, installation_id, registration_id,
			boot_id, container_instance, config_revision, state, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, binding.AuthorityID,
			binding.InstallationID, binding.RegistrationID, binding.BootID, binding.ContainerInstance, binding.ConfigRevision, ApplianceTrustActive, now)
	case err != nil:
		return err
	default:
		if existing.InstallationID != binding.InstallationID || existing.RegistrationID != binding.RegistrationID || existing.State != ApplianceTrustActive {
			return ErrTrustRevisionConflict
		}
		var storedAnchor, storedCredential []byte
		if err := tx.QueryRowContext(ctx, `SELECT a.anchor, m.credential FROM trust_anchors a JOIN membership_credentials m
			ON m.authority_id=a.authority_id WHERE a.authority_id=? AND m.subject_device_id=?`, binding.AuthorityID,
			credential.GetSubjectDeviceId()).Scan(&storedAnchor, &storedCredential); err != nil {
			return err
		}
		if !bytes.Equal(storedAnchor, anchorPayload) || !bytes.Equal(storedCredential, credentialPayload) {
			return ErrTrustRevisionConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE appliance_trust_authorities SET boot_id=?, container_instance=?, config_revision=?, updated_at=? WHERE authority_id=?`, binding.BootID, binding.ContainerInstance, binding.ConfigRevision, now, binding.AuthorityID); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) GetApplianceTrustAuthority(ctx context.Context, authorityID string) (ApplianceTrustAuthority, error) {
	var value ApplianceTrustAuthority
	err := store.db.QueryRowContext(ctx, `SELECT authority_id, installation_id, registration_id, boot_id, container_instance, config_revision, state
		FROM appliance_trust_authorities WHERE authority_id=?`, authorityID).Scan(&value.AuthorityID, &value.InstallationID,
		&value.RegistrationID, &value.BootID, &value.ContainerInstance, &value.ConfigRevision, &value.State)
	return value, err
}

func (store *Store) ValidateApplianceRouteAuthority(ctx context.Context, authorityID, allocationID string, routeGeneration, policyRevision uint64) error {
	var retired int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM revoked_appliance_authorities WHERE authority_id=?`, authorityID).Scan(&retired); err != nil {
		return err
	}
	if retired != 0 {
		return ErrTrustRevisionConflict
	}
	var state int
	var registration string
	var generation, environmentPolicy uint64
	err := store.db.QueryRowContext(ctx, `SELECT a.state, a.registration_id, a.config_revision, e.policy_revision
		FROM appliance_trust_authorities a JOIN environments e ON e.registration_id=a.registration_id
		WHERE a.authority_id=? AND e.allocation_id=?`, authorityID, allocationID).Scan(&state, &registration, &generation, &environmentPolicy)
	if errors.Is(err, sql.ErrNoRows) {
		var scoped int
		if lookupErr := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM appliance_trust_authorities WHERE authority_id=?`, authorityID).Scan(&scoped); lookupErr != nil {
			return lookupErr
		}
		if scoped == 0 {
			return nil
		}
	}
	if err != nil || registration == "" || state != ApplianceTrustActive || generation != routeGeneration || environmentPolicy != policyRevision {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	return nil
}

func (store *Store) RevokeApplianceTrustAuthority(ctx context.Context, installationID string) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var registration string
	if err := tx.QueryRowContext(ctx, `SELECT registration_id FROM appliance_trust_authorities WHERE installation_id=?`, installationID).Scan(&registration); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `UPDATE appliance_trust_authorities SET state=?, updated_at=? WHERE installation_id=? AND state=?`,
		ApplianceTrustRevoked, now, installationID, ApplianceTrustActive); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1, revision=revision+1, updated_at=? WHERE revoked=0 AND
		(source_environment_id IN (SELECT id FROM environments WHERE registration_id=?) OR destination_export_id IN
		(SELECT x.export_id FROM service_exports x JOIN environments e ON e.id=x.environment_id WHERE e.registration_id=?))`, now, registration, registration); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?, revision=revision+1, generation=generation+1, updated_at=?
		WHERE state=? AND import_id IN (SELECT import_id FROM service_imports WHERE revoked=1)`, servicev1.RouteState_ROUTE_STATE_REVOKED,
		now, servicev1.RouteState_ROUTE_STATE_ACTIVE); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) ListApplianceRouteImports(ctx context.Context, installationID string) ([]string, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT DISTINCT i.import_id FROM appliance_trust_authorities a
		JOIN environments se ON se.registration_id=a.registration_id
		JOIN service_imports i ON i.source_environment_id=se.id
		WHERE a.installation_id=? AND i.revoked=0 UNION SELECT DISTINCT i.import_id FROM appliance_trust_authorities a
		JOIN environments de ON de.registration_id=a.registration_id JOIN service_exports x ON x.environment_id=de.id
		JOIN service_imports i ON i.destination_export_id=x.export_id WHERE a.installation_id=? AND i.revoked=0 ORDER BY 1`, installationID, installationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

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
