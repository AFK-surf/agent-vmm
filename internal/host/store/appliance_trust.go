package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

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
	var enabled bool
	var installationState, registrationState int
	if err := tx.QueryRowContext(ctx, `SELECT i.revision, i.registration_id, i.current_boot_id, i.current_container_instance, i.desired_enabled, i.state, r.state
		FROM appliance_installations i JOIN remote_registrations r ON r.id=i.registration_id WHERE i.id=?`, binding.InstallationID).Scan(&revision, &registration, &bootID, &containerInstance, &enabled, &installationState, &registrationState); err != nil {
		return err
	}
	if binding.AuthorityID != anchor.GetAuthority().GetAuthorityId() || binding.RegistrationID == "" || binding.RegistrationID != registration ||
		binding.BootID == "" || binding.BootID != bootID || binding.ContainerInstance == "" || binding.ContainerInstance != containerInstance ||
		binding.ConfigRevision != revision || !enabled ||
		(installationState != int(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) && installationState != int(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)) ||
		registrationState == int(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
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
			if errors.Is(err, sql.ErrNoRows) {
				return ErrTrustRevisionConflict
			}
			return err
		}
		if !bytes.Equal(storedAnchor, anchorPayload) || !bytes.Equal(storedCredential, credentialPayload) {
			previousAnchor := new(trustv1.TrustAnchor)
			previousCredential := new(trustv1.MembershipCredential)
			if proto.Unmarshal(storedAnchor, previousAnchor) != nil || proto.Unmarshal(storedCredential, previousCredential) != nil ||
				!applianceTrustRenewal(previousAnchor, previousCredential, anchor, credential) {
				return ErrTrustRevisionConflict
			}
			if err := installManagedTrustTx(ctx, tx, anchor, credential, anchorPayload, credentialPayload, now); err != nil {
				return err
			}
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
		(source_installation_id=? OR destination_installation_id=? OR source_environment_id IN (SELECT id FROM environments WHERE registration_id=?) OR destination_export_id IN
		(SELECT x.export_id FROM service_exports x JOIN environments e ON e.id=x.environment_id WHERE e.registration_id=?))`, now, installationID, installationID, registration, registration); err != nil {
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
		JOIN service_imports i ON i.destination_export_id=x.export_id WHERE a.installation_id=? AND i.revoked=0 UNION SELECT import_id FROM service_imports WHERE (source_installation_id=? OR destination_installation_id=?) AND revoked=0 ORDER BY 1`, installationID, installationID, installationID, installationID)
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

func applianceTrustRenewal(oldAnchor *trustv1.TrustAnchor, oldCredential *trustv1.MembershipCredential, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) bool {
	if anchor.GetRevision() <= oldAnchor.GetRevision() || credential.GetPolicyRevision() <= oldCredential.GetPolicyRevision() ||
		!anchor.GetNotAfter().AsTime().After(oldAnchor.GetNotAfter().AsTime()) || !credential.GetExpiresAt().AsTime().After(oldCredential.GetExpiresAt().AsTime()) {
		return false
	}
	previousAnchor := proto.Clone(oldAnchor).(*trustv1.TrustAnchor)
	nextAnchor := proto.Clone(anchor).(*trustv1.TrustAnchor)
	previousAnchor.Revision, nextAnchor.Revision = 0, 0
	previousAnchor.Authority.Revision, nextAnchor.Authority.Revision = 0, 0
	previousAnchor.NotBefore, nextAnchor.NotBefore = nil, nil
	previousAnchor.NotAfter, nextAnchor.NotAfter = nil, nil
	previousCredential := proto.Clone(oldCredential).(*trustv1.MembershipCredential)
	nextCredential := proto.Clone(credential).(*trustv1.MembershipCredential)
	previousCredential.Authority.Revision, nextCredential.Authority.Revision = 0, 0
	previousCredential.PolicyRevision, nextCredential.PolicyRevision = 0, 0
	previousCredential.ExpiresAt, nextCredential.ExpiresAt = nil, nil
	previousCredential.Signature, nextCredential.Signature = nil, nil
	return proto.Equal(previousAnchor, nextAnchor) && proto.Equal(previousCredential, nextCredential)
}

func (store *Store) GetApplianceMembershipCredential(ctx context.Context, installationID string) (*trustv1.MembershipCredential, error) {
	var payload []byte
	err := store.db.QueryRowContext(ctx, `SELECT m.credential FROM appliance_trust_authorities a JOIN membership_credentials m
        ON m.authority_id=a.authority_id JOIN device_identity d ON d.device_id=m.subject_device_id
        WHERE a.installation_id=? AND a.state=?`, installationID, ApplianceTrustActive).Scan(&payload)
	if err != nil {
		return nil, err
	}
	credential := new(trustv1.MembershipCredential)
	if err := proto.Unmarshal(payload, credential); err != nil {
		return nil, err
	}
	return credential, nil
}
