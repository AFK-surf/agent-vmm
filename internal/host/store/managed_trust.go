package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"time"
)

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
