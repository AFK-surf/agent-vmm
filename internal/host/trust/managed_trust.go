package trust

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
)

// InstallManagedTrust accepts only a bundle delivered through the
// TLS-authenticated, one-time controller enrollment flow. It verifies the
// controller-signed device credential before atomically advancing local facts.
func (manager *Manager) InstallManagedTrust(ctx context.Context, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	validatedAt := manager.now()
	if err := manager.validateManagedTrustAt(ctx, anchor, credential, validatedAt); err != nil {
		return err
	}
	return manager.store.InstallManagedTrust(ctx, anchor, credential, validatedAt)
}

func (manager *Manager) InstallApplianceManagedTrust(ctx context.Context, binding store.ApplianceTrustAuthority, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	now := manager.now()
	if err := manager.validateManagedTrustAt(ctx, anchor, credential, now); err != nil {
		return err
	}
	if excess := anchor.GetNotAfter().AsTime().Sub(now) - 365*24*time.Hour; excess > 0 {
		return fmt.Errorf("managed trust stage=appliance_ttl reason=anchor_remaining excess_ms=%d: %w", excess.Milliseconds(), ErrRouteUnauthorized)
	}
	if excess := anchor.GetNotAfter().AsTime().Sub(anchor.GetNotBefore().AsTime()) - (365*24*time.Hour + time.Minute); excess > 0 {
		return fmt.Errorf("managed trust stage=appliance_ttl reason=anchor_span excess_ms=%d: %w", excess.Milliseconds(), ErrRouteUnauthorized)
	}
	if excess := credential.GetExpiresAt().AsTime().Sub(now) - 30*24*time.Hour; excess > 0 {
		return fmt.Errorf("managed trust stage=appliance_ttl reason=membership_remaining excess_ms=%d: %w", excess.Milliseconds(), ErrRouteUnauthorized)
	}
	return manager.store.InstallApplianceManagedTrust(ctx, binding, anchor, credential)
}

func (manager *Manager) validateManagedTrustAt(ctx context.Context, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential, now time.Time) error {
	identity, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || identity.State != store.DeviceIdentityReady || anchor == nil || credential == nil {
		return fmt.Errorf("managed trust stage=identity reason=unavailable: %w", ErrRouteUnauthorized)
	}
	if anchor.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER ||
		anchor.GetAuthority().GetAuthorityId() == "" || anchor.GetAuthority().GetRevision() != anchor.GetRevision() || anchor.GetRevision() == 0 {
		return fmt.Errorf("managed trust stage=scope reason=anchor_authority: %w", ErrRouteUnauthorized)
	}
	if anchor.GetNotBefore() == nil || anchor.GetNotAfter() == nil {
		return fmt.Errorf("managed trust stage=anchor_time reason=missing: %w", ErrRouteUnauthorized)
	}
	if excess := anchor.GetNotBefore().AsTime().Sub(now); excess > 0 {
		return fmt.Errorf("managed trust stage=anchor_time reason=not_yet_valid excess_ms=%d: %w", excess.Milliseconds(), ErrRouteUnauthorized)
	}
	if !anchor.GetNotAfter().AsTime().After(now) {
		return fmt.Errorf("managed trust stage=anchor_time reason=expired excess_ms=%d: %w", now.Sub(anchor.GetNotAfter().AsTime()).Milliseconds(), ErrRouteUnauthorized)
	}
	if !proto.Equal(anchor.GetAuthority(), credential.GetAuthority()) || credential.GetPolicyRevision() == 0 || len(credential.GetOpaqueClaimsDigest()) != sha256.Size {
		return fmt.Errorf("managed trust stage=scope reason=membership_authority: %w", ErrRouteUnauthorized)
	}
	if credential.GetSubjectDeviceId() != identity.Identity.GetDeviceId() ||
		credential.GetSubjectRootKeyRevision() != identity.Identity.GetRootKeyRevision() ||
		!validP256PublicKey(credential.GetSubjectRootPublicKey()) ||
		!equalBytes(credential.GetSubjectRootPublicKey(), identity.Identity.GetRootPublicKey()) {
		return fmt.Errorf("managed trust stage=identity reason=subject_mismatch: %w", ErrRouteUnauthorized)
	}
	if credential.GetExpiresAt() == nil {
		return fmt.Errorf("managed trust stage=membership_time reason=missing: %w", ErrRouteUnauthorized)
	}
	if !credential.GetExpiresAt().AsTime().After(now) {
		return fmt.Errorf("managed trust stage=membership_time reason=expired excess_ms=%d: %w", now.Sub(credential.GetExpiresAt().AsTime()).Milliseconds(), ErrRouteUnauthorized)
	}
	if excess := credential.GetExpiresAt().AsTime().Sub(anchor.GetNotAfter().AsTime()); excess > 0 {
		return fmt.Errorf("managed trust stage=membership_time reason=beyond_anchor excess_ms=%d: %w", excess.Milliseconds(), ErrRouteUnauthorized)
	}
	allowed := false
	for _, kind := range anchor.GetAllowedCapabilityKinds() {
		allowed = allowed || kind == trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE
	}
	if !allowed || len(anchor.GetVerificationKeys()) == 0 || len(anchor.GetVerificationKeys()) > 8 {
		return fmt.Errorf("managed trust stage=scope reason=capability_keys: %w", ErrRouteUnauthorized)
	}
	var verificationKey []byte
	for _, key := range anchor.GetVerificationKeys() {
		if key.GetKeyId() == anchor.GetAuthority().GetKeyId() && key.GetSignatureSuite() == trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
			verificationKey = key.GetPublicKey()
		}
	}
	input, err := signing.CanonicalInput(credential)
	if err != nil {
		return fmt.Errorf("managed trust stage=signature reason=canonical_input: %w", ErrRouteUnauthorized)
	}
	digest := sha256.Sum256(input)
	if len(verificationKey) != 33 || !verifyP256(verificationKey, digest[:], credential.GetSignature()) {
		return fmt.Errorf("managed trust stage=signature reason=verification: %w", ErrRouteUnauthorized)
	}
	return nil
}
