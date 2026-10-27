package trust

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
)

type RouteExpectation struct {
	SourceDeviceID      string
	SourceAllocationID  string
	DestinationDeviceID string
	DestinationExportID string
	RouteClass          string
	Audience            string
	RouteGeneration     uint64
	PolicyRevision      uint64
}

func (manager *Manager) AuthorizeRoute(ctx context.Context, capability *trustv1.RouteCapability, expected RouteExpectation) error {
	if capability == nil || capability.GetAuthority() == nil || capability.GetCapabilityId() == "" || len(capability.GetNonce()) != 32 ||
		capability.GetSourceDeviceId() == "" || capability.GetDestinationDeviceId() == "" || capability.GetDestinationExportId() == "" ||
		!validRouteClass(capability.GetRouteClass()) || capability.GetAllowedProtocol() == "" || !strictlySortedStrings(capability.GetAllowedVerbs()) || capability.GetBudget() == nil ||
		capability.GetBudget().GetConnectionLimit() == 0 || capability.GetBudget().GetByteLimit() == 0 || capability.GetBudget().GetConcurrencyLimit() == 0 {
		return ErrRouteUnauthorized
	}
	if proto.Size(capability) > 64<<10 {
		return ErrRouteUnauthorized
	}
	now := manager.now()
	if capability.GetNotBefore() == nil || capability.GetExpiresAt() == nil || now.Before(capability.GetNotBefore().AsTime()) ||
		!now.Before(capability.GetExpiresAt().AsTime()) || capability.GetExpiresAt().AsTime().Sub(capability.GetNotBefore().AsTime()) > maxRouteTTL {
		return ErrRouteUnauthorized
	}
	if (expected.SourceDeviceID != "" && capability.GetSourceDeviceId() != expected.SourceDeviceID) || capability.GetDestinationDeviceId() != expected.DestinationDeviceID ||
		capability.GetDestinationExportId() != expected.DestinationExportID || capability.GetAudience() != expected.Audience ||
		(expected.RouteClass != "" && capability.GetRouteClass() != expected.RouteClass) ||
		capability.GetRouteGeneration() != expected.RouteGeneration || capability.GetPolicyRevision() != expected.PolicyRevision {
		return ErrRouteUnauthorized
	}
	if expected.SourceAllocationID != "" && capability.GetSourceAllocationId() != expected.SourceAllocationID {
		return ErrRouteUnauthorized
	}
	denied, err := manager.store.IsRouteDenied(ctx, capability.GetCapabilityId())
	if err != nil || denied {
		return errors.Join(ErrRouteUnauthorized, err)
	}
	ownerKind, publicKey, err := manager.resolveRouteIssuer(ctx, capability, now)
	if err != nil {
		return errors.Join(ErrRouteUnauthorized, err)
	}
	if capability.GetAuthority().GetAuthorityClass() == trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER && ownerKind != store.TrustOwnerManaged ||
		capability.GetAuthority().GetAuthorityClass() == trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH && ownerKind != store.TrustOwnerPersonalMesh {
		return ErrRouteUnauthorized
	}
	if capability.GetAuthority().GetAuthorityClass() == trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER {
		if err := manager.store.ValidateApplianceRouteAuthority(ctx, capability.GetAuthority().GetAuthorityId(), capability.GetSourceAllocationId(), capability.GetRouteGeneration(), capability.GetPolicyRevision()); err != nil {
			return ErrRouteUnauthorized
		}
	}
	input, err := signing.CanonicalInput(capability)
	if err != nil {
		return errors.Join(ErrRouteUnauthorized, err)
	}
	digest := sha256.Sum256(input)
	if !verifyP256(publicKey, digest[:], capability.GetSignature()) {
		return ErrRouteUnauthorized
	}
	return nil
}

func validRouteClass(value string) bool {
	switch value {
	case "local", "device", "compute", "lan", "public_http":
		return true
	default:
		return false
	}
}

func (manager *Manager) resolveRouteIssuer(ctx context.Context, capability *trustv1.RouteCapability, now time.Time) (int, []byte, error) {
	authority := capability.GetAuthority()
	switch authority.GetAuthorityClass() {
	case trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER:
		if capability.GetIssuerDeviceId() != "" || authority.GetKeyId() != capability.GetIssuerKeyId() {
			return 0, nil, ErrRouteUnauthorized
		}
		owner, anchor, err := manager.store.GetTrustAnchor(ctx, authority.GetAuthorityId())
		if err != nil || anchor.GetRevision() != authority.GetRevision() || anchor.GetNotBefore() == nil || anchor.GetNotAfter() == nil ||
			now.Before(anchor.GetNotBefore().AsTime()) || !now.Before(anchor.GetNotAfter().AsTime()) {
			return 0, nil, ErrRouteUnauthorized
		}
		allowed := false
		for _, kind := range anchor.GetAllowedCapabilityKinds() {
			allowed = allowed || kind == trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE
		}
		if !allowed {
			return 0, nil, ErrRouteUnauthorized
		}
		credential := capability.GetSourceMembershipCredential()
		if credential == nil || credential.GetAuthority() == nil || credential.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER ||
			credential.GetAuthority().GetAuthorityId() != authority.GetAuthorityId() || credential.GetAuthority().GetRevision() != authority.GetRevision() ||
			credential.GetSubjectDeviceId() != capability.GetSourceDeviceId() || credential.GetSubjectRootKeyRevision() == 0 ||
			!validP256PublicKey(credential.GetSubjectRootPublicKey()) ||
			len(credential.GetOpaqueClaimsDigest()) != sha256.Size || credential.GetPolicyRevision() != capability.GetPolicyRevision() ||
			credential.GetExpiresAt() == nil || !now.Before(credential.GetExpiresAt().AsTime()) {
			return 0, nil, ErrRouteUnauthorized
		}
		if local, err := manager.store.GetDeviceIdentity(ctx); err == nil && local.Identity.GetDeviceId() == capability.GetSourceDeviceId() &&
			credential.GetSubjectRootKeyRevision() != local.Identity.GetRootKeyRevision() {
			return 0, nil, ErrRouteUnauthorized
		}
		var credentialKey, issuerKey []byte
		for _, key := range anchor.GetVerificationKeys() {
			if key.GetKeyId() == credential.GetAuthority().GetKeyId() {
				credentialKey = key.GetPublicKey()
			}
			if key.GetKeyId() == capability.GetIssuerKeyId() && key.GetSignatureSuite() == trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
				issuerKey = key.GetPublicKey()
			}
		}
		credentialInput, err := signing.CanonicalInput(credential)
		if err != nil {
			return 0, nil, ErrRouteUnauthorized
		}
		credentialDigest := sha256.Sum256(credentialInput)
		if len(issuerKey) == 0 || !verifyP256(credentialKey, credentialDigest[:], credential.GetSignature()) {
			return 0, nil, ErrRouteUnauthorized
		}
		return owner, issuerKey, nil
	case trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH:
		if capability.GetIssuerDeviceId() != capability.GetDestinationDeviceId() || capability.GetIssuerKeyId() != "" || capability.GetSourceMembershipCredential() != nil {
			return 0, nil, ErrRouteUnauthorized
		}
		meshID := authority.GetAuthorityId()
		if string(authority.GetTrustDomainRef()) != meshID {
			return 0, nil, ErrRouteUnauthorized
		}
		descriptor, snapshot, err := manager.store.GetPersonalMesh(ctx, meshID)
		if err != nil || snapshot.GetRevision() != authority.GetRevision() || snapshot.GetPolicyEpoch() != capability.GetPolicyRevision() ||
			snapshot.GetExpiresAt() == nil || !now.Before(snapshot.GetExpiresAt().AsTime()) {
			return 0, nil, ErrRouteUnauthorized
		}
		if err := manager.verifyRegistryReceipt(ctx, descriptor, snapshot, now); err != nil {
			return 0, nil, ErrRouteUnauthorized
		}
		issuer := activeMember(snapshot, capability.GetIssuerDeviceId())
		source := activeMember(snapshot, capability.GetSourceDeviceId())
		local, err := manager.store.GetDeviceIdentity(ctx)
		if err != nil {
			return 0, nil, err
		}
		for _, member := range []*trustv1.MeshMembership{issuer, source} {
			if !hasPermission(member, trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES) {
				return 0, nil, ErrRouteUnauthorized
			}
			if local.Identity.GetDeviceId() == member.GetDeviceId() &&
				(member.GetRootKeyRevision() != local.Identity.GetRootKeyRevision() || !equalBytes(member.GetRootPublicKey(), local.Identity.GetRootPublicKey())) {
				return 0, nil, ErrRouteUnauthorized
			}
			denied, err := manager.store.IsMeshMemberDenied(ctx, meshID, member.GetDeviceId(), member.GetRootKeyRevision())
			if err != nil || denied {
				return 0, nil, ErrRouteUnauthorized
			}
		}
		return store.TrustOwnerPersonalMesh, issuer.GetRootPublicKey(), nil

	}
	return 0, nil, ErrRouteUnauthorized
}

func (manager *Manager) verifyRegistryReceipt(ctx context.Context, descriptor *trustv1.PersonalMeshDescriptor, snapshot *trustv1.MeshSnapshot, now time.Time) error {
	receipt := snapshot.GetRegistryReceipt()
	if receipt == nil || receipt.GetExpiresAt() == nil || !now.Before(receipt.GetExpiresAt().AsTime()) ||
		receipt.GetRevision() != snapshot.GetRevision() || receipt.GetPolicyEpoch() != snapshot.GetPolicyEpoch() ||
		receipt.GetMeshId() != snapshot.GetMeshId() || receipt.GetRegistryAudience() != descriptor.GetRegistryAudience() ||
		!equalBytes(receipt.GetSnapshotDigest(), snapshot.GetSnapshotDigest()) {
		return ErrRouteUnauthorized
	}
	anchorOwner, registryAnchor, err := manager.store.GetTrustAnchor(ctx, descriptor.GetRegistryAudience())
	if err != nil || anchorOwner != store.TrustOwnerPersonalMesh || registryAnchor.GetNotBefore() == nil || registryAnchor.GetNotAfter() == nil ||
		now.Before(registryAnchor.GetNotBefore().AsTime()) || !now.Before(registryAnchor.GetNotAfter().AsTime()) {
		return ErrRouteUnauthorized
	}
	var receiptKey []byte
	for _, key := range registryAnchor.GetVerificationKeys() {
		if key.GetKeyId() == receipt.GetKeyId() && key.GetSignatureSuite() == trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
			receiptKey = key.GetPublicKey()
		}
	}
	receiptInput, err := signing.CanonicalInput(receipt)
	if err != nil {
		return ErrRouteUnauthorized
	}
	receiptDigest := sha256.Sum256(receiptInput)
	if !verifyP256(receiptKey, receiptDigest[:], receipt.GetSignature()) {
		return ErrRouteUnauthorized
	}
	return nil
}

func activeMember(snapshot *trustv1.MeshSnapshot, id string) *trustv1.MeshMembership {
	for _, member := range snapshot.GetActiveMembers() {
		if member.GetDeviceId() == id && member.GetState() == trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE {
			return member
		}
	}
	return nil
}

func hasPermission(member *trustv1.MeshMembership, permission trustv1.MeshPermission) bool {
	for _, value := range member.GetPermissions() {
		if value == permission {
			return true
		}
	}
	return false
}
