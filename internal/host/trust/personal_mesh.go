package trust

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"slices"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (manager *Manager) ListPersonalMeshes(ctx context.Context) ([]*trustv1.PersonalMeshState, error) {
	records, err := manager.store.ListPersonalMeshes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*trustv1.PersonalMeshState, 0, len(records))
	for _, record := range records {
		result = append(result, &trustv1.PersonalMeshState{
			MeshDescriptor: proto.Clone(record.Descriptor).(*trustv1.PersonalMeshDescriptor),
			Snapshot:       proto.Clone(record.Snapshot).(*trustv1.MeshSnapshot),
		})
	}
	return result, nil
}

func (manager *Manager) CreatePersonalMesh(ctx context.Context, requestID, registryAudience string) (*trustv1.PersonalMeshDescriptor, *trustv1.MeshSnapshot, *trustv1.RegistryOperation, error) {
	if requestID == "" {
		return nil, nil, nil, errors.New("request ID is required")
	}
	if existing, err := manager.store.GetRegistryOperation(ctx, requestID); err == nil {
		if existing.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS {
			return nil, nil, nil, errors.New("request ID belongs to another registry operation")
		}
		descriptor, snapshot, err := manager.store.GetPersonalMesh(ctx, existing.GetMeshId())
		return descriptor, snapshot, existing, err
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, err
	}
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady {
		return nil, nil, nil, errors.Join(ErrNeedsRepair, err)
	}
	meshID, err := randomIdentifier()
	if err != nil {
		return nil, nil, nil, err
	}
	now := manager.now()
	descriptor := &trustv1.PersonalMeshDescriptor{MeshId: meshID, GenesisDeviceId: record.Identity.GetDeviceId(),
		GenesisRootPublicKey: record.Identity.GetRootPublicKey(), RegistryAudience: registryAudience, PolicyEpoch: 1,
		CreatedAt: timestamppb.New(now)}
	input, err := signing.CanonicalInput(descriptor)
	if err != nil {
		return nil, nil, nil, err
	}
	digest := sha256.Sum256(input)
	descriptor.GenesisSignature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, nil, nil, err
	}
	membership := &trustv1.MeshMembership{MeshId: meshID, DeviceId: record.Identity.GetDeviceId(), RootPublicKey: record.Identity.GetRootPublicKey(),
		RootKeyRevision: record.Identity.GetRootKeyRevision(), Permissions: []trustv1.MeshPermission{
			trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES, trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS},
		State: trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE, JoinedAtRevision: 1, OperationDigest: digest[:]}
	snapshot := &trustv1.MeshSnapshot{MeshId: meshID, Revision: 1, PolicyEpoch: 1, ActiveMembers: []*trustv1.MeshMembership{membership},
		IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(15 * time.Minute))}
	snapshot.SnapshotDigest = snapshotDigest(snapshot)
	operation := &trustv1.RegistryOperation{OperationId: requestID, Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS,
		MeshId: meshID, PolicyEpoch: 1, IssuerDeviceId: record.Identity.GetDeviceId(), IssuerRootKeyRevision: record.Identity.GetRootKeyRevision(),
		Membership: proto.Clone(membership).(*trustv1.MeshMembership), ExpiresAt: timestamppb.New(now.Add(15 * time.Minute))}
	operationInput, err := signing.CanonicalInput(operation)
	if err != nil {
		return nil, nil, nil, err
	}
	operationDigest := sha256.Sum256(operationInput)
	operation.Signature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), operationDigest[:])
	if err != nil {
		return nil, nil, nil, err
	}
	if err := manager.store.CreatePersonalMesh(ctx, descriptor, snapshot, operation); err != nil {
		return nil, nil, nil, err
	}
	return descriptor, snapshot, operation, nil
}

func (manager *Manager) ApplyPersonalMeshSnapshot(ctx context.Context, snapshot *trustv1.MeshSnapshot, expectedRevision uint64) error {
	_, current, currentErr := manager.store.GetPersonalMesh(ctx, snapshot.GetMeshId())
	if currentErr == nil && current.GetRevision() == snapshot.GetRevision() && equalBytes(current.GetSnapshotDigest(), snapshot.GetSnapshotDigest()) {
		return nil
	}
	if err := validateSnapshot(snapshot, manager.now()); err != nil {
		return err
	}
	descriptor, _, err := manager.store.GetPersonalMesh(ctx, snapshot.GetMeshId())
	if err != nil {
		return err
	}
	if err := manager.verifyRegistryReceipt(ctx, descriptor, snapshot, manager.now()); err != nil {
		return err
	}
	return manager.store.ApplyPersonalMeshSnapshot(ctx, snapshot, expectedRevision)
}

func (manager *Manager) PendingRegistryOperations(ctx context.Context, afterSequence uint64, limit uint32) ([]store.PendingRegistryOperation, error) {
	return manager.store.ListPendingRegistryOperations(ctx, afterSequence, limit)
}

func (manager *Manager) CommitRegistryOperation(ctx context.Context, operationID string, snapshot *trustv1.MeshSnapshot) error {
	operation, err := manager.store.GetPendingRegistryOperation(ctx, operationID)
	if err != nil {
		return err
	}
	if operation.GetMeshId() != snapshot.GetMeshId() || operation.GetMembership() == nil ||
		(operation.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS && snapshot.GetRevision() <= operation.GetExpectedRevision()) ||
		(operation.GetKind() == trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS && snapshot.GetRevision() != 1) {
		return store.ErrTrustRevisionConflict
	}
	input, err := signing.CanonicalInput(operation)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(input)
	if operation.GetKind() == trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE {
		tombstoneFound := false
		for _, tombstone := range snapshot.GetTombstones() {
			if tombstone.GetDeviceId() == operation.GetMembership().GetDeviceId() &&
				tombstone.GetRootKeyRevision() == operation.GetMembership().GetRootKeyRevision() && equalBytes(tombstone.GetOperationDigest(), digest[:]) {
				tombstoneFound = true
			}
		}
		if !tombstoneFound {
			return errors.New("registry snapshot does not commit the pending revoke")
		}
	}
	if err := validateSnapshot(snapshot, manager.now()); err != nil {
		return err
	}
	descriptor, _, err := manager.store.GetPersonalMesh(ctx, snapshot.GetMeshId())
	if err != nil {
		return err
	}
	if err := manager.verifyRegistryReceipt(ctx, descriptor, snapshot, manager.now()); err != nil {
		return err
	}
	return manager.store.CommitRegistryOperation(ctx, operationID, snapshot)
}

func (manager *Manager) RevokePersonalMeshMember(ctx context.Context, requestID, meshID, deviceID string, expectedRevision uint64) (*trustv1.PersonalMeshDescriptor, *trustv1.MeshSnapshot, *trustv1.RegistryOperation, error) {
	if requestID == "" {
		return nil, nil, nil, errors.New("request ID is required")
	}
	if existing, err := manager.store.GetRegistryOperation(ctx, requestID); err == nil {
		if existing.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE {
			return nil, nil, nil, errors.New("request ID belongs to another registry operation")
		}
		descriptor, snapshot, err := manager.store.GetPersonalMesh(ctx, existing.GetMeshId())
		return descriptor, snapshot, existing, err
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, err
	}
	descriptor, current, err := manager.store.GetPersonalMesh(ctx, meshID)
	if err != nil {
		return nil, nil, nil, err
	}
	if current.GetRevision() != expectedRevision {
		return nil, nil, nil, store.ErrTrustRevisionConflict
	}
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady {
		return nil, nil, nil, errors.Join(ErrNeedsRepair, err)
	}
	local := activeMember(current, record.Identity.GetDeviceId())
	revoked := activeMember(current, deviceID)
	if !hasPermission(local, trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS) || revoked == nil {
		return nil, nil, nil, ErrRouteUnauthorized
	}
	revokedRootRevision := revoked.GetRootKeyRevision()
	now := manager.now()
	operation := &trustv1.RegistryOperation{OperationId: requestID, Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE,
		MeshId: meshID, ExpectedRevision: expectedRevision, PolicyEpoch: current.GetPolicyEpoch() + 1,
		IssuerDeviceId: record.Identity.GetDeviceId(), IssuerRootKeyRevision: record.Identity.GetRootKeyRevision(),
		Membership: &trustv1.MeshMembership{MeshId: meshID, DeviceId: deviceID, RootKeyRevision: revokedRootRevision,
			State: trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_REVOKED}, ExpiresAt: timestamppb.New(now.Add(15 * time.Minute))}
	input, err := signing.CanonicalInput(operation)
	if err != nil {
		return nil, nil, nil, err
	}
	digest := sha256.Sum256(input)
	operation.Signature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, nil, nil, err
	}
	if err := manager.store.QueueRegistryOperation(ctx, operation); err != nil {
		return nil, nil, nil, err
	}
	return descriptor, current, operation, nil
}

func (manager *Manager) SignPairingProof(ctx context.Context, proof *trustv1.PairingProof) (*trustv1.PairingProof, error) {
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady || proof == nil || len(proof.GetSessionId()) != 16 ||
		proof.GetMeshId() == "" || proof.GetInviteRevision() == 0 || len(proof.GetContextDigest()) != sha256.Size ||
		proof.GetExpiresAt() == nil || !proof.GetExpiresAt().AsTime().After(manager.now()) {
		return nil, ErrRouteUnauthorized
	}
	result := proto.Clone(proof).(*trustv1.PairingProof)
	result.Role = trustv1.PairingRole_PAIRING_ROLE_JOINER
	result.DeviceId = record.Identity.GetDeviceId()
	result.RootKeyRevision = record.Identity.GetRootKeyRevision()
	result.RootPublicKey = append([]byte(nil), record.Identity.GetRootPublicKey()...)
	result.Signature = nil
	input, err := signing.CanonicalInput(result)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	result.Signature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	return result, err
}

func (manager *Manager) CreatePersonalMeshJoin(ctx context.Context, requestID string, invitation *hostv1.PersonalMeshPairingInvitation, peerIdentity, peerRootKey []byte, proof *trustv1.PairingProof) (*trustv1.RegistryOperation, error) {
	if requestID == "" || invitation.GetInviteId() == "" || len(invitation.GetNonce()) != 32 || proof == nil {
		return nil, ErrRouteUnauthorized
	}
	contextDigest, contextErr := PairingContext(invitation, peerIdentity, peerRootKey).Digest()
	if contextErr != nil || !equalBytes(proof.GetSessionId(), invitation.GetSessionId()) ||
		proof.GetMeshId() != invitation.GetMeshId() || proof.GetInviteRevision() != invitation.GetInviteRevision() ||
		!slices.Equal(proof.GetPermissions(), invitation.GetPermissions()) || len(invitation.GetPermissions()) == 0 ||
		proof.GetDeviceId() != string(peerIdentity) || !equalBytes(proof.GetRootPublicKey(), peerRootKey) ||
		!equalBytes(proof.GetContextDigest(), contextDigest[:]) || !proto.Equal(proof.GetExpiresAt(), invitation.GetExpiresAt()) {
		return nil, ErrPairingContextMismatch
	}
	if existing, err := manager.store.GetRegistryOperation(ctx, requestID); err == nil {
		if existing.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_JOIN {
			return nil, errors.New("request ID belongs to another registry operation")
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	_, snapshot, err := manager.store.GetPersonalMesh(ctx, proof.GetMeshId())
	if err != nil || snapshot.GetRevision() != proof.GetInviteRevision() {
		return nil, store.ErrTrustRevisionConflict
	}
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady {
		return nil, ErrNeedsRepair
	}
	managerAuthorized := CanManageMesh(snapshot, record.Identity)
	now := manager.now()
	proofInput, canonicalErr := signing.CanonicalInput(proof)
	proofDigest := sha256.Sum256(proofInput)
	if !managerAuthorized || canonicalErr != nil || proof.GetRole() != trustv1.PairingRole_PAIRING_ROLE_JOINER ||
		len(proof.GetSessionId()) != 16 || len(proof.GetContextDigest()) != sha256.Size || !validP256PublicKey(proof.GetRootPublicKey()) ||
		proof.GetExpiresAt() == nil || !proof.GetExpiresAt().AsTime().After(now) || proof.GetExpiresAt().AsTime().Sub(now) > 15*time.Minute ||
		!verifyP256(proof.GetRootPublicKey(), proofDigest[:], proof.GetSignature()) {
		return nil, ErrRouteUnauthorized
	}
	pairingProof, err := proto.MarshalOptions{Deterministic: true}.Marshal(proof)
	if err != nil {
		return nil, err
	}
	membership := &trustv1.MeshMembership{MeshId: proof.GetMeshId(), DeviceId: proof.GetDeviceId(),
		RootPublicKey: append([]byte(nil), proof.GetRootPublicKey()...), RootKeyRevision: proof.GetRootKeyRevision(),
		Permissions: append([]trustv1.MeshPermission(nil), invitation.GetPermissions()...), State: trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE}
	operation := &trustv1.RegistryOperation{OperationId: requestID, Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_JOIN,
		MeshId: proof.GetMeshId(), ExpectedRevision: proof.GetInviteRevision(), PolicyEpoch: snapshot.GetPolicyEpoch(),
		IssuerDeviceId: record.Identity.GetDeviceId(), IssuerRootKeyRevision: record.Identity.GetRootKeyRevision(), Membership: membership,
		InviteId: invitation.GetInviteId(), ExpiresAt: proof.GetExpiresAt(), Nonce: append([]byte(nil), invitation.GetNonce()...), PairingProof: pairingProof}
	input, err := signing.CanonicalInput(operation)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	operation.Signature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, err
	}
	if err := manager.store.QueueRegistryOperation(ctx, operation); err != nil {
		return nil, err
	}
	return operation, nil
}

func snapshotDigest(snapshot *trustv1.MeshSnapshot) []byte {
	cloned := proto.Clone(snapshot).(*trustv1.MeshSnapshot)
	cloned.SnapshotDigest = nil
	cloned.RegistryReceipt = nil
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
	digest := sha256.Sum256(payload)
	return digest[:]
}

func validateSnapshot(snapshot *trustv1.MeshSnapshot, now time.Time) error {
	if snapshot == nil || snapshot.GetMeshId() == "" || snapshot.GetRevision() == 0 || snapshot.GetPolicyEpoch() == 0 ||
		snapshot.GetExpiresAt() == nil || !snapshot.GetExpiresAt().AsTime().After(now) || len(snapshot.GetActiveMembers()) > 32 || len(snapshot.GetTombstones()) > 32 ||
		!equalBytes(snapshot.GetSnapshotDigest(), snapshotDigest(snapshot)) {
		return errors.New("invalid or stale personal mesh snapshot")
	}
	tombstoned := make(map[string]uint64, len(snapshot.GetTombstones()))
	for _, tombstone := range snapshot.GetTombstones() {
		if tombstone.GetDeviceId() == "" || tombstoned[tombstone.GetDeviceId()] != 0 {
			return errors.New("snapshot contains duplicate or invalid tombstones")
		}
		tombstoned[tombstone.GetDeviceId()] = tombstone.GetRootKeyRevision()
	}
	active := make(map[string]struct{}, len(snapshot.GetActiveMembers()))
	for _, member := range snapshot.GetActiveMembers() {
		if _, duplicate := active[member.GetDeviceId()]; duplicate || member.GetDeviceId() == "" || member.GetRootKeyRevision() == 0 {
			return errors.New("snapshot contains duplicate or invalid active members")
		}
		active[member.GetDeviceId()] = struct{}{}
		if member.GetMeshId() != snapshot.GetMeshId() || member.GetState() != trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE ||
			(tombstoned[member.GetDeviceId()] != 0 && member.GetRootKeyRevision() <= tombstoned[member.GetDeviceId()]) {
			return errors.New("snapshot violates remove-wins membership")
		}
	}
	return nil
}

var ErrPairingContextMismatch = errors.New("pairing proof does not match the authenticated invitation")

func PairingContext(invitation *hostv1.PersonalMeshPairingInvitation, peerIdentity, peerRootKey []byte) dataworker.PairingContext {
	permissions := make([]byte, len(invitation.GetPermissions()))
	for index, permission := range invitation.GetPermissions() {
		permissions[index] = byte(permission)
	}
	return dataworker.PairingContext{SessionID: invitation.GetSessionId(), MeshID: []byte(invitation.GetMeshId()),
		InviteRevision: invitation.GetInviteRevision(), ManagerIdentity: []byte(invitation.GetManagerDeviceId()),
		ManagerRootKey: invitation.GetManagerRootPublicKey(), JoinerIdentity: peerIdentity, JoinerRootKey: peerRootKey,
		RolesAndFeatures: permissions}
}

func CanManageMesh(snapshot *trustv1.MeshSnapshot, identity *trustv1.DeviceIdentity) bool {
	member := activeMember(snapshot, identity.GetDeviceId())
	return hasPermission(member, trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS) &&
		member.GetRootKeyRevision() == identity.GetRootKeyRevision() && equalBytes(member.GetRootPublicKey(), identity.GetRootPublicKey())
}
