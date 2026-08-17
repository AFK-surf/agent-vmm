package trust

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxRouteTTL = time.Hour

const maxEndpointObservationBytes = 16 << 10

var (
	ErrNeedsRepair       = errors.New("device identity needs explicit repair")
	ErrRouteUnauthorized = errors.New("route capability is unauthorized")
)

type Manager struct {
	store *store.Store
	keys  trustkeys.Provider
	now   func() time.Time
}

func New(ctx context.Context, database *store.Store, keys trustkeys.Provider) (*Manager, error) {
	if database == nil || keys == nil {
		return nil, errors.New("trust store and key provider are required")
	}
	manager := &Manager{store: database, keys: keys, now: time.Now}
	if err := manager.initialize(ctx); err != nil {
		return nil, err
	}
	return manager, nil
}

func (manager *Manager) initialize(ctx context.Context) error {
	record, err := manager.store.GetDeviceIdentity(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		publicKey, custody, err := manager.keys.Ensure(ctx, 1)
		if err != nil {
			return fmt.Errorf("create device root: %w", err)
		}
		deviceID, err := randomIdentifier()
		if err != nil {
			return err
		}
		return manager.store.CreateDeviceIdentity(ctx, store.DeviceIdentityRecord{
			Identity: &trustv1.DeviceIdentity{DeviceId: deviceID, RootPublicKey: publicKey, RootKeyRevision: 1,
				SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256},
			Custody: custody, State: store.DeviceIdentityReady,
		})
	}
	if err != nil {
		return err
	}
	if record.State == store.DeviceIdentityNeedsRepair {
		return nil
	}
	publicKey, _, err := manager.keys.Ensure(ctx, record.Identity.GetRootKeyRevision())
	if err != nil || !equalBytes(publicKey, record.Identity.GetRootPublicKey()) {
		if markErr := manager.store.MarkDeviceIdentityNeedsRepair(ctx, record.Identity.GetRootKeyRevision()); markErr != nil {
			return errors.Join(ErrNeedsRepair, err, markErr)
		}
	}
	return nil
}

func (manager *Manager) Identity(ctx context.Context) (store.DeviceIdentityRecord, error) {
	return manager.store.GetDeviceIdentity(ctx)
}

func (manager *Manager) RotateRoot(ctx context.Context, requestID string, expectedRevision uint64) (*trustv1.DeviceRootRotation, store.DeviceIdentityRecord, error) {
	if requestID == "" {
		return nil, store.DeviceIdentityRecord{}, errors.New("request ID is required")
	}
	if existing, err := manager.store.GetDeviceRootRotation(ctx, requestID); err == nil {
		current, currentErr := manager.store.GetDeviceIdentity(ctx)
		if currentErr != nil {
			return nil, store.DeviceIdentityRecord{}, currentErr
		}
		current.Identity = &trustv1.DeviceIdentity{DeviceId: existing.GetDeviceId(), RootPublicKey: existing.GetNewRootPublicKey(),
			RootKeyRevision: existing.GetNewRootKeyRevision(), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}
		return existing, current, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, store.DeviceIdentityRecord{}, err
	}
	current, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil {
		return nil, store.DeviceIdentityRecord{}, err
	}
	if current.State != store.DeviceIdentityReady || current.Identity.GetRootKeyRevision() != expectedRevision {
		return nil, store.DeviceIdentityRecord{}, ErrNeedsRepair
	}
	newRevision := expectedRevision + 1
	newPublicKey, custody, err := manager.keys.Ensure(ctx, newRevision)
	if err != nil {
		return nil, store.DeviceIdentityRecord{}, err
	}
	rotation := &trustv1.DeviceRootRotation{DeviceId: current.Identity.GetDeviceId(), OldRootKeyRevision: expectedRevision,
		OldRootPublicKey: current.Identity.GetRootPublicKey(), NewRootKeyRevision: newRevision, NewRootPublicKey: newPublicKey,
		RotatedAt: timestamppb.New(manager.now())}
	input, err := signing.CanonicalInput(rotation)
	if err != nil {
		_ = manager.keys.Delete(ctx, newRevision)
		return nil, store.DeviceIdentityRecord{}, err
	}
	digest := sha256.Sum256(input)
	oldSignature, err := manager.keys.Sign(ctx, expectedRevision, digest[:])
	if err != nil {
		_ = manager.store.MarkDeviceIdentityNeedsRepair(ctx, expectedRevision)
		_ = manager.keys.Delete(ctx, newRevision)
		return nil, store.DeviceIdentityRecord{}, ErrNeedsRepair
	}
	newSignature, err := manager.keys.Sign(ctx, newRevision, digest[:])
	if err != nil {
		_ = manager.keys.Delete(ctx, newRevision)
		return nil, store.DeviceIdentityRecord{}, err
	}
	rotation.OldSignsNew = oldSignature
	rotation.NewSignsOld = newSignature
	next := store.DeviceIdentityRecord{Identity: &trustv1.DeviceIdentity{DeviceId: current.Identity.GetDeviceId(), RootPublicKey: newPublicKey,
		RootKeyRevision: newRevision, SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}, Custody: custody,
		State: store.DeviceIdentityReady, EndpointGeneration: current.EndpointGeneration}
	if err := manager.store.RotateDeviceIdentity(ctx, requestID, expectedRevision, next, rotation); err != nil {
		_ = manager.keys.Delete(ctx, newRevision)
		return nil, store.DeviceIdentityRecord{}, err
	}
	_ = manager.keys.Delete(ctx, expectedRevision)
	return rotation, next, nil
}

func (manager *Manager) Repair(ctx context.Context, requestID string, confirmed bool) (store.DeviceIdentityRecord, error) {
	if requestID == "" {
		return store.DeviceIdentityRecord{}, errors.New("request ID is required")
	}
	if repaired, err := manager.store.HasDeviceRepair(ctx, requestID); err != nil {
		return store.DeviceIdentityRecord{}, err
	} else if repaired {
		identity, err := manager.store.GetDeviceRepair(ctx, requestID)
		if err != nil {
			return store.DeviceIdentityRecord{}, err
		}
		return store.DeviceIdentityRecord{Identity: identity, State: store.DeviceIdentityReady}, nil
	}
	if !confirmed {
		return store.DeviceIdentityRecord{}, errors.New("membership reset confirmation is required")
	}
	current, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil {
		return store.DeviceIdentityRecord{}, err
	}
	if current.State != store.DeviceIdentityNeedsRepair {
		return store.DeviceIdentityRecord{}, errors.New("device identity is not awaiting repair")
	}
	newRevision := current.Identity.GetRootKeyRevision() + 1
	publicKey, custody, err := manager.keys.Ensure(ctx, newRevision)
	if err != nil {
		return store.DeviceIdentityRecord{}, err
	}
	deviceID, err := randomIdentifier()
	if err != nil {
		return store.DeviceIdentityRecord{}, err
	}
	next := store.DeviceIdentityRecord{Identity: &trustv1.DeviceIdentity{DeviceId: deviceID, RootPublicKey: publicKey,
		RootKeyRevision: newRevision, SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256},
		Custody: custody, State: store.DeviceIdentityReady}
	if err := manager.store.RepairDeviceIdentity(ctx, requestID, next); err != nil {
		_ = manager.keys.Delete(ctx, newRevision)
		return store.DeviceIdentityRecord{}, err
	}
	return next, nil
}

func (manager *Manager) SignEndpointObservation(ctx context.Context, observation *trustv1.EndpointObservation) (*trustv1.EndpointObservation, error) {
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if record.State != store.DeviceIdentityReady || observation.GetDeviceId() != record.Identity.GetDeviceId() ||
		observation.GetRootKeyRevision() != record.Identity.GetRootKeyRevision() || observation.GetEndpointGeneration() < record.EndpointGeneration ||
		len(observation.GetEndpointNodeId()) == 0 || observation.GetExpiresAt() == nil || !observation.GetExpiresAt().AsTime().After(manager.now()) ||
		observation.GetExpiresAt().AsTime().Sub(manager.now()) > 15*time.Minute || !strictlySortedStrings(observation.GetSupportedAlpns()) ||
		!strictlySortedStrings(observation.GetFeatureSet()) {
		return nil, ErrRouteUnauthorized
	}
	if observation.GetEndpointGeneration() == record.EndpointGeneration {
		current, currentErr := manager.store.GetEndpointObservationAny(ctx, observation.GetDeviceId())
		if currentErr != nil || !sameEndpointIdentity(current, observation) || current.GetExpiresAt() == nil ||
			!observation.GetExpiresAt().AsTime().After(current.GetExpiresAt().AsTime()) {
			return nil, ErrRouteUnauthorized
		}
	}
	result := proto.Clone(observation).(*trustv1.EndpointObservation)
	if proto.Size(result) > maxEndpointObservationBytes {
		return nil, errors.New("endpoint observation exceeds size limit")
	}
	input, err := signing.CanonicalInput(result)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	result.DeviceSignature, err = manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, err
	}
	if err := manager.store.PutEndpointObservation(ctx, result, record.EndpointGeneration); err != nil {
		return nil, err
	}
	return result, nil
}

func (manager *Manager) ApplyMeshEndpointObservation(ctx context.Context, meshID string, observation *trustv1.EndpointObservation) (*trustv1.EndpointObservation, error) {
	if meshID == "" || observation == nil || observation.GetDeviceId() == "" || observation.GetEndpointGeneration() == 0 ||
		len(observation.GetEndpointNodeId()) == 0 || observation.GetExpiresAt() == nil || !observation.GetExpiresAt().AsTime().After(manager.now()) ||
		observation.GetExpiresAt().AsTime().Sub(manager.now()) > 15*time.Minute || !strictlySortedStrings(observation.GetSupportedAlpns()) ||
		!strictlySortedStrings(observation.GetFeatureSet()) || proto.Size(observation) > maxEndpointObservationBytes {
		return nil, ErrRouteUnauthorized
	}
	_, snapshot, err := manager.store.GetPersonalMesh(ctx, meshID)
	if err != nil || snapshot.GetExpiresAt() == nil || !snapshot.GetExpiresAt().AsTime().After(manager.now()) {
		return nil, ErrRouteUnauthorized
	}
	var publicKey []byte
	for _, member := range snapshot.GetActiveMembers() {
		if member.GetDeviceId() == observation.GetDeviceId() && member.GetRootKeyRevision() == observation.GetRootKeyRevision() &&
			member.GetState() == trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE {
			publicKey = member.GetRootPublicKey()
			break
		}
	}
	if len(publicKey) == 0 {
		return nil, ErrRouteUnauthorized
	}
	if current, currentErr := manager.store.GetEndpointObservationAny(ctx, observation.GetDeviceId()); currentErr == nil &&
		observation.GetEndpointGeneration() == current.GetEndpointGeneration() &&
		(!sameEndpointIdentity(current, observation) || current.GetExpiresAt() == nil ||
			!observation.GetExpiresAt().AsTime().After(current.GetExpiresAt().AsTime())) {
		return nil, ErrRouteUnauthorized
	}
	unsigned := proto.Clone(observation).(*trustv1.EndpointObservation)
	unsigned.DeviceSignature = nil
	input, err := signing.CanonicalInput(unsigned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	if !verifyP256(publicKey, digest[:], observation.GetDeviceSignature()) {
		return nil, ErrRouteUnauthorized
	}
	if err := manager.store.PutRemoteEndpointObservation(ctx, observation); err != nil {
		return nil, err
	}
	return proto.Clone(observation).(*trustv1.EndpointObservation), nil
}

func sameEndpointIdentity(left, right *trustv1.EndpointObservation) bool {
	return left != nil && right != nil && left.GetDeviceId() == right.GetDeviceId() &&
		left.GetRootKeyRevision() == right.GetRootKeyRevision() &&
		left.GetEndpointGeneration() == right.GetEndpointGeneration() &&
		equalBytes(left.GetEndpointNodeId(), right.GetEndpointNodeId()) &&
		equalBytes(left.GetObservedAddressesDigest(), right.GetObservedAddressesDigest()) &&
		slices.Equal(left.GetSupportedAlpns(), right.GetSupportedAlpns()) &&
		slices.Equal(left.GetFeatureSet(), right.GetFeatureSet())
}

func (manager *Manager) GetEndpointObservation(ctx context.Context, deviceID string) (*trustv1.EndpointObservation, error) {
	if deviceID == "" {
		return nil, ErrRouteUnauthorized
	}
	return manager.store.GetEndpointObservation(ctx, deviceID)
}

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

// InstallManagedTrust accepts only a bundle delivered through the
// TLS-authenticated, one-time controller enrollment flow. It verifies the
// controller-signed device credential before atomically advancing local facts.
func (manager *Manager) InstallManagedTrust(ctx context.Context, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	if err := manager.validateManagedTrust(ctx, anchor, credential); err != nil {
		return err
	}
	return manager.store.InstallManagedTrust(ctx, anchor, credential)
}

func (manager *Manager) InstallApplianceManagedTrust(ctx context.Context, binding store.ApplianceTrustAuthority, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	if err := manager.validateManagedTrust(ctx, anchor, credential); err != nil {
		return err
	}
	return manager.store.InstallApplianceManagedTrust(ctx, binding, anchor, credential)
}

func (manager *Manager) validateManagedTrust(ctx context.Context, anchor *trustv1.TrustAnchor, credential *trustv1.MembershipCredential) error {
	now := manager.now()
	identity, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || identity.State != store.DeviceIdentityReady || anchor == nil || credential == nil ||
		anchor.GetAuthority().GetAuthorityClass() != trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER ||
		anchor.GetAuthority().GetAuthorityId() == "" || anchor.GetAuthority().GetRevision() != anchor.GetRevision() ||
		anchor.GetRevision() == 0 || anchor.GetNotBefore() == nil || anchor.GetNotAfter() == nil ||
		anchor.GetNotBefore().AsTime().After(now) || !anchor.GetNotAfter().AsTime().After(now) ||
		!proto.Equal(anchor.GetAuthority(), credential.GetAuthority()) || credential.GetSubjectDeviceId() != identity.Identity.GetDeviceId() ||
		credential.GetSubjectRootKeyRevision() != identity.Identity.GetRootKeyRevision() ||
		!validP256PublicKey(credential.GetSubjectRootPublicKey()) ||
		!equalBytes(credential.GetSubjectRootPublicKey(), identity.Identity.GetRootPublicKey()) || credential.GetPolicyRevision() == 0 ||
		len(credential.GetOpaqueClaimsDigest()) != sha256.Size || credential.GetExpiresAt() == nil ||
		!credential.GetExpiresAt().AsTime().After(now) || credential.GetExpiresAt().AsTime().After(anchor.GetNotAfter().AsTime()) {
		return ErrRouteUnauthorized
	}
	allowed := false
	for _, kind := range anchor.GetAllowedCapabilityKinds() {
		allowed = allowed || kind == trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE
	}
	if !allowed || len(anchor.GetVerificationKeys()) == 0 || len(anchor.GetVerificationKeys()) > 8 {
		return ErrRouteUnauthorized
	}
	var verificationKey []byte
	for _, key := range anchor.GetVerificationKeys() {
		if key.GetKeyId() == anchor.GetAuthority().GetKeyId() && key.GetSignatureSuite() == trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
			verificationKey = key.GetPublicKey()
		}
	}
	input, err := signing.CanonicalInput(credential)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(input)
	if len(verificationKey) != 33 || !verifyP256(verificationKey, digest[:], credential.GetSignature()) {
		return ErrRouteUnauthorized
	}
	return nil
}

func (manager *Manager) IssuePersonalRoute(ctx context.Context, request *servicev1.PersonalRouteRequest, destination *servicev1.ServiceExport) (*servicev1.PersonalRouteResponse, error) {
	now := manager.now()
	budget := request.GetBudget()
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetSourceDeviceId() == "" || request.GetSourceRootKeyRevision() == 0 ||
		destination == nil || destination.GetRevoked() ||
		request.GetDestinationExportId() != destination.GetExportId() || request.GetRouteGeneration() == 0 || request.GetPolicyEpoch() == 0 ||
		budget.GetConnectionLimit() == 0 || budget.GetConcurrencyLimit() == 0 || budget.GetByteLimit() == 0 || len(request.GetNonce()) != 32 ||
		request.GetExpiresAt() == nil || !request.GetExpiresAt().AsTime().After(now) || request.GetExpiresAt().AsTime().Sub(now) > maxRouteTTL {
		return nil, ErrRouteUnauthorized
	}
	if request.GetRouteClass() == servicev1.RouteClass_ROUTE_CLASS_COMPUTE {
		if request.GetSourceEnvironmentId() == "" || request.GetSourceContainerInstanceId() == "" {
			return nil, ErrRouteUnauthorized
		}
	} else if request.GetRouteClass() == servicev1.RouteClass_ROUTE_CLASS_DEVICE || request.GetRouteClass() == servicev1.RouteClass_ROUTE_CLASS_LAN {
		if request.GetSourceEnvironmentId() != "" || request.GetSourceContainerInstanceId() != "" {
			return nil, ErrRouteUnauthorized
		}
	} else {
		return nil, ErrRouteUnauthorized
	}
	descriptor, snapshot, err := manager.store.GetPersonalMesh(ctx, request.GetMeshId())
	if err != nil || snapshot.GetPolicyEpoch() != request.GetPolicyEpoch() || snapshot.GetExpiresAt() == nil || !snapshot.GetExpiresAt().AsTime().After(now) ||
		manager.verifyRegistryReceipt(ctx, descriptor, snapshot, now) != nil {
		return nil, ErrRouteUnauthorized
	}
	identity, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || identity.State != store.DeviceIdentityReady {
		return nil, ErrRouteUnauthorized
	}
	var source *trustv1.MeshMembership
	localActive := false
	for _, member := range snapshot.GetActiveMembers() {
		allowed := false
		for _, permission := range member.GetPermissions() {
			allowed = allowed || permission == trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES
		}
		if member.GetDeviceId() == request.GetSourceDeviceId() && member.GetRootKeyRevision() == request.GetSourceRootKeyRevision() && allowed {
			source = member
		}
		if member.GetDeviceId() == identity.Identity.GetDeviceId() && member.GetRootKeyRevision() == identity.Identity.GetRootKeyRevision() &&
			equalBytes(member.GetRootPublicKey(), identity.Identity.GetRootPublicKey()) && allowed {
			localActive = true
		}
	}
	if source == nil || !localActive {
		return nil, ErrRouteUnauthorized
	}
	denied, err := manager.store.IsMeshMemberDenied(ctx, request.GetMeshId(), source.GetDeviceId(), source.GetRootKeyRevision())
	if err != nil || denied {
		return nil, ErrRouteUnauthorized
	}
	input, err := signing.CanonicalInput(request)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	if !verifyP256(source.GetRootPublicKey(), digest[:], request.GetSourceSignature()) {
		return nil, ErrRouteUnauthorized
	}
	// A durable response makes a lost result retryable, but it is not an
	// authorization cache. Re-check the signed request, current snapshot,
	// permissions, local deny, and export fence above before returning it.
	if existing, err := manager.store.GetPersonalRouteIssuance(ctx, request); err == nil {
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	authority := &trustv1.AuthorityDescriptor{AuthorityId: request.GetMeshId(), AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH,
		TrustDomainRef: []byte(request.GetMeshId()), Revision: snapshot.GetRevision()}
	capability := &trustv1.RouteCapability{CapabilityId: "mesh-route-" + request.GetRequestId(), Authority: authority,
		SourceDeviceId: request.GetSourceDeviceId(), DestinationDeviceId: identity.Identity.GetDeviceId(), DestinationExportId: destination.GetExportId(),
		AllowedProtocol: "tcp", AllowedVerbs: []string{"connect"}, RouteGeneration: request.GetRouteGeneration(), PolicyRevision: snapshot.GetPolicyEpoch(),
		RouteClass: routeClassValue(request.GetRouteClass()),
		Budget:     proto.Clone(budget).(*trustv1.RouteBudget), NotBefore: timestamppb.New(now), ExpiresAt: request.GetExpiresAt(), Nonce: append([]byte(nil), request.GetNonce()...),
		Audience: "agent-vmm/service-tunnel/1", IssuerDeviceId: identity.Identity.GetDeviceId()}
	capabilityInput, err := signing.CanonicalInput(capability)
	if err != nil {
		return nil, err
	}
	capabilityDigest := sha256.Sum256(capabilityInput)
	capability.Signature, err = manager.keys.Sign(ctx, identity.Identity.GetRootKeyRevision(), capabilityDigest[:])
	if err != nil {
		return nil, err
	}
	response := &servicev1.PersonalRouteResponse{Capability: capability, DestinationExport: proto.Clone(destination).(*servicev1.ServiceExport)}
	if err := manager.store.PutPersonalRouteIssuance(ctx, request, response); err != nil {
		if existing, getErr := manager.store.GetPersonalRouteIssuance(ctx, request); getErr == nil {
			return existing, nil
		}
		return nil, err
	}
	return response, nil
}

func routeClassValue(value servicev1.RouteClass) string {
	switch value {
	case servicev1.RouteClass_ROUTE_CLASS_LOCAL:
		return "local"
	case servicev1.RouteClass_ROUTE_CLASS_DEVICE:
		return "device"
	case servicev1.RouteClass_ROUTE_CLASS_COMPUTE:
		return "compute"
	case servicev1.RouteClass_ROUTE_CLASS_LAN:
		return "lan"
	case servicev1.RouteClass_ROUTE_CLASS_PUBLIC_HTTP:
		return "public_http"
	default:
		return ""
	}
}

func (manager *Manager) SignPersonalRouteRequest(ctx context.Context, request *servicev1.PersonalRouteRequest) (*servicev1.PersonalRouteRequest, error) {
	identity, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || identity.State != store.DeviceIdentityReady || request == nil || request.GetMeshId() == "" {
		return nil, ErrRouteUnauthorized
	}
	_, snapshot, err := manager.store.GetPersonalMesh(ctx, request.GetMeshId())
	if err != nil || snapshot.GetExpiresAt() == nil || !snapshot.GetExpiresAt().AsTime().After(manager.now()) {
		return nil, ErrRouteUnauthorized
	}
	active := false
	for _, member := range snapshot.GetActiveMembers() {
		if member.GetDeviceId() == identity.Identity.GetDeviceId() && member.GetRootKeyRevision() == identity.Identity.GetRootKeyRevision() &&
			equalBytes(member.GetRootPublicKey(), identity.Identity.GetRootPublicKey()) {
			for _, permission := range member.GetPermissions() {
				active = active || permission == trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES
			}
		}
	}
	if !active {
		return nil, ErrRouteUnauthorized
	}
	result := proto.Clone(request).(*servicev1.PersonalRouteRequest)
	result.SourceDeviceId = identity.Identity.GetDeviceId()
	result.SourceRootKeyRevision = identity.Identity.GetRootKeyRevision()
	result.PolicyEpoch = snapshot.GetPolicyEpoch()
	input, err := signing.CanonicalInput(result)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(input)
	result.SourceSignature, err = manager.keys.Sign(ctx, identity.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, err
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
	managerAuthorized := false
	found := false
	var revokedRootRevision uint64
	for _, member := range current.GetActiveMembers() {
		if member.GetDeviceId() == record.Identity.GetDeviceId() {
			for _, permission := range member.GetPermissions() {
				managerAuthorized = managerAuthorized || permission == trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS
			}
		}
		if member.GetDeviceId() == deviceID {
			found = true
			revokedRootRevision = member.GetRootKeyRevision()
			continue
		}
	}
	if !managerAuthorized || !found {
		return nil, nil, nil, ErrRouteUnauthorized
	}
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

func (manager *Manager) CreatePersonalMeshJoin(ctx context.Context, requestID, inviteID string, nonce []byte, proof *trustv1.PairingProof) (*trustv1.RegistryOperation, error) {
	if requestID == "" || inviteID == "" || len(nonce) != 32 || proof == nil {
		return nil, ErrRouteUnauthorized
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
	managerAuthorized := false
	for _, member := range snapshot.GetActiveMembers() {
		if member.GetDeviceId() == record.Identity.GetDeviceId() &&
			member.GetRootKeyRevision() == record.Identity.GetRootKeyRevision() &&
			equalBytes(member.GetRootPublicKey(), record.Identity.GetRootPublicKey()) {
			for _, permission := range member.GetPermissions() {
				managerAuthorized = managerAuthorized || permission == trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS
			}
		}
	}
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
		Permissions: append([]trustv1.MeshPermission(nil), proof.GetPermissions()...), State: trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE}
	operation := &trustv1.RegistryOperation{OperationId: requestID, Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_JOIN,
		MeshId: proof.GetMeshId(), ExpectedRevision: proof.GetInviteRevision(), PolicyEpoch: snapshot.GetPolicyEpoch(),
		IssuerDeviceId: record.Identity.GetDeviceId(), IssuerRootKeyRevision: record.Identity.GetRootKeyRevision(), Membership: membership,
		InviteId: inviteID, ExpiresAt: proof.GetExpiresAt(), Nonce: append([]byte(nil), nonce...), PairingProof: pairingProof}
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

func randomIdentifier() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
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

func verifyP256(publicKey, digest, signature []byte) bool {
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), publicKey)
	if x == nil || y == nil || len(signature) != 64 {
		return false
	}
	r, s := new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	return s.Sign() > 0 && s.Cmp(halfOrder) <= 0 && ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest, r, s)
}

func validP256PublicKey(publicKey []byte) bool {
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), publicKey)
	return len(publicKey) == 33 && x != nil && y != nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func strictlySortedStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}
