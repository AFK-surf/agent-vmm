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
	"sync"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/keychain"
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
	identityMu sync.Mutex
	store      *store.Store
	keys       trustkeys.Provider
	now        func() time.Time
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
	manager.identityMu.Lock()
	defer manager.identityMu.Unlock()
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
	if errors.Is(err, keychain.ErrUnavailable) {
		return err
	}
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

func (manager *Manager) Repair(ctx context.Context, requestID string, confirmed bool) (store.DeviceIdentityRecord, error) {
	manager.identityMu.Lock()
	defer manager.identityMu.Unlock()
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
		_ = manager.discardUncommittedKey(ctx, newRevision)
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

func (manager *Manager) IssuePersonalRoute(ctx context.Context, request *servicev1.RequestPersonalRouteRequest, destination *servicev1.ServiceExport) (*servicev1.RequestPersonalRouteResponse, error) {
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
	source := activeMember(snapshot, request.GetSourceDeviceId())
	local := activeMember(snapshot, identity.Identity.GetDeviceId())
	if source.GetRootKeyRevision() != request.GetSourceRootKeyRevision() ||
		!hasPermission(source, trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES) ||
		!hasPermission(local, trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES) ||
		local.GetRootKeyRevision() != identity.Identity.GetRootKeyRevision() || !equalBytes(local.GetRootPublicKey(), identity.Identity.GetRootPublicKey()) {
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
	response := &servicev1.RequestPersonalRouteResponse{Capability: capability, DestinationExport: proto.Clone(destination).(*servicev1.ServiceExport)}
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

func (manager *Manager) SignPersonalRouteRequest(ctx context.Context, request *servicev1.RequestPersonalRouteRequest) (*servicev1.RequestPersonalRouteRequest, error) {
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
	result := proto.Clone(request).(*servicev1.RequestPersonalRouteRequest)
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

func randomIdentifier() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
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

func (manager *Manager) discardUncommittedKey(ctx context.Context, revision uint64) error {
	current, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil {
		return err
	}
	if current.Identity.GetRootKeyRevision() == revision {
		return nil
	}
	return manager.keys.Delete(ctx, revision)
}
