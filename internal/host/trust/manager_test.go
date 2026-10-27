package trust

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/testfixture"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newMemoryKeys(t *testing.T) *testfixture.MemoryKeys {
	t.Helper()
	return testfixture.NewMemoryKeys()
}

var signDigest = testfixture.SignDigest

func openManager(t *testing.T, provider *testfixture.MemoryKeys) (*Manager, *store.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := New(context.Background(), database, provider)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return manager, database, path
}

func TestDeviceRootCustodyAndExplicitRepair(t *testing.T) {
	ctx := context.Background()
	provider := newMemoryKeys(t)
	manager, database, path := openManager(t, provider)
	identity, err := manager.Identity(ctx)
	if err != nil || identity.State != store.DeviceIdentityReady || identity.Identity.GetRootKeyRevision() != 1 {
		t.Fatalf("initial identity = %+v err=%v", identity, err)
	}
	privateScalar := provider.Keys[1].D.Bytes()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	databaseBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if containsBytes(databaseBytes, privateScalar) {
		t.Fatal("SQLite contains device root private scalar")
	}
	database, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	manager.store = database
	next := identity
	managedAuthority := &trustv1.AuthorityDescriptor{AuthorityId: "managed-before-repair",
		AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, KeyId: "controller", Revision: 1}
	credential := &trustv1.MembershipCredential{Authority: managedAuthority, SubjectDeviceId: next.Identity.GetDeviceId(),
		SubjectRootKeyRevision: next.Identity.GetRootKeyRevision(), SubjectRootPublicKey: next.Identity.GetRootPublicKey(), OpaqueClaimsDigest: make([]byte, 32), PolicyRevision: 1,
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)), Signature: make([]byte, 64)}
	if err := database.PutMembershipCredential(ctx, credential, 0); err != nil {
		t.Fatal(err)
	}
	meshBeforeRepair, _, _, err := manager.CreatePersonalMesh(ctx, "mesh-before-repair", "registry.test")
	if err != nil {
		t.Fatal(err)
	}
	provider.Unavailable[1] = true
	if err := manager.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := manager.Identity(ctx)
	if err != nil || record.State != store.DeviceIdentityNeedsRepair {
		t.Fatalf("lost root state = %+v err=%v", record, err)
	}
	if _, err := manager.Repair(ctx, "repair-1", false); err == nil {
		t.Fatal("repair succeeded without explicit membership reset confirmation")
	}
	provider.Unavailable[2] = false
	repaired, err := manager.Repair(ctx, "repair-1", true)
	if err != nil || repaired.State != store.DeviceIdentityReady || repaired.Identity.GetDeviceId() == identity.Identity.GetDeviceId() {
		t.Fatalf("repaired identity = %+v err=%v", repaired, err)
	}
	if replayedRepair, err := manager.Repair(ctx, "repair-1", true); err != nil || !proto.Equal(repaired.Identity, replayedRepair.Identity) {
		t.Fatalf("idempotent repair=%+v err=%v", replayedRepair, err)
	}
	if _, _, err := database.GetPersonalMesh(ctx, meshBeforeRepair.GetMeshId()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("mesh membership survived root repair err=%v", err)
	}
	if _, err := database.GetMembershipCredential(ctx, managedAuthority.GetAuthorityId(), next.Identity.GetDeviceId()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("managed credential survived root repair err=%v", err)
	}
	if operations, err := manager.PendingRegistryOperations(ctx, 0, 32); err != nil || len(operations) != 0 {
		t.Fatalf("pending registry operations survived root repair operations=%+v err=%v", operations, err)
	}
}

func TestPersonalMeshCASRemoveWinsAndFreshness(t *testing.T) {
	ctx := context.Background()
	manager, database, databasePath := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	descriptor, genesis, genesisOperation, err := manager.CreatePersonalMesh(ctx, "create-mesh-1", "registry.test")
	if err != nil {
		t.Fatal(err)
	}
	if records, err := database.ListPersonalMeshes(ctx); err != nil || len(records) != 1 || records[0].Descriptor.GetMeshId() != descriptor.GetMeshId() || records[0].Snapshot.GetRevision() != genesis.GetRevision() {
		t.Fatalf("personal mesh list records=%+v err=%v", records, err)
	}
	if replayedDescriptor, _, replayedOperation, err := manager.CreatePersonalMesh(ctx, "create-mesh-1", "registry.test"); err != nil || replayedDescriptor.GetMeshId() != descriptor.GetMeshId() || replayedOperation.GetOperationId() != genesisOperation.GetOperationId() {
		t.Fatalf("idempotent mesh create descriptor=%+v operation=%+v err=%v", replayedDescriptor, replayedOperation, err)
	}
	deviceB := testKey(t)
	deviceC := testKey(t)
	registryKey := testKey(t)
	if err := database.PutTrustAnchor(ctx, store.TrustOwnerPersonalMesh,
		anchor("registry.test", trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH, "receipt-key", registryKey, now), 0); err != nil {
		t.Fatal(err)
	}
	attachReceipt(t, genesis, descriptor.GetRegistryAudience(), registryKey, now)
	if err := manager.CommitRegistryOperation(ctx, genesisOperation.GetOperationId(), genesis); err != nil {
		t.Fatal(err)
	}
	revision2 := nextSnapshot(genesis, now, member(descriptor.GetMeshId(), "device-b", deviceB, 2))
	attachReceipt(t, revision2, descriptor.GetRegistryAudience(), registryKey, now)
	if err := manager.ApplyPersonalMeshSnapshot(ctx, revision2, 1); err != nil {
		t.Fatal(err)
	}
	remoteEndpoint := &trustv1.EndpointObservation{DeviceId: "device-b", RootKeyRevision: 1,
		EndpointNodeId: []byte("device-b-iroh-endpoint"), EndpointGeneration: 1,
		SupportedAlpns: []string{"agent-vmm/service-tunnel/1"}, FeatureSet: []string{"direct", "relay"},
		ExpiresAt: timestamppb.New(now.Add(10 * time.Minute))}
	signMessage(t, deviceB, remoteEndpoint)
	if applied, err := manager.ApplyMeshEndpointObservation(ctx, descriptor.GetMeshId(), remoteEndpoint); err != nil || !proto.Equal(applied, remoteEndpoint) {
		t.Fatalf("apply mesh endpoint=%+v err=%v", applied, err)
	}
	if loaded, err := manager.GetEndpointObservation(ctx, "device-b"); err != nil || !proto.Equal(loaded, remoteEndpoint) {
		t.Fatalf("loaded mesh endpoint=%+v err=%v", loaded, err)
	}
	remoteRenewal := proto.Clone(remoteEndpoint).(*trustv1.EndpointObservation)
	remoteRenewal.ExpiresAt = timestamppb.New(now.Add(11 * time.Minute))
	signMessage(t, deviceB, remoteRenewal)
	if _, err := manager.ApplyMeshEndpointObservation(ctx, descriptor.GetMeshId(), remoteRenewal); err != nil {
		t.Fatalf("same-generation endpoint renewal failed: %v", err)
	}
	forgedEndpoint := proto.Clone(remoteEndpoint).(*trustv1.EndpointObservation)
	forgedEndpoint.EndpointGeneration++
	forgedEndpoint.DeviceSignature[0] ^= 0xff
	if _, err := manager.ApplyMeshEndpointObservation(ctx, descriptor.GetMeshId(), forgedEndpoint); err == nil {
		t.Fatal("forged mesh endpoint observation was accepted")
	}
	personalRequest := &servicev1.RequestPersonalRouteRequest{RequestId: "personal-route-1", MeshId: descriptor.GetMeshId(), SourceDeviceId: "device-b",
		SourceRootKeyRevision: 1, SourceEnvironmentId: "source-env", SourceContainerInstanceId: "source-instance",
		DestinationExportId: "export-local", RouteGeneration: 9, PolicyEpoch: revision2.GetPolicyEpoch(),
		RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE,
		Budget:     &trustv1.RouteBudget{ConnectionLimit: 2, ConcurrencyLimit: 1, ByteLimit: 4096}, ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)), Nonce: []byte("0123456789abcdef0123456789abcdef")}
	signMessage(t, deviceB, personalRequest)
	destination := &servicev1.ServiceExport{ExportId: "export-local", EnvironmentId: "destination-env", ContainerId: "api", ContainerInstanceId: "api-v1", Port: 8443, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, Revision: 1}
	issued, err := manager.IssuePersonalRoute(ctx, personalRequest, destination)
	if err != nil || issued.GetCapability().GetIssuerDeviceId() == "" || issued.GetCapability().GetDestinationExportId() != "export-local" {
		t.Fatalf("issued personal route=%+v err=%v", issued, err)
	}
	if replayed, err := manager.IssuePersonalRoute(ctx, personalRequest, destination); err != nil || !proto.Equal(replayed, issued) {
		t.Fatalf("idempotent personal route=%+v err=%v", replayed, err)
	}
	forgedReplay := proto.Clone(personalRequest).(*servicev1.RequestPersonalRouteRequest)
	forgedReplay.SourceSignature[0] ^= 0xff
	if _, err := manager.IssuePersonalRoute(ctx, forgedReplay, destination); err == nil {
		t.Fatal("durable personal route response bypassed request signature verification")
	}
	if err := manager.AuthorizeRoute(ctx, issued.GetCapability(), routeExpectation(issued.GetCapability(), now)); err != nil {
		t.Fatalf("destination-signed personal route was not verifiable: %v", err)
	}
	for index, routeClass := range []servicev1.RouteClass{
		servicev1.RouteClass_ROUTE_CLASS_DEVICE,
		servicev1.RouteClass_ROUTE_CLASS_LAN,
	} {
		request := proto.Clone(personalRequest).(*servicev1.RequestPersonalRouteRequest)
		request.RequestId = fmt.Sprintf("personal-listener-%d", index)
		request.RouteGeneration = uint64(10 + index)
		request.RouteClass = routeClass
		request.SourceEnvironmentId = ""
		request.SourceContainerInstanceId = ""
		nonce := sha256.Sum256([]byte(request.RequestId))
		request.Nonce = nonce[:]
		request.SourceSignature = nil
		signMessage(t, deviceB, request)
		listenerRoute, err := manager.IssuePersonalRoute(ctx, request, destination)
		if err != nil || listenerRoute.GetCapability().GetRouteClass() != routeClassValue(routeClass) {
			t.Fatalf("listener class %s route=%+v err=%v", routeClass, listenerRoute, err)
		}
		if err := manager.AuthorizeRoute(ctx, listenerRoute.GetCapability(), routeExpectation(listenerRoute.GetCapability(), now)); err != nil {
			t.Fatalf("listener class %s capability rejected: %v", routeClass, err)
		}
	}
	invalidDevice := proto.Clone(personalRequest).(*servicev1.RequestPersonalRouteRequest)
	invalidDevice.RequestId = "personal-device-with-container"
	invalidDevice.RouteClass = servicev1.RouteClass_ROUTE_CLASS_DEVICE
	invalidDevice.SourceSignature = nil
	signMessage(t, deviceB, invalidDevice)
	if _, err := manager.IssuePersonalRoute(ctx, invalidDevice, destination); err == nil {
		t.Fatal("DEVICE route accepted a source container scope")
	}
	revision3 := nextSnapshot(revision2, now, member(descriptor.GetMeshId(), "device-c", deviceC, 3))
	attachReceipt(t, revision3, descriptor.GetRegistryAudience(), registryKey, now)
	if err := manager.ApplyPersonalMeshSnapshot(ctx, revision3, 2); err != nil {
		t.Fatal(err)
	}
	if err := manager.ApplyPersonalMeshSnapshot(ctx, revision3, 2); err != nil {
		t.Fatalf("idempotent duplicate CAS err=%v", err)
	}
	conflict := proto.Clone(revision3).(*trustv1.MeshSnapshot)
	conflict.SnapshotDigest = []byte("different")
	if err := manager.ApplyPersonalMeshSnapshot(ctx, conflict, 2); err == nil {
		t.Fatal("conflicting snapshot at current revision was accepted")
	}
	_, unchanged, pending, err := manager.RevokePersonalMeshMember(ctx, "revoke-b", descriptor.GetMeshId(), "device-b", 3)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.GetRevision() != 3 || pending.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE {
		t.Fatalf("offline revoke snapshot=%d pending=%+v", unchanged.GetRevision(), pending)
	}
	if _, _, replayedPending, err := manager.RevokePersonalMeshMember(ctx, "revoke-b", descriptor.GetMeshId(), "device-b", 3); err != nil || !proto.Equal(pending, replayedPending) {
		t.Fatalf("idempotent revoke=%+v err=%v", replayedPending, err)
	}
	denied, err := database.IsMeshMemberDenied(ctx, descriptor.GetMeshId(), "device-b", 1)
	if err != nil || !denied {
		t.Fatalf("offline local deny=%t err=%v", denied, err)
	}
	if _, err := manager.IssuePersonalRoute(ctx, personalRequest, destination); err == nil {
		t.Fatal("durable personal route response bypassed current local deny")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	manager.store = database
	recovered, err := manager.PendingRegistryOperations(ctx, 0, 32)
	if err != nil || len(recovered) != 1 || recovered[0].Operation.GetOperationId() != pending.GetOperationId() {
		t.Fatalf("recovered pending operations=%+v err=%v", recovered, err)
	}
	committed := proto.Clone(revision3).(*trustv1.MeshSnapshot)
	committed.Revision = 4
	committed.PolicyEpoch++
	committed.PreviousDigest = revision3.GetSnapshotDigest()
	committed.ActiveMembers = []*trustv1.MeshMembership{committed.ActiveMembers[0], committed.ActiveMembers[2]}
	pendingInput, err := signing.CanonicalInput(pending)
	if err != nil {
		t.Fatal(err)
	}
	pendingDigest := sha256.Sum256(pendingInput)
	committed.Tombstones = append(committed.Tombstones, &trustv1.MeshTombstone{DeviceId: "device-b", RootKeyRevision: 1,
		RevokedAtRevision: 4, PolicyEpoch: committed.PolicyEpoch, OperationDigest: pendingDigest[:]})
	committed.SnapshotDigest = snapshotDigest(committed)
	attachReceipt(t, committed, descriptor.GetRegistryAudience(), registryKey, now)
	if err := manager.CommitRegistryOperation(ctx, pending.GetOperationId(), committed); err != nil {
		t.Fatal(err)
	}
	resurrected := nextSnapshot(committed, now, member(descriptor.GetMeshId(), "device-b", deviceB, 5))
	if err := manager.ApplyPersonalMeshSnapshot(ctx, resurrected, 4); err == nil {
		t.Fatal("remove-wins tombstone allowed revoked member to rejoin")
	}
	_, current, err := database.GetPersonalMesh(ctx, descriptor.GetMeshId())
	if err != nil || len(current.GetActiveMembers()) != 2 || current.GetRevision() != 4 {
		t.Fatalf("A→B→C then revoke B snapshot=%+v err=%v", current, err)
	}
	stale := proto.Clone(current).(*trustv1.MeshSnapshot)
	stale.Revision++
	stale.ExpiresAt = timestamppb.New(now.Add(-time.Second))
	stale.SnapshotDigest = snapshotDigest(stale)
	if err := manager.ApplyPersonalMeshSnapshot(ctx, stale, 4); err == nil {
		t.Fatal("expired snapshot was accepted")
	}
}

func TestPairingProofRequiresJoinerRootAndQueuesManagerCAS(t *testing.T) {
	ctx := context.Background()
	manager, _, _ := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_100_000_000, 0)
	manager.now = func() time.Time { return now }
	descriptor, snapshot, _, err := manager.CreatePersonalMesh(ctx, "pairing-mesh", "registry.test")
	if err != nil {
		t.Fatal(err)
	}
	joiner := testKey(t)
	proof := &trustv1.PairingProof{SessionId: []byte("0123456789abcdef"), MeshId: descriptor.GetMeshId(),
		InviteRevision: snapshot.GetRevision(), Role: trustv1.PairingRole_PAIRING_ROLE_JOINER, DeviceId: "joiner",
		RootKeyRevision: 1, RootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), joiner.X, joiner.Y),
		Permissions:   []trustv1.MeshPermission{trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES},
		ContextDigest: make([]byte, sha256.Size), ExpiresAt: timestamppb.New(now.Add(10 * time.Minute))}
	identity, err := manager.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	invitation := &hostv1.PersonalMeshPairingInvitation{SessionId: proof.GetSessionId(), MeshId: proof.GetMeshId(), InviteRevision: proof.GetInviteRevision(),
		InviteId: "invite", Nonce: make([]byte, 32), ManagerDeviceId: identity.Identity.GetDeviceId(), ManagerRootPublicKey: identity.Identity.GetRootPublicKey(),
		Permissions: proof.GetPermissions(), ExpiresAt: proof.GetExpiresAt()}
	contextDigest, err := PairingContext(invitation, []byte(proof.GetDeviceId()), proof.GetRootPublicKey()).Digest()
	if err != nil {
		t.Fatal(err)
	}
	proof.ContextDigest = contextDigest[:]
	signMessage(t, joiner, proof)
	operation, err := manager.CreatePersonalMeshJoin(ctx, "join-operation", invitation, []byte(proof.GetDeviceId()), proof.GetRootPublicKey(), proof)
	if err != nil {
		t.Fatal(err)
	}
	if operation.GetKind() != trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_JOIN || len(operation.GetPairingProof()) == 0 ||
		operation.GetMembership().GetDeviceId() != "joiner" || len(operation.GetSignature()) != 64 {
		t.Fatalf("join operation = %+v", operation)
	}
	for name, change := range map[string]func(*trustv1.PairingProof){
		"session": func(value *trustv1.PairingProof) { value.SessionId = []byte("differentSession") },
		"context": func(value *trustv1.PairingProof) { value.ContextDigest[0] ^= 1 },
		"permissions": func(value *trustv1.PairingProof) {
			value.Permissions = append(value.Permissions, trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS)
		},
		"identity": func(value *trustv1.PairingProof) { value.DeviceId = "attacker" },
		"root":     func(value *trustv1.PairingProof) { value.RootPublicKey = make([]byte, 33) },
	} {
		t.Run(name, func(t *testing.T) {
			forged := proto.Clone(proof).(*trustv1.PairingProof)
			change(forged)
			signMessage(t, joiner, forged)
			if _, err := manager.CreatePersonalMeshJoin(ctx, "forged-"+name, invitation, []byte(proof.GetDeviceId()), proof.GetRootPublicKey(), forged); !errors.Is(err, ErrPairingContextMismatch) {
				t.Fatalf("bound proof error = %v", err)
			}
		})
	}
}

func TestEndpointObservationFencesRootAndGeneration(t *testing.T) {
	ctx := context.Background()
	manager, _, _ := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	record, err := manager.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	observation := &trustv1.EndpointObservation{DeviceId: record.Identity.GetDeviceId(), RootKeyRevision: 1,
		EndpointNodeId: []byte("independent-iroh-endpoint-key"), EndpointGeneration: 1,
		SupportedAlpns: []string{"agent-vmm/control/1", "agent-vmm/data/1"}, FeatureSet: []string{"direct", "relay"},
		ExpiresAt: timestamppb.New(now.Add(10 * time.Minute))}
	signed, err := manager.SignEndpointObservation(ctx, observation)
	if err != nil || len(signed.GetDeviceSignature()) != 64 {
		t.Fatalf("signed observation=%+v err=%v", signed, err)
	}
	if _, err := manager.SignEndpointObservation(ctx, observation); err == nil {
		t.Fatal("same-generation endpoint was renewed without advancing expiry")
	}
	renewal := proto.Clone(observation).(*trustv1.EndpointObservation)
	renewal.ExpiresAt = timestamppb.New(now.Add(11 * time.Minute))
	if renewed, err := manager.SignEndpointObservation(ctx, renewal); err != nil || renewed.GetEndpointGeneration() != observation.GetEndpointGeneration() {
		t.Fatalf("same endpoint generation renewal=%+v err=%v", renewed, err)
	}
	mutatedRenewal := proto.Clone(renewal).(*trustv1.EndpointObservation)
	mutatedRenewal.ExpiresAt = timestamppb.New(now.Add(12 * time.Minute))
	mutatedRenewal.EndpointNodeId = []byte("different-endpoint-with-same-generation")
	if _, err := manager.SignEndpointObservation(ctx, mutatedRenewal); err == nil {
		t.Fatal("same endpoint generation changed transport identity")
	}
	staleRoot := proto.Clone(observation).(*trustv1.EndpointObservation)
	staleRoot.EndpointGeneration = 2
	staleRoot.RootKeyRevision = 9
	if _, err := manager.SignEndpointObservation(ctx, staleRoot); err == nil {
		t.Fatal("stale/foreign root signed endpoint observation")
	}
}

func TestManagedEnrollmentTrustInstallIsAtomicAndRevisionFenced(t *testing.T) {
	ctx := context.Background()
	manager, database, _ := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	identity, err := manager.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(t)
	managedAnchor := anchor("managed-enrollment", trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, "managed-key", key, now)
	managedAnchor.NotAfter = timestamppb.New(now.Add(3 * time.Hour))
	claims := sha256.Sum256([]byte("registration"))
	credential := &trustv1.MembershipCredential{Authority: proto.Clone(managedAnchor.GetAuthority()).(*trustv1.AuthorityDescriptor),
		SubjectDeviceId: identity.Identity.GetDeviceId(), SubjectRootKeyRevision: identity.Identity.GetRootKeyRevision(),
		SubjectRootPublicKey: identity.Identity.GetRootPublicKey(),
		OpaqueClaimsDigest:   claims[:], PolicyRevision: 1, ExpiresAt: timestamppb.New(now.Add(time.Hour))}
	signMessage(t, key, credential)
	if err := manager.InstallManagedTrust(ctx, managedAnchor, credential); err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallManagedTrust(ctx, managedAnchor, credential); err != nil {
		t.Fatalf("idempotent managed trust install failed: %v", err)
	}
	replacementClaims := sha256.Sum256([]byte("replacement-registration"))
	replacement := proto.Clone(credential).(*trustv1.MembershipCredential)
	replacement.OpaqueClaimsDigest = replacementClaims[:]
	replacement.ExpiresAt = timestamppb.New(now.Add(2 * time.Hour))
	signMessage(t, key, replacement)
	if err := manager.InstallManagedTrust(ctx, managedAnchor, replacement); err == nil {
		t.Fatal("unexpired same-revision managed credential was replaced")
	}
	forged := proto.Clone(credential).(*trustv1.MembershipCredential)
	forged.Signature[0] ^= 0xff
	if err := manager.InstallManagedTrust(ctx, managedAnchor, forged); err == nil {
		t.Fatal("forged same-revision managed credential was installed")
	}
	stored, err := database.GetMembershipCredential(ctx, managedAnchor.GetAuthority().GetAuthorityId(), identity.Identity.GetDeviceId())
	if err != nil || !proto.Equal(stored, credential) {
		t.Fatalf("stored credential changed after rejected install: %+v err=%v", stored, err)
	}
	manager.now = func() time.Time { return credential.GetExpiresAt().AsTime() }
	if err := manager.InstallManagedTrust(ctx, managedAnchor, replacement); err != nil {
		t.Fatalf("expired same-revision managed credential was not replaced: %v", err)
	}
	if err := manager.InstallManagedTrust(ctx, managedAnchor, replacement); err != nil {
		t.Fatalf("replacement was not byte-identically idempotent: %v", err)
	}
	stored, err = database.GetMembershipCredential(ctx, managedAnchor.GetAuthority().GetAuthorityId(), identity.Identity.GetDeviceId())
	if err != nil || !proto.Equal(stored, replacement) {
		t.Fatalf("expired credential replacement was not stored: %+v err=%v", stored, err)
	}
}

func TestRouteVerifierSeparatesManagedAndPersonalAuthorities(t *testing.T) {
	ctx := context.Background()
	manager, database, _ := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	managedKey := testKey(t)
	managedAnchor := anchor("managed-a", trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, "managed-key", managedKey, now)
	if err := database.PutTrustAnchor(ctx, store.TrustOwnerManaged, managedAnchor, 0); err != nil {
		t.Fatal(err)
	}
	credential := &trustv1.MembershipCredential{Authority: proto.Clone(managedAnchor.GetAuthority()).(*trustv1.AuthorityDescriptor),
		SubjectDeviceId: "source", SubjectRootKeyRevision: 1, SubjectRootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), managedKey.X, managedKey.Y), OpaqueClaimsDigest: make([]byte, 32), PolicyRevision: 1,
		ExpiresAt: timestamppb.New(now.Add(time.Hour))}
	signMessage(t, managedKey, credential)
	if err := database.PutMembershipCredential(ctx, credential, 0); err != nil {
		t.Fatal(err)
	}
	managed := route("managed-route", managedAnchor.GetAuthority(), "managed-key", "", now)
	managed.SourceMembershipCredential = proto.Clone(credential).(*trustv1.MembershipCredential)
	signMessage(t, managedKey, managed)
	expected := routeExpectation(managed, now)
	if err := manager.AuthorizeRoute(ctx, managed, expected); err != nil {
		t.Fatalf("managed route rejected: %v", err)
	}
	crossClass := proto.Clone(managed).(*trustv1.RouteCapability)
	crossClass.Authority.AuthorityClass = trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH
	signMessage(t, managedKey, crossClass)
	if err := manager.AuthorizeRoute(ctx, crossClass, expected); err == nil {
		t.Fatal("managed key crossed into personal-mesh authority")
	}

	descriptor, genesis, genesisOperation, err := manager.CreatePersonalMesh(ctx, "create-mesh-2", "registry.test")
	if err != nil {
		t.Fatal(err)
	}
	registryKey := testKey(t)
	registryAnchor := anchor("registry.test", trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH, "receipt-key", registryKey, now)
	if err := database.PutTrustAnchor(ctx, store.TrustOwnerPersonalMesh, registryAnchor, 0); err != nil {
		t.Fatal(err)
	}
	attachReceipt(t, genesis, descriptor.GetRegistryAudience(), registryKey, now)
	if err := manager.CommitRegistryOperation(ctx, genesisOperation.GetOperationId(), genesis); err != nil {
		t.Fatal(err)
	}
	issuerKey := testKey(t)
	sourceWithoutServices := member(descriptor.GetMeshId(), "source-no-services", testKey(t), 2)
	sourceWithoutServices.Permissions = []trustv1.MeshPermission{trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS}
	destinationWithoutServices := member(descriptor.GetMeshId(), "destination-no-services", issuerKey, 2)
	destinationWithoutServices.Permissions = []trustv1.MeshPermission{trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS}
	current := nextSnapshot(genesis, now, member(descriptor.GetMeshId(), "issuer", issuerKey, 2), member(descriptor.GetMeshId(), "source", testKey(t), 2), sourceWithoutServices, destinationWithoutServices)
	current.RegistryReceipt = &trustv1.RegistryReceipt{RegistryAudience: "registry.test", MeshId: descriptor.GetMeshId(), Revision: current.GetRevision(),
		PolicyEpoch: current.GetPolicyEpoch(), SnapshotDigest: current.GetSnapshotDigest(), IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)), KeyId: "receipt-key"}
	signMessage(t, registryKey, current.RegistryReceipt)
	if err := manager.ApplyPersonalMeshSnapshot(ctx, current, 1); err != nil {
		t.Fatal(err)
	}
	authority := &trustv1.AuthorityDescriptor{AuthorityId: descriptor.GetMeshId(), AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH,
		TrustDomainRef: []byte(descriptor.GetMeshId()), Revision: current.GetRevision()}
	personal := route("personal-route", authority, "", "issuer", now)
	personal.DestinationDeviceId = "issuer"
	personal.PolicyRevision = current.GetPolicyEpoch()
	signMessage(t, issuerKey, personal)
	if err := manager.AuthorizeRoute(ctx, personal, routeExpectation(personal, now)); err != nil {
		t.Fatalf("personal route rejected: %v", err)
	}
	for _, change := range []struct {
		name  string
		apply func(*trustv1.RouteCapability)
	}{
		{"non-destination issuer", func(value *trustv1.RouteCapability) { value.DestinationDeviceId = "source" }},
		{"absent source", func(value *trustv1.RouteCapability) { value.SourceDeviceId = "absent" }},
		{"source without service permission", func(value *trustv1.RouteCapability) { value.SourceDeviceId = "source-no-services" }},
		{"destination without service permission", func(value *trustv1.RouteCapability) {
			value.IssuerDeviceId = "destination-no-services"
			value.DestinationDeviceId = "destination-no-services"
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			value := proto.Clone(personal).(*trustv1.RouteCapability)
			change.apply(value)
			signMessage(t, issuerKey, value)
			if err := manager.AuthorizeRoute(ctx, value, routeExpectation(value, now)); err == nil {
				t.Fatal("unauthorized personal route accepted")
			}
		})
	}
	wrongMesh := proto.Clone(personal).(*trustv1.RouteCapability)
	wrongMesh.Authority.TrustDomainRef = []byte("another-mesh")
	signMessage(t, issuerKey, wrongMesh)
	if err := manager.AuthorizeRoute(ctx, wrongMesh, routeExpectation(wrongMesh, now)); err == nil {
		t.Fatal("issuer membership crossed into another mesh")
	}
	personal.ExpiresAt = timestamppb.New(now.Add(20 * time.Minute))
	signMessage(t, issuerKey, personal)
	manager.now = func() time.Time { return now.Add(11 * time.Minute) }
	if err := manager.AuthorizeRoute(ctx, personal, routeExpectation(personal, now.Add(11*time.Minute))); err == nil {
		t.Fatal("expired registry freshness receipt authorized a route")
	}
	if err := database.DenyRoute(ctx, managed.GetCapabilityId(), "draining"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AuthorizeRoute(ctx, managed, expected); err == nil {
		t.Fatal("local STOPPING/drain deny did not fence managed route")
	}
}

func anchor(id string, class trustv1.AuthorityClass, keyID string, key *ecdsa.PrivateKey, now time.Time) *trustv1.TrustAnchor {
	authorityKeyID := ""
	if class == trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER {
		authorityKeyID = keyID
	}
	return &trustv1.TrustAnchor{Authority: &trustv1.AuthorityDescriptor{AuthorityId: id, AuthorityClass: class, KeyId: authorityKeyID, Revision: 1},
		VerificationKeys: []*trustv1.VerificationKey{{KeyId: keyID, PublicKey: elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y),
			SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}},
		AllowedCapabilityKinds: []trustv1.CapabilityKind{trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE, trustv1.CapabilityKind_CAPABILITY_KIND_MESH_CONTROL},
		NotBefore:              timestamppb.New(now.Add(-time.Minute)), NotAfter: timestamppb.New(now.Add(time.Hour)), Revision: 1}
}

func route(id string, authority *trustv1.AuthorityDescriptor, keyID, issuerDeviceID string, now time.Time) *trustv1.RouteCapability {
	return &trustv1.RouteCapability{CapabilityId: id, Authority: proto.Clone(authority).(*trustv1.AuthorityDescriptor), SourceDeviceId: "source",
		DestinationDeviceId: "destination", DestinationExportId: "export", AllowedProtocol: "tcp", RouteGeneration: 7, PolicyRevision: 1,
		RouteClass: "compute",
		Budget:     &trustv1.RouteBudget{ConnectionLimit: 2, ByteLimit: 1024, ConcurrencyLimit: 1}, NotBefore: timestamppb.New(now.Add(-time.Second)),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), Nonce: []byte("0123456789abcdef0123456789abcdef"), Audience: "service.test", IssuerKeyId: keyID,
		IssuerDeviceId: issuerDeviceID}
}

func routeExpectation(value *trustv1.RouteCapability, now time.Time) RouteExpectation {
	return RouteExpectation{SourceDeviceID: value.GetSourceDeviceId(), DestinationDeviceID: value.GetDestinationDeviceId(),
		DestinationExportID: value.GetDestinationExportId(), RouteClass: value.GetRouteClass(), Audience: value.GetAudience(), RouteGeneration: value.GetRouteGeneration(),
		PolicyRevision: value.GetPolicyRevision()}
}

func signMessage(t *testing.T, key *ecdsa.PrivateKey, message proto.Message) {
	t.Helper()
	input, err := signing.CanonicalInput(message)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(input)
	signature, err := signDigest(key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	switch value := message.(type) {
	case *trustv1.RouteCapability:
		value.Signature = signature
	case *trustv1.RegistryReceipt:
		value.Signature = signature
	case *trustv1.MembershipCredential:
		value.Signature = signature
	case *trustv1.EndpointObservation:
		value.DeviceSignature = signature
	case *trustv1.PairingProof:
		value.Signature = signature
	case *servicev1.RequestPersonalRouteRequest:
		value.SourceSignature = signature
	default:
		t.Fatalf("unsupported signed test message %T", message)
	}
}

func attachReceipt(t *testing.T, snapshot *trustv1.MeshSnapshot, audience string, key *ecdsa.PrivateKey, now time.Time) {
	t.Helper()
	snapshot.RegistryReceipt = &trustv1.RegistryReceipt{RegistryAudience: audience, MeshId: snapshot.GetMeshId(), Revision: snapshot.GetRevision(),
		PolicyEpoch: snapshot.GetPolicyEpoch(), SnapshotDigest: snapshot.GetSnapshotDigest(), IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)), KeyId: "receipt-key"}
	signMessage(t, key, snapshot.RegistryReceipt)
}

func testKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func member(meshID, deviceID string, key *ecdsa.PrivateKey, revision uint64) *trustv1.MeshMembership {
	return &trustv1.MeshMembership{MeshId: meshID, DeviceId: deviceID, RootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y),
		RootKeyRevision: 1, Permissions: []trustv1.MeshPermission{trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES},
		State: trustv1.MeshMembershipState_MESH_MEMBERSHIP_STATE_ACTIVE, JoinedAtRevision: revision, OperationDigest: []byte{byte(revision)}}
}

func nextSnapshot(current *trustv1.MeshSnapshot, now time.Time, added ...*trustv1.MeshMembership) *trustv1.MeshSnapshot {
	next := proto.Clone(current).(*trustv1.MeshSnapshot)
	next.Revision++
	next.PreviousDigest = current.GetSnapshotDigest()
	next.ActiveMembers = append(next.ActiveMembers, added...)
	next.IssuedAt = timestamppb.New(now)
	next.ExpiresAt = timestamppb.New(now.Add(15 * time.Minute))
	next.RegistryReceipt = nil
	next.SnapshotDigest = snapshotDigest(next)
	return next
}

func containsBytes(value, search []byte) bool {
	for index := 0; index+len(search) <= len(value); index++ {
		if equalBytes(value[index:index+len(search)], search) {
			return true
		}
	}
	return false
}

func TestApplianceManagedTrustRenewalValidatesHostSignatureAndLifetime(t *testing.T) {
	ctx := context.Background()
	manager, database, _ := openManager(t, newMemoryKeys(t))
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	identity, err := manager.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "artifact", ProductID: "product", CanonicalHash: []byte("hash"), ImageDigest: "digest", ImageReference: "image", Platform: "linux/arm64", Release: "1", Manifest: []byte("manifest")}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx,
		store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: 1, Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20},
		store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1},
		store.RemoteRegistration{ID: "registration", InstallationID: "installation", OwnerKind: store.RemoteRegistrationPrivate, DisplayName: "private", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1}, nil,
		store.ApplianceOperation{ID: "install", RequestID: "install", State: 1, Phase: 2})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = 3
	installation.CurrentBootID = "boot"
	installation.CurrentContainerInstance = "instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(t)
	makeBundle := func(revision uint64) (*trustv1.TrustAnchor, *trustv1.MembershipCredential) {
		a := anchor("managed-appliance", trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, "managed-key", key, now)
		a.Revision = revision
		a.Authority.Revision = revision
		a.NotAfter = timestamppb.New(now.Add(365 * 24 * time.Hour))
		claims := sha256.Sum256([]byte("registration"))
		c := &trustv1.MembershipCredential{Authority: proto.Clone(a.Authority).(*trustv1.AuthorityDescriptor), SubjectDeviceId: identity.Identity.GetDeviceId(), SubjectRootKeyRevision: identity.Identity.GetRootKeyRevision(), SubjectRootPublicKey: identity.Identity.GetRootPublicKey(), OpaqueClaimsDigest: claims[:], PolicyRevision: revision, ExpiresAt: timestamppb.New(now.Add(30 * 24 * time.Hour))}
		signMessage(t, key, c)
		return a, c
	}
	binding := store.ApplianceTrustAuthority{AuthorityID: "managed-appliance", InstallationID: "installation", RegistrationID: "registration", BootID: "boot", ContainerInstance: "instance", ConfigRevision: installation.Revision}
	for _, test := range []struct {
		name   string
		reason string
		change func(*trustv1.TrustAnchor, *trustv1.MembershipCredential)
	}{
		{"subject", "stage=identity reason=subject_mismatch", func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.SubjectDeviceId = "private-subject" }},
		{"authority", "stage=scope reason=anchor_authority", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) { a.Authority.AuthorityId = "" }},
		{"anchor future", "stage=anchor_time reason=not_yet_valid excess_ms=1", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) {
			a.NotBefore = timestamppb.New(now.Add(time.Millisecond))
		}},
		{"anchor expired", "stage=anchor_time reason=expired excess_ms=0", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) { a.NotAfter = timestamppb.New(now) }},
		{"membership expired", "stage=membership_time reason=expired excess_ms=0", func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.ExpiresAt = timestamppb.New(now) }},
		{"membership beyond anchor", "stage=membership_time reason=beyond_anchor excess_ms=1", func(a *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			a.NotAfter = timestamppb.New(c.ExpiresAt.AsTime().Add(-time.Millisecond))
		}},
		{"capability scope", "stage=scope reason=capability_keys", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) { a.AllowedCapabilityKinds = nil }},
		{"signature", "stage=signature reason=verification", func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) { c.Signature[0] ^= 255 }},
		{"anchor ceiling", "stage=appliance_ttl reason=anchor_remaining excess_ms=1", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) {
			a.NotAfter = timestamppb.New(now.Add(365*24*time.Hour + time.Millisecond))
		}},
		{"anchor span", "stage=appliance_ttl reason=anchor_span excess_ms=1", func(a *trustv1.TrustAnchor, _ *trustv1.MembershipCredential) {
			a.NotBefore = timestamppb.New(now.Add(-time.Minute - time.Millisecond))
		}},
		{"membership ceiling", "stage=appliance_ttl reason=membership_remaining excess_ms=1", func(_ *trustv1.TrustAnchor, c *trustv1.MembershipCredential) {
			c.ExpiresAt = timestamppb.New(now.Add(30*24*time.Hour + time.Millisecond))
			signMessage(t, key, c)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, c := makeBundle(1)
			test.change(a, c)
			err := manager.InstallApplianceManagedTrust(ctx, binding, a, c)
			if !errors.Is(err, ErrRouteUnauthorized) || !strings.Contains(err.Error(), test.reason) || strings.Contains(err.Error(), "private-subject") {
				t.Fatalf("managed rejection reason: %v", err)
			}
		})
	}
	t.Run("unbackdated issuer lead", func(t *testing.T) {
		a, c := makeBundle(1)
		manager.now = func() time.Time { return now.Add(-time.Millisecond) }
		defer func() { manager.now = func() time.Time { return now } }()
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, c); !errors.Is(err, ErrRouteUnauthorized) || !strings.Contains(err.Error(), "stage=appliance_ttl reason=anchor_remaining excess_ms=1") {
			t.Fatalf("one millisecond issuer clock lead: %v", err)
		}
	})
	for index, lead := range []time.Duration{0, time.Millisecond, 234 * time.Millisecond, time.Minute, time.Minute + time.Millisecond} {
		t.Run("backdated issuer lead "+lead.String(), func(t *testing.T) {
			issued := now.Add(lead - time.Minute)
			a, c := makeBundle(uint64(index + 1))
			a.NotBefore = timestamppb.New(issued.Add(-time.Minute))
			a.NotAfter = timestamppb.New(issued.Add(365 * 24 * time.Hour))
			c.ExpiresAt = timestamppb.New(issued.Add(30 * 24 * time.Hour))
			signMessage(t, key, c)
			err := manager.InstallApplianceManagedTrust(ctx, binding, a, c)
			if lead > time.Minute {
				if !errors.Is(err, ErrRouteUnauthorized) || !strings.Contains(err.Error(), "stage=appliance_ttl reason=anchor_remaining excess_ms=1") {
					t.Fatalf("issuer lead %s beyond backdating: %v", lead, err)
				}
			} else if err != nil {
				t.Fatalf("issuer lead %s within backdating: %v", lead, err)
			}
		})
	}
	initial := now
	for _, days := range []int{29, 30, 366} {
		now = initial.Add(time.Duration(days) * 24 * time.Hour)
		revision := uint64(days)
		a, c := makeBundle(revision)
		forged := proto.Clone(c).(*trustv1.MembershipCredential)
		forged.Signature[0] ^= 255
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, forged); err == nil {
			t.Fatal("forged renewal accepted")
		}
		excessive := proto.Clone(c).(*trustv1.MembershipCredential)
		excessive.ExpiresAt = timestamppb.New(now.Add(30*24*time.Hour + time.Second))
		signMessage(t, key, excessive)
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, excessive); err == nil {
			t.Fatal("membership lifetime expanded")
		}
		overlongAnchor := proto.Clone(a).(*trustv1.TrustAnchor)
		overlongAnchor.NotAfter = timestamppb.New(now.Add(365*24*time.Hour + time.Second))
		if err := manager.InstallApplianceManagedTrust(ctx, binding, overlongAnchor, c); err == nil {
			t.Fatal("anchor lifetime expanded")
		}
		foreign := proto.Clone(c).(*trustv1.MembershipCredential)
		foreign.SubjectDeviceId = "foreign"
		signMessage(t, key, foreign)
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, foreign); err == nil {
			t.Fatal("foreign host accepted")
		}
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, c); err != nil {
			t.Fatalf("renewal day %d: %v", days, err)
		}
		if err := manager.InstallApplianceManagedTrust(ctx, binding, a, c); err != nil {
			t.Fatalf("ACK retry day %d: %v", days, err)
		}
	}
}

func TestConcurrentDeviceRepairPreservesWinningKey(t *testing.T) {
	provider := newMemoryKeys(t)
	manager, database, _ := openManager(t, provider)
	if err := database.MarkDeviceIdentityNeedsRepair(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	type result struct {
		record store.DeviceIdentityRecord
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, request := range []string{"repair-a", "repair-b"} {
		go func() {
			<-start
			record, err := manager.Repair(t.Context(), request, true)
			results <- result{record, err}
		}()
	}
	close(start)
	var winner store.DeviceIdentityRecord
	successes := 0
	for range 2 {
		r := <-results
		if r.err == nil {
			successes++
			winner = r.record
		}
	}
	if successes != 1 {
		t.Fatalf("successful repairs = %d", successes)
	}
	current, err := manager.Identity(t.Context())
	if err != nil || !proto.Equal(current.Identity, winner.Identity) {
		t.Fatalf("committed identity = %v, %v", current, err)
	}
	digest := sha256.Sum256([]byte("winning key remains usable"))
	signature, err := provider.Sign(t.Context(), winner.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		t.Fatal(err)
	}
	key := provider.Keys[winner.Identity.GetRootKeyRevision()]
	if !ecdsa.Verify(&key.PublicKey, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("winning key cannot sign")
	}
}
