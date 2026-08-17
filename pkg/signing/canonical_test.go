package signing_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const endpointObservationCanonicalHex = "6167656e742d766d6d2f7369676e61747572652f763100456e64706f696e744f62736572766174696f6e000a0d6465766963652d676f6c64656e10071a10656e64706f696e742d6e6f64652d3031200b2a186167656e742d766d6d2f6d6573682d636f6e74726f6c2f312a1a6167656e742d766d6d2f736572766963652d74756e6e656c2f313206646972656374320572656c61793a20000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f42060880a8d6b907"
const endpointObservationDigestHex = "119c317e06bbbe94c770dbef2b8cd6ff929c1feb1ed7802761edb8b2853b5bae"
const endpointObservationPublicKeyHex = "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
const endpointObservationSignatureHex = "a88bec732af82ff8a839afacd8d69d59a3280bae70fb6d7e40b4943218b321b732a1ca150a228e6a118f80cb2b1b5ec1da5bd57b3bd1e86023838c87f803f5c7"

func TestCanonicalInputVector(t *testing.T) {
	t.Parallel()
	capability := &trustv1.RouteCapability{
		CapabilityId: "route-cap-01",
		Authority: &trustv1.AuthorityDescriptor{
			AuthorityId:    "mesh-authority-01",
			AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_PERSONAL_MESH,
			KeyId:          "device-root-7",
			TrustDomainRef: []byte("mesh-01"),
			Revision:       11,
		},
		SourceDeviceId:      "device-a",
		DestinationDeviceId: "device-b",
		DestinationExportId: "export-web",
		AllowedProtocol:     "tcp",
		AllowedVerbs:        []string{"connect"},
		RouteGeneration:     3,
		PolicyRevision:      11,
		Budget:              &trustv1.RouteBudget{ConnectionLimit: 2, ByteLimit: 1048576, ConcurrencyLimit: 1},
		NotBefore:           &timestamppb.Timestamp{Seconds: 1_800_000_000},
		ExpiresAt:           &timestamppb.Timestamp{Seconds: 1_800_003_600},
		Nonce:               []byte{0, 1, 2, 3},
		Audience:            "agent-vmm/service-tunnel/1",
		IssuerKeyId:         "device-root-7",
		Signature:           []byte("ignored"),
	}
	input, err := signing.CanonicalInput(capability)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(input)
	const expected = "a77e15647b6cb61760efa7d44069e52e967f159602a5eaeabe83bb544fab62a0"
	if got := hex.EncodeToString(digest[:]); got != expected {
		t.Fatalf("canonical digest = %s, want %s", got, expected)
	}
}

func TestEndpointObservationCanonicalAndRawLowSSignatureVector(t *testing.T) {
	t.Parallel()
	observation := &trustv1.EndpointObservation{
		DeviceId: "device-golden", RootKeyRevision: 7, EndpointNodeId: []byte("endpoint-node-01"), EndpointGeneration: 11,
		SupportedAlpns: []string{"agent-vmm/mesh-control/1", "agent-vmm/service-tunnel/1"}, FeatureSet: []string{"direct", "relay"},
		ObservedAddressesDigest: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
		ExpiresAt:               &timestamppb.Timestamp{Seconds: 2_000_000_000}, DeviceSignature: []byte("ignored"),
	}
	input, err := signing.CanonicalInput(observation)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(input); got != endpointObservationCanonicalHex {
		t.Fatalf("endpoint observation canonical input = %s", got)
	}
	digest := sha256.Sum256(input)
	if got := hex.EncodeToString(digest[:]); got != endpointObservationDigestHex {
		t.Fatalf("endpoint observation digest = %s", got)
	}
	publicKey, err := hex.DecodeString(endpointObservationPublicKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), publicKey)
	signature, err := hex.DecodeString(endpointObservationSignatureHex)
	if err != nil {
		t.Fatal(err)
	}
	if len(signature) != 64 {
		t.Fatalf("endpoint observation signature length = %d", len(signature))
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if x == nil || y == nil || s.Cmp(halfOrder) > 0 || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], r, s) {
		t.Fatal("endpoint observation raw low-S signature vector is invalid")
	}
}

func TestCanonicalInputRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	operation := &trustv1.RegistryOperation{OperationId: "op-1"}
	operation.ProtoReflect().SetUnknown(protowire.AppendTag(nil, 100, protowire.VarintType))
	if _, err := signing.CanonicalInput(operation); err == nil {
		t.Fatal("CanonicalInput accepted unknown fields")
	}
}

func TestManagedCredentialCanonicalInputMatchesSalixVector(t *testing.T) {
	t.Parallel()
	credential := &trustv1.MembershipCredential{
		Authority: &trustv1.AuthorityDescriptor{AuthorityId: "managed:test", AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER,
			KeyId: "key-1", TrustDomainRef: []byte{1, 2, 3}, Revision: 7},
		SubjectDeviceId: "device-a", SubjectRootKeyRevision: 3, SubjectRootPublicKey: make([]byte, 33), OpaqueClaimsDigest: make([]byte, 32), PolicyRevision: 9,
		ExpiresAt: &timestamppb.Timestamp{Seconds: 2_000_000_000}, Signature: []byte("ignored"),
	}
	for index := range credential.OpaqueClaimsDigest {
		credential.OpaqueClaimsDigest[index] = 4
	}
	input, err := signing.CanonicalInput(credential)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "6167656e742d766d6d2f7369676e61747572652f7631004d656d6265727368697043726564656e7469616c000a1e0a0c6d616e616765643a7465737410011a056b65792d312203010203280712086465766963652d61180322200404040404040404040404040404040404040404040404040404040404040404280932060880a8d6b9074221000000000000000000000000000000000000000000000000000000000000000000"
	if got := hex.EncodeToString(input); got != expected {
		t.Fatalf("managed credential input = %s, want %s", got, expected)
	}
}

func TestManagedRouteCanonicalInputMatchesSalixVector(t *testing.T) {
	t.Parallel()
	credential := &trustv1.MembershipCredential{Authority: &trustv1.AuthorityDescriptor{AuthorityId: "managed:test", AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER,
		KeyId: "key-1", TrustDomainRef: []byte{1, 2, 3}, Revision: 7}, SubjectDeviceId: "device-a", SubjectRootKeyRevision: 3,
		SubjectRootPublicKey: make([]byte, 33), OpaqueClaimsDigest: make([]byte, 32), PolicyRevision: 9, ExpiresAt: &timestamppb.Timestamp{Seconds: 2_000_000_000}, Signature: make([]byte, 64)}
	for index := range credential.OpaqueClaimsDigest {
		credential.OpaqueClaimsDigest[index] = 4
	}
	for index := range credential.Signature {
		credential.Signature[index] = 5
	}
	capability := &trustv1.RouteCapability{CapabilityId: "route-1", Authority: credential.Authority, SourceDeviceId: "device-a", SourceAllocationId: stringPointer("alloc-1"),
		DestinationDeviceId: "device-b", DestinationExportId: "export-1", AllowedProtocol: "tcp", AllowedVerbs: []string{"connect"}, RouteGeneration: 2,
		PolicyRevision: 9, Budget: &trustv1.RouteBudget{ConnectionLimit: 3, ByteLimit: 4096, ConcurrencyLimit: 2}, NotBefore: &timestamppb.Timestamp{Seconds: 1_999_999_900},
		ExpiresAt: &timestamppb.Timestamp{Seconds: 2_000_000_000}, Nonce: make([]byte, 32), Audience: "agent-vmm/service-tunnel/1", IssuerKeyId: "key-1",
		RouteClass: "compute", SourceMembershipCredential: credential, Signature: []byte("ignored")}
	for index := range capability.Nonce {
		capability.Nonce[index] = 6
	}
	input, err := signing.CanonicalInput(capability)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(input)
	if got := hex.EncodeToString(digest[:]); got != "fa37b567d74edd504ba560b5f4080a70d767475dc6d6c8ce39ff67838654f013" {
		t.Fatalf("managed route canonical digest = %s", got)
	}
}

func stringPointer(value string) *string { return &value }

func TestCanonicalInputRejectsUnsignedTypes(t *testing.T) {
	t.Parallel()
	if _, err := signing.CanonicalInput(&trustv1.DeviceIdentity{}); err == nil {
		t.Fatal("CanonicalInput accepted a message without a V1 signature contract")
	}
}
