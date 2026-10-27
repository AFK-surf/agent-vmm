package signing_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"

	_ "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	_ "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"
)

type signingVector struct {
	Type         string          `json:"type"`
	Message      json.RawMessage `json:"message"`
	CanonicalHex string          `json:"canonical_hex"`
	DigestHex    string          `json:"digest_hex"`
	PublicKeyHex string          `json:"public_key_hex"`
	SignatureHex string          `json:"signature_hex"`
}

func TestSharedSigningVectors(t *testing.T) {
	data, err := os.ReadFile("../../api/trust/v1/testdata/signing-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []signingVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	names := map[string]string{"PersonalMeshDescriptor": "trust", "MembershipCredential": "trust", "EndpointObservation": "trust", "PairingProof": "trust", "RouteCapability": "trust", "RegistryOperation": "trust", "RegistryReceipt": "trust", "PersonalRouteRequest": "service", "ApplianceLaunchClaim": "appliance"}
	if len(vectors) != len(names) {
		t.Fatalf("signing vector count = %d", len(vectors))
	}
	for _, vector := range vectors {
		t.Run(vector.Type, func(t *testing.T) {
			namespace, ok := names[vector.Type]
			if !ok {
				t.Fatalf("unknown or duplicate signing vector %q", vector.Type)
			}
			delete(names, vector.Type)
			messageName := vector.Type
			if messageName == "PersonalRouteRequest" {
				messageName = "RequestPersonalRouteRequest"
			}
			messageType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName("agentvmm." + namespace + ".v1." + messageName))
			if err != nil {
				t.Fatal(err)
			}
			message := messageType.New().Interface()
			if err := protojson.Unmarshal(vector.Message, message); err != nil {
				t.Fatal(err)
			}
			fieldName := protoreflect.Name("signature")
			switch vector.Type {
			case "PersonalMeshDescriptor":
				fieldName = "genesis_signature"
			case "EndpointObservation":
				fieldName = "device_signature"
			case "PersonalRouteRequest":
				fieldName = "source_signature"
			}
			signatureField := message.ProtoReflect().Descriptor().Fields().ByName(fieldName)
			message.ProtoReflect().Set(signatureField, protoreflect.ValueOfBytes([]byte("signature must be cleared")))
			input, err := signing.CanonicalInput(message)
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(input) != vector.CanonicalHex {
				t.Fatal("canonical input differs from shared vector")
			}
			digest := sha256.Sum256(input)
			if hex.EncodeToString(digest[:]) != vector.DigestHex {
				t.Fatal("digest differs from shared vector")
			}
			publicKey, err := hex.DecodeString(vector.PublicKeyHex)
			if err != nil {
				t.Fatal(err)
			}
			x, y := elliptic.UnmarshalCompressed(elliptic.P256(), publicKey)
			signature, err := hex.DecodeString(vector.SignatureHex)
			if err != nil {
				t.Fatal(err)
			}
			if len(signature) != 64 {
				t.Fatalf("raw signature length = %d", len(signature))
			}
			r, s := new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])
			halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
			if x == nil || s.Cmp(halfOrder) > 0 || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], r, s) {
				t.Fatal("shared signature is not valid low-S P256")
			}
		})
	}
}

func TestCanonicalInputRejectsUnknownFields(t *testing.T) {
	operation := &trustv1.RegistryOperation{OperationId: "op-1"}
	operation.ProtoReflect().SetUnknown(protowire.AppendTag(nil, 100, protowire.VarintType))
	if _, err := signing.CanonicalInput(operation); err == nil {
		t.Fatal("unknown fields were accepted")
	}
}

func TestCanonicalInputRejectsMapsEvenWhenUnsetOrNested(t *testing.T) {
	for _, message := range []proto.Message{&structpb.Struct{}, &structpb.ListValue{}} {
		if _, err := signing.CanonicalInput(message); err == nil || !strings.Contains(err.Error(), "map field") {
			t.Fatalf("map descriptor error = %v", err)
		}
	}
}

func TestCanonicalInputRejectsUnsignedTypes(t *testing.T) {
	if _, err := signing.CanonicalInput(&trustv1.DeviceIdentity{}); err == nil {
		t.Fatal("unsigned message type was accepted")
	}
}
