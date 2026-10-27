package appliancebootstrap

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func TestLaunchClaimAndSeedContract(t *testing.T) {
	t.Parallel()
	now := time.Now()
	seed := []byte("opaque-product-seed")
	claim := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", ArtifactId: "artifact", RegistrationId: "registration", BootId: "boot", SystemNamespace: "system", ContainerInstance: "container", BootstrapId: "bootstrap", BootNonce: bytes.Repeat([]byte{1}, 32), ConfigRevision: 1, ApprovedGrant: &appliancev1.ApprovedGrant{}, ExpiresUnixMillis: now.Add(time.Minute).UnixMilli()}
	claim.DeviceIdentity = &trustv1.DeviceIdentity{DeviceId: "device", RootKeyRevision: 1, RootPublicKey: bytes.Repeat([]byte{1}, 33), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}
	if err := VerifyLaunchClaim(claim, now); err != nil {
		t.Fatal(err)
	}
	envelope := &appliancev1.OpaqueBootstrapEnvelope{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: bytes.Repeat([]byte{2}, 32), Ciphertext: append([]byte(nil), seed...), ExpiresUnixMillis: claim.GetExpiresUnixMillis()}
	if err := ValidateEnvelope(claim, envelope); err != nil {
		t.Fatalf("validateEnvelope = %v", err)
	}
	for _, test := range []struct {
		name        string
		repeatClaim bool
	}{{name: "seed"}, {name: "repeated-claim", repeatClaim: true}} {
		t.Run(test.name, func(t *testing.T) {
			seed := proto.Clone(envelope).(*appliancev1.OpaqueBootstrapEnvelope)
			frames := []*appliancev1.BootstrapRequest{
				{Payload: &appliancev1.BootstrapRequest_LaunchClaim{LaunchClaim: claim}},
				{Payload: &appliancev1.BootstrapRequest_Envelope{Envelope: seed}},
				{Payload: &appliancev1.BootstrapRequest_CloseWrite{CloseWrite: true}},
			}
			if test.repeatClaim {
				frames[1] = frames[0]
			}
			stream := &bootstrapStream{frames: frames}
			store := SeedStore{Directory: t.TempDir()}
			err := Serve(stream, Hooks{OnClaim: store.Initialize, OnEnvelope: store.Accept, BeforeReceipt: func(Stream, *appliancev1.ApplianceLaunchClaim, *appliancev1.BootstrapReceipt) error { return nil }})
			if test.repeatClaim {
				if err == nil || len(stream.responses) != 0 {
					t.Fatal("repeated claim reached readiness", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(stream.responses) != 3 || stream.responses[0].GetReceipt().GetBootstrapId() != seed.GetBootstrapId() || !stream.responses[1].GetHealth().GetHealthy() || !stream.responses[2].GetCloseWrite() {
				t.Fatalf("bootstrap responses = %v", stream.responses)
			}
			if !bytes.Equal(seed.Ciphertext, make([]byte, len(seed.Ciphertext))) {
				t.Fatal("seed retained after receipt")
			}
			stream = &bootstrapStream{frames: []*appliancev1.BootstrapRequest{frames[0], frames[2]}}
			if err := Serve(stream, Hooks{OnClaim: store.Initialize, OnEnvelope: store.Accept, BeforeReceipt: func(Stream, *appliancev1.ApplianceLaunchClaim, *appliancev1.BootstrapReceipt) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			if len(stream.responses) != 2 || !stream.responses[0].GetHealth().GetHealthy() || !stream.responses[1].GetCloseWrite() {
				t.Fatalf("restart responses = %v", stream.responses)
			}
		})
	}
	empty := proto.Clone(envelope).(*appliancev1.OpaqueBootstrapEnvelope)
	empty.BootstrapId = ""
	if err := ValidateEnvelope(claim, empty); err == nil {
		t.Fatal("empty bootstrap ID accepted")
	}
	envelope.InstallationId = "other"
	if err := ValidateEnvelope(claim, envelope); err == nil {
		t.Fatal("cross-installation seed was accepted")
	}
}

func TestOrdinaryRestartClaimOmitsBootstrapID(t *testing.T) {
	t.Parallel()
	now := time.Now()
	claim := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", ArtifactId: "artifact", RegistrationId: "registration", BootId: "replacement-boot", SystemNamespace: "system", ContainerInstance: "replacement-container", BootNonce: bytes.Repeat([]byte{3}, 32), ConfigRevision: 2, ApprovedGrant: &appliancev1.ApprovedGrant{}, ExpiresUnixMillis: now.Add(time.Minute).UnixMilli()}
	claim.DeviceIdentity = &trustv1.DeviceIdentity{DeviceId: "device", RootKeyRevision: 1, RootPublicKey: bytes.Repeat([]byte{1}, 33), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}
	if claim.GetBootstrapId() != "" {
		t.Fatal("ordinary restart unexpectedly carried a bootstrap ID")
	}
	if err := VerifyLaunchClaim(claim, now); err != nil {
		t.Fatalf("ordinary restart claim was rejected: %v", err)
	}
}

func TestDurableIdentityAndSeedRotation(t *testing.T) {
	t.Parallel()
	store := SeedStore{Directory: t.TempDir()}
	identity := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", RegistrationId: "registration"}
	if err := store.Initialize(identity); err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(identity); err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(&appliancev1.ApplianceLaunchClaim{InstallationId: "other", RegistrationId: "registration"}); err == nil {
		t.Fatal("durable identity was replaced")
	}
	if err := store.Accept(identity, &appliancev1.OpaqueBootstrapEnvelope{BootstrapId: "bootstrap", Ciphertext: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Accept(identity, &appliancev1.OpaqueBootstrapEnvelope{BootstrapId: "bootstrap", Ciphertext: []byte("first")}); err != nil {
		t.Fatalf("same bootstrap retry was not idempotent: %v", err)
	}
	if err := store.Accept(identity, &appliancev1.OpaqueBootstrapEnvelope{BootstrapId: "bootstrap", Ciphertext: []byte("different")}); err == nil {
		t.Fatal("same ID changed seed")
	}
	if err := store.Accept(identity, &appliancev1.OpaqueBootstrapEnvelope{BootstrapId: "other", Ciphertext: []byte("rotated")}); err != nil {
		t.Fatal(err)
	}
	seed, err := store.Seed()
	if err != nil || string(seed) != "rotated" {
		t.Fatalf("rotated seed: %q, %v", seed, err)
	}
	info, err := os.Stat(filepath.Join(store.Directory, "bootstrap.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("seed receipt mode = %v, %v", info.Mode().Perm(), err)
	}
}

func TestSeedPublicationRecoversOnlyUnpublishedTemporaryFiles(t *testing.T) {
	for _, base := range []string{"identity.json", "bootstrap.json"} {
		t.Run(base, func(t *testing.T) {
			store := SeedStore{Directory: t.TempDir()}
			temporary := filepath.Join(store.Directory, "."+base+".interrupted.tmp")
			if err := os.WriteFile(temporary, []byte("partial"), 0600); err != nil {
				t.Fatal(err)
			}
			claim := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", RegistrationId: "registration"}
			if err := store.Initialize(claim); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(temporary); !os.IsNotExist(err) {
				t.Fatalf("temporary file survived: %v", err)
			}
			if err := store.Accept(claim, &appliancev1.OpaqueBootstrapEnvelope{BootstrapId: "bootstrap", Ciphertext: []byte("first")}); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(store.Directory, base)
			if err := os.WriteFile(target, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if base == "identity.json" {
				if err := store.Initialize(claim); err == nil {
					t.Fatal("empty committed identity repaired without approval")
				}
			} else if _, err := store.Seed(); err == nil {
				t.Fatal("empty committed seed accepted")
			}
			body, err := os.ReadFile(target)
			if err != nil || len(body) != 0 {
				t.Fatal("committed partial file was replaced", err)
			}
		})
	}
}

type bootstrapStream struct {
	grpc.ServerStream
	frames    []*appliancev1.BootstrapRequest
	responses []*appliancev1.BootstrapResponse
}

func (s *bootstrapStream) Recv() (*appliancev1.BootstrapRequest, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}
func (s *bootstrapStream) Send(response *appliancev1.BootstrapResponse) error {
	s.responses = append(s.responses, response)
	return nil
}
