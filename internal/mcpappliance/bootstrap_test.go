package mcpappliance

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
)

func TestLaunchClaimAndSeedContract(t *testing.T) {
	t.Parallel()
	now := time.Now()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	seed := []byte("opaque-product-seed")
	claim := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", ArtifactId: "artifact", RegistrationId: "registration", BootId: "boot", SystemNamespace: "system", ContainerInstance: "container", BootstrapId: "bootstrap", BootNonce: bytes.Repeat([]byte{1}, 32), ConfigRevision: 1, ApprovedGrant: &appliancev1.ApprovedGrant{}, ExpiresUnixMillis: now.Add(time.Minute).UnixMilli(), DeviceRootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), privateKey.X, privateKey.Y), DeviceRootRevision: 1}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(canonical)
	r, s, err := ecdsa.Sign(rand.Reader, privateKey, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	claim.Signature = append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	if err := verifyClaim(claim, now); err != nil {
		t.Fatal(err)
	}
	envelope := &appliancev1.OpaqueBootstrapEnvelope{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: bytes.Repeat([]byte{2}, 32), Ciphertext: append([]byte(nil), seed...), ExpiresUnixMillis: claim.GetExpiresUnixMillis()}
	if err := validateEnvelope(claim, envelope); err != nil {
		t.Fatalf("validateEnvelope = %v", err)
	}
	envelope.InstallationId = "other"
	if err := validateEnvelope(claim, envelope); err == nil {
		t.Fatal("cross-installation seed was accepted")
	}
}

func TestOrdinaryRestartClaimOmitsBootstrapID(t *testing.T) {
	t.Parallel()
	now := time.Now()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	claim := &appliancev1.ApplianceLaunchClaim{InstallationId: "installation", ArtifactId: "artifact", RegistrationId: "registration", BootId: "replacement-boot", SystemNamespace: "system", ContainerInstance: "replacement-container", BootNonce: bytes.Repeat([]byte{3}, 32), ConfigRevision: 2, ApprovedGrant: &appliancev1.ApprovedGrant{}, ExpiresUnixMillis: now.Add(time.Minute).UnixMilli(), DeviceRootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), privateKey.X, privateKey.Y), DeviceRootRevision: 1}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(canonical)
	r, s, err := ecdsa.Sign(rand.Reader, privateKey, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	claim.Signature = append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	if claim.GetBootstrapId() != "" {
		t.Fatal("ordinary restart unexpectedly carried a bootstrap ID")
	}
	if err := verifyClaim(claim, now); err != nil {
		t.Fatalf("ordinary restart claim was rejected: %v", err)
	}
}

func TestDurableIdentityAndSingleUseSeed(t *testing.T) {
	t.Parallel()
	store := stateStore{directory: t.TempDir()}
	identity := durableIdentity{InstallationID: "installation", RegistrationID: "registration"}
	if err := store.initializeIdentity(identity); err != nil {
		t.Fatal(err)
	}
	if err := store.initializeIdentity(identity); err != nil {
		t.Fatal(err)
	}
	if err := store.initializeIdentity(durableIdentity{InstallationID: "other", RegistrationID: "registration"}); err == nil {
		t.Fatal("durable identity was replaced")
	}
	if err := store.consumeBootstrap("bootstrap"); err != nil {
		t.Fatal(err)
	}
	if err := store.consumeBootstrap("bootstrap"); err != nil {
		t.Fatalf("same bootstrap retry was not idempotent: %v", err)
	}
	if err := store.consumeBootstrap("other"); err == nil {
		t.Fatal("bootstrap seed was consumed twice")
	}
	info, err := os.Stat(filepath.Join(store.directory, "bootstrap.id"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("seed receipt mode = %v, %v", info.Mode().Perm(), err)
	}
}
