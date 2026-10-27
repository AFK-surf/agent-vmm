package trust

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestRegistrationRecoveryProofBindsExactChallengeToExistingRoot(t *testing.T) {
	vector, err := os.ReadFile("testdata/registration_recovery_wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Challenge json.RawMessage `json:"challenge"`
		Wire      string          `json:"wire"`
	}
	if err := json.Unmarshal(vector, &fixture); err != nil {
		t.Fatal(err)
	}
	_, wire, err := Parse(fixture.Challenge, time.Unix(1790848400, 0))
	if err != nil || string(wire) != fixture.Wire {
		t.Fatalf("cross-language wire=%q err=%v", wire, err)
	}
	keys := newMemoryKeys(t)
	manager, _, _ := openManager(t, keys)
	now := time.Unix(1790848400, 0)
	manager.now = func() time.Time { return now }
	challenge := Challenge{Purpose: Purpose, Audience: "https://controller.example", Subject: "original-user", SessionID: "new-session", ControllerBinding: `["tenant","group","workspace"]`, EnvironmentID: "environment", OperationID: "operation", RegistrationID: "registration", Nonce: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), Revision: 19, ExpiresAt: now.Add(time.Minute).Unix()}
	raw, err := json.Marshal(challenge)
	if err != nil {
		t.Fatal(err)
	}
	identity, signature, err := manager.SignRegistrationRecovery(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	_, wire, err = Parse(raw, now)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	if !ecdsa.Verify(&keys.Keys[identity.GetRootKeyRevision()].PublicKey, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("proof does not verify with original root")
	}
	changed := challenge
	changed.SessionID = "another-session"
	raw, _ = json.Marshal(changed)
	_, wire, _ = Parse(raw, now)
	digest = sha256.Sum256(wire)
	if ecdsa.Verify(&keys.Keys[identity.GetRootKeyRevision()].PublicKey, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("proof accepted for another Session")
	}
	changed.Purpose = "arbitrary-signature"
	raw, _ = json.Marshal(changed)
	if _, _, err := manager.SignRegistrationRecovery(context.Background(), raw); err == nil {
		t.Fatal("arbitrary signing purpose accepted")
	}
	changed = challenge
	changed.ExpiresAt = now.Unix()
	raw, _ = json.Marshal(changed)
	if _, _, err := manager.SignRegistrationRecovery(context.Background(), raw); err == nil {
		t.Fatal("expired challenge signed")
	}
	if _, _, err := manager.SignRegistrationRecovery(context.Background(), append(raw, []byte(" {}")...)); err == nil {
		t.Fatal("trailing input signed")
	}
	keys.Unavailable[identity.GetRootKeyRevision()] = true
	raw, _ = json.Marshal(challenge)
	if _, _, err := manager.SignRegistrationRecovery(context.Background(), raw); err == nil {
		t.Fatal("unavailable root signed proof")
	}
}
