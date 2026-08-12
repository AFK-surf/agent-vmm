package dataworker

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestPairingBytesUseExplicitIntegerArrayWireEncoding(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Secret PairingBytes `json:"secret"`
	}{Secret: PairingBytes("12345678")})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"secret":[49,50,51,52,53,54,55,56]}` {
		t.Fatalf("unexpected pairing byte encoding: %s", encoded)
	}
	var decoded struct {
		Secret PairingBytes `json:"secret"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.Secret) != "12345678" {
		t.Fatal("pairing bytes did not round trip")
	}
}

func TestPairingContextDigestVector(t *testing.T) {
	value, err := (PairingContext{SessionID: PairingBytes("session-00000001"), MeshID: PairingBytes("mesh-01"), InviteRevision: 7,
		ManagerIdentity: PairingBytes("device-manager"), JoinerIdentity: PairingBytes("device-joiner"),
		ManagerRootKey: PairingBytes("manager-p256-root-public-key"), JoinerRootKey: PairingBytes("joiner-p256-root-public-key"),
		RolesAndFeatures: PairingBytes("manager,host;service-tunnel-v1")}).Digest()
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(value[:]); got != "7b9129ee42293b8037afcbd0442e03796d46909987bcbad116c741f66217a004" {
		t.Fatalf("pairing context digest = %s", got)
	}
}

func TestPairingBytesRejectValuesOutsideByteRange(t *testing.T) {
	var value PairingBytes
	if err := json.Unmarshal([]byte(`[256]`), &value); err == nil {
		t.Fatal("out-of-range pairing byte was accepted")
	}
}
