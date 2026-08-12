package remote

import (
	"bytes"
	"testing"
)

func TestBearerCredentialWire(t *testing.T) {
	raw := []byte{0, 1, 2, 253, 254, 255}
	wire := EncodeBearer(raw)
	decoded, err := DecodeBearer(wire)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("DecodeBearer(%q) = %x, %v", wire, decoded, err)
	}
	for _, invalid := range []string{"", "bearer AAE=", "Bearer AAE", "Bearer AA-_"} {
		if _, err := DecodeBearer(invalid); err == nil {
			t.Fatalf("DecodeBearer(%q) succeeded", invalid)
		}
	}
}
