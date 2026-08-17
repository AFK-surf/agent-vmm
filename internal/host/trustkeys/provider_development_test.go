//go:build darwin && (development || e2e)

package trustkeys

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"math/big"
	"path/filepath"
	"testing"
)

func TestDevelopmentFileProviderPersistsSigningIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "device-root.key")
	first := NewDevelopmentFileProvider(path)
	public, custody, err := first.Ensure(context.Background(), 1)
	if err != nil || custody != "test" {
		t.Fatalf("Ensure() = %x, %q, %v", public, custody, err)
	}
	digest := sha256.Sum256([]byte("claim"))
	signature, err := first.Sign(context.Background(), 1, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), public)
	if x == nil || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("file provider signature did not verify")
	}
	second := NewDevelopmentFileProvider(path)
	reloaded, _, err := second.Ensure(context.Background(), 1)
	if err != nil || string(reloaded) != string(public) {
		t.Fatalf("device root did not persist: %x, %v", reloaded, err)
	}
}
