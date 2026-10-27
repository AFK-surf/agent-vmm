//go:build darwin || linux

package trustkeys

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	keychainfixture "github.com/AFK-surf/agent-vmm/internal/keychain/testfixture"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func TestProvidersKeepSigningAndRepairRevisions(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "device-root.key")
	memory := keychainfixture.NewKeys()
	for _, test := range []struct {
		name, custody string
		new           func() Provider
	}{
		{"file", "file", func() Provider { return NewFileProvider(path) }},
		{"keychain-memory", "keychain", func() Provider { return NewKeychainProvider(memory, "") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			first := test.new()
			public, custody, err := first.Ensure(t.Context(), 1)
			if err != nil || custody != test.custody {
				t.Fatalf("Ensure=%x,%q,%v", public, custody, err)
			}
			digest := sha256.Sum256([]byte("claim"))
			signature, err := first.Sign(t.Context(), 1, digest[:])
			if err != nil {
				t.Fatal(err)
			}
			x, y := elliptic.UnmarshalCompressed(elliptic.P256(), public)
			if x == nil || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
				t.Fatal("signature did not verify")
			}
			reloaded, _, err := test.new().Ensure(t.Context(), 1)
			if err != nil || string(reloaded) != string(public) {
				t.Fatalf("root did not persist: %x,%v", reloaded, err)
			}
			second, _, err := first.Ensure(t.Context(), 2)
			if err != nil || string(second) == string(public) {
				t.Fatalf("repair key=%x,%v", second, err)
			}
			if err := first.Delete(t.Context(), 2); err != nil {
				t.Fatal(err)
			}
			if _, err := first.Sign(t.Context(), 2, digest[:]); !errors.Is(err, ErrKeyUnavailable) {
				t.Fatalf("deleted repair key=%v", err)
			}
			reloaded, _, err = first.Ensure(t.Context(), 1)
			if err != nil || string(reloaded) != string(public) {
				t.Fatalf("repair deletion changed root: %x,%v", reloaded, err)
			}
		})
	}
}

func TestFileProviderRejectsInsecureKeyMode(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "device-root.key")
	provider := NewFileProvider(path)
	if _, _, err := provider.Ensure(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Sign(context.Background(), 1, make([]byte, 32)); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("insecure key mode error = %v", err)
	}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
