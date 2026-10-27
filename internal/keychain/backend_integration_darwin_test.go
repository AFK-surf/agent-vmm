//go:build darwin && keychain && keychain_integration && cgo

package keychain

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"math/big"
	"os"
	"testing"
)

var expectKeychain = flag.Bool("expect-keychain", true, "expect signed Keychain access")

func TestSignedKeychainStorage(t *testing.T) {
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	backend := New("test-" + hex.EncodeToString(suffix[:]))
	if !*expectKeychain {
		if err := backend.Probe(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("unentitled Keychain probe=%v, want unavailable", err)
		}
		return
	}
	if err := backend.Probe(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.DeletePrefix(context.Background(), ""); err != nil {
			t.Error(err)
		}
		if err := backend.DeleteKey(context.Background(), "device-root.key"); err != nil {
			t.Error(err)
		}
	})
	if err := backend.Put(t.Context(), "bootstrap", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Put(t.Context(), "bootstrap", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Put(t.Context(), "bootstrap", []byte("second")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("conflicting Put=%v", err)
	}
	if err := backend.Replace(t.Context(), "bootstrap", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	value, err := backend.Get(t.Context(), "bootstrap")
	if err != nil || !bytes.Equal(value, []byte("replacement")) {
		t.Fatalf("Get=%q,%v", value, err)
	}
	if err := backend.Delete(t.Context(), "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Get(t.Context(), "bootstrap"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get after deletion=%v", err)
	}
	public, custody, err := backend.EnsureKey(t.Context(), "device-root.key", "")
	if err != nil || (custody != "secure-enclave" && custody != "keychain") {
		t.Fatalf("EnsureKey custody=%q,%v", custody, err)
	}
	digest := sha256.Sum256([]byte("signed release storage test"))
	signature, err := backend.Sign(t.Context(), "device-root.key", digest[:])
	if err != nil {
		t.Fatal(err)
	}
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), public)
	if x == nil || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("Keychain signature did not verify")
	}
	reloaded, reloadedCustody, err := backend.EnsureKey(t.Context(), "device-root.key", custody)
	if err != nil || !bytes.Equal(reloaded, public) || custody != reloadedCustody {
		t.Fatalf("persisted root changed: custody=%q,%v", reloadedCustody, err)
	}
}
