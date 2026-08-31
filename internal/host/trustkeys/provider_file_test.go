//go:build darwin

package trustkeys

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFileProviderPersistsSigningIdentity(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "device-root.key")
	first := NewFileProvider(path)
	public, custody, err := first.Ensure(context.Background(), 1)
	if err != nil || custody != "file" {
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
	second := NewFileProvider(path)
	reloaded, _, err := second.Ensure(context.Background(), 1)
	if err != nil || string(reloaded) != string(public) {
		t.Fatalf("device root did not persist: %x, %v", reloaded, err)
	}
}

func TestFileProviderSupportsRepairRevisions(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "device-root.key")
	provider := NewFileProvider(path)
	first, _, err := provider.Ensure(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := provider.Ensure(context.Background(), 2)
	if err != nil || string(first) == string(second) {
		t.Fatalf("repair key = %x, err = %v", second, err)
	}
	if err := provider.Delete(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Sign(context.Background(), 2, make([]byte, 32)); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("deleted repair key error = %v", err)
	}
	if reloaded, _, err := provider.Ensure(context.Background(), 1); err != nil || string(reloaded) != string(first) {
		t.Fatalf("revision one changed after deleting repair key: %x, %v", reloaded, err)
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

func TestWritePrivateKeyPublishesOneLinkAndDoesNotReplace(t *testing.T) {
	directory := privateTempDir(t)
	path := filepath.Join(directory, "device-root.key")
	first := []byte("first")
	if err := writePrivateKey(path, first); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		t.Fatalf("published private key link count = %v", stat)
	}
	if err := writePrivateKey(path, []byte("second")); !os.IsExist(err) {
		t.Fatalf("exclusive publish error = %v", err)
	}
	value, err := os.ReadFile(path)
	if err != nil || string(value) != string(first) {
		t.Fatalf("existing private key changed: %q, %v", value, err)
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".device-root-*.tmp"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary links remained after publish: %v, %v", matches, err)
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
