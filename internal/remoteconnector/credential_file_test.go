//go:build darwin

package remoteconnector

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

func TestFileCredentialStorePersistsAndDeletes(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store := NewCredentialStore(directory)
	credential := []byte("controller-credential")
	if err := store.Replace(context.Background(), "registration-a", credential); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), "registration-a")
	if err != nil || !bytes.Equal(loaded, credential) {
		t.Fatalf("Load() = %q, %v", loaded, err)
	}
	path := store.(*fileCredentialStore).path("registration-a")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode = %v, %v", info, err)
	}
	if err := store.Delete(context.Background(), "registration-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "registration-a"); !errors.Is(err, ErrCredentialUnavailable) {
		t.Fatalf("Load() after delete = %v", err)
	}
}

func TestFileCredentialStoreRejectsInsecureMode(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store := NewCredentialStore(directory)
	if err := store.Replace(context.Background(), "registration-a", []byte("credential")); err != nil {
		t.Fatal(err)
	}
	path := store.(*fileCredentialStore).path("registration-a")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "registration-a"); !errors.Is(err, ErrCredentialUnavailable) {
		t.Fatalf("insecure credential mode error = %v", err)
	}
}
