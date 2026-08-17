//go:build darwin && (development || e2e)

package remoteconnector

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

func TestDevelopmentCredentialStorePersistsAndDeletes(t *testing.T) {
	store := NewCredentialStore(t.TempDir())
	credential := []byte("development-controller-credential")
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
