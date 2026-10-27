//go:build darwin || linux

package remoteconnector

import (
	"bytes"
	"context"
	"errors"
	keychainfixture "github.com/AFK-surf/agent-vmm/internal/keychain/testfixture"
	"os"
	"testing"
)

func TestCredentialStoresPersistReplaceAndDelete(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		store CredentialStore
	}{
		{"file", NewCredentialStore(directory)},
		{"keychain-memory", NewKeychainCredentialStore(keychainfixture.NewPasswords())},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := test.store
			for _, credential := range [][]byte{[]byte("first-credential"), []byte("replacement-credential")} {
				if err := store.Replace(t.Context(), "registration-a", credential); err != nil {
					t.Fatal(err)
				}
				loaded, err := store.Load(t.Context(), "registration-a")
				if err != nil || !bytes.Equal(loaded, credential) {
					t.Fatalf("Load=%q,%v", loaded, err)
				}
			}
			if file, ok := store.(*fileCredentialStore); ok {
				info, err := os.Stat(file.path("registration-a"))
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("credential mode=%v,%v", info, err)
				}
			}
			if _, err := store.Load(t.Context(), "registration-b"); !errors.Is(err, ErrCredentialUnavailable) {
				t.Fatalf("credential crossed registration: %v", err)
			}
			if err := store.Delete(t.Context(), "registration-a"); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(t.Context(), "registration-a"); !errors.Is(err, ErrCredentialUnavailable) {
				t.Fatalf("Load after delete=%v", err)
			}
		})
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
