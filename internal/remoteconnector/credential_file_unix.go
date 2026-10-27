//go:build darwin || linux

package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
)

type fileCredentialStore struct{ directory string }

func NewCredentialStore(directory string) CredentialStore {
	return &fileCredentialStore{directory: directory}
}

func (store *fileCredentialStore) Load(_ context.Context, account string) ([]byte, error) {
	value, err := privatefs.ReadFile(store.path(account), 1, MaxCredentialBytes)
	if err != nil {
		return nil, ErrCredentialUnavailable
	}
	return value, nil
}

func (store *fileCredentialStore) Replace(_ context.Context, account string, credential []byte) error {
	if len(credential) == 0 || len(credential) > MaxCredentialBytes {
		return errors.New("credential size is invalid")
	}
	if err := privatefs.EnsureDirectory(store.directory); err != nil {
		return err
	}
	if err := atomicfile.ReplaceFile(store.path(account), credential, 0o600); err != nil {
		return fmt.Errorf("write controller credential: %w", err)
	}
	return nil
}

func (store *fileCredentialStore) Delete(_ context.Context, account string) error {
	err := os.Remove(store.path(account))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *fileCredentialStore) path(account string) string {
	return filepath.Join(store.directory, credentialName(account))
}
