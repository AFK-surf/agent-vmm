//go:build darwin && (development || e2e)

package remoteconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type fileCredentialStore struct{ directory string }

func NewCredentialStore(directory string) CredentialStore {
	return &fileCredentialStore{directory: directory}
}

func (store *fileCredentialStore) Load(_ context.Context, account string) ([]byte, error) {
	value, err := os.ReadFile(store.path(account))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrCredentialUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("load development credential: %w", err)
	}
	if len(value) == 0 || len(value) > MaxCredentialBytes {
		return nil, ErrCredentialUnavailable
	}
	return value, nil
}

func (store *fileCredentialStore) Replace(_ context.Context, account string, credential []byte) error {
	if len(credential) == 0 || len(credential) > MaxCredentialBytes {
		return errors.New("credential size is invalid")
	}
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return fmt.Errorf("create development credential directory: %w", err)
	}
	if err := os.Chmod(store.directory, 0o700); err != nil {
		return fmt.Errorf("protect development credential directory: %w", err)
	}
	temporary, err := os.CreateTemp(store.directory, ".credential-*")
	if err != nil {
		return fmt.Errorf("create development credential: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(credential)
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write development credential: %w", err)
	}
	if err := os.Rename(temporaryName, store.path(account)); err != nil {
		return fmt.Errorf("commit development credential: %w", err)
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
	digest := sha256.Sum256([]byte(account))
	return filepath.Join(store.directory, hex.EncodeToString(digest[:16])+".credential")
}
