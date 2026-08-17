//go:build darwin && (development || e2e)

package host

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// This provider is compiled only into development and E2E hosts. Release hosts
// have no runtime switch to it and use the platform-backed provider instead.
type fileApplianceSecrets struct{ directory string }

func newApplianceSecretStore(directory string) (applianceSecretStore, error) {
	directory = filepath.Join(directory, "appliance-secrets")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	return &fileApplianceSecrets{directory: directory}, nil
}

func (store *fileApplianceSecrets) EnsureVolumeKey(_ context.Context, installationID string) ([]byte, error) {
	path := filepath.Join(store.directory, applianceSecretID(installationID)+"-volume.key")
	if value, err := readDevelopmentSecret(path, 32); err == nil {
		return value, nil
	}
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return nil, err
	}
	if err := writeDevelopmentSecret(path, value); err != nil {
		clearSecret(value)
		if errors.Is(err, os.ErrExist) {
			return readDevelopmentSecret(path, 32)
		}
		return nil, err
	}
	return value, nil
}

func (store *fileApplianceSecrets) ReplaceVolumeKey(_ context.Context, installationID string, value []byte) error {
	if len(value) != 32 {
		return errors.New("appliance recovery key has an invalid length")
	}
	path := filepath.Join(store.directory, applianceSecretID(installationID)+"-volume.key")
	file, err := os.CreateTemp(store.directory, ".replacement-volume-key-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(value); err != nil {
		file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return syncHostDirectory(store.directory)
}

func (store *fileApplianceSecrets) StoreBootstrap(_ context.Context, installationID, bootstrapID string, plaintext []byte) error {
	if len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return errors.New("opaque bootstrap seed is outside the allowed bound")
	}
	return writeDevelopmentSecret(store.bootstrapPath(installationID, bootstrapID), plaintext)
}

func (store *fileApplianceSecrets) LoadBootstrap(_ context.Context, installationID, bootstrapID string) ([]byte, error) {
	path := store.bootstrapPath(installationID, bootstrapID)
	value, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errBootstrapNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read protected bootstrap envelope: %w", err)
	}
	if len(value) == 0 || len(value) > 1<<20 {
		return nil, fmt.Errorf("%w: protected bootstrap envelope is invalid", errBootstrapStoreIntegrity)
	}
	return value, nil
}

func (store *fileApplianceSecrets) DeleteBootstrap(_ context.Context, installationID, bootstrapID string) error {
	err := os.Remove(store.bootstrapPath(installationID, bootstrapID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.Join(err, syncHostDirectory(store.directory))
}

func (store *fileApplianceSecrets) DeleteInstallation(_ context.Context, installationID string) error {
	paths, err := filepath.Glob(filepath.Join(store.directory, applianceSecretID(installationID)+"-*"))
	if err != nil {
		return err
	}
	var result error
	for _, path := range paths {
		result = errors.Join(result, os.Remove(path))
	}
	return errors.Join(result, syncHostDirectory(store.directory))
}

func (store *fileApplianceSecrets) bootstrapPath(installationID, bootstrapID string) string {
	return filepath.Join(store.directory, applianceSecretID(installationID)+"-"+bootstrapID+".bootstrap")
}

func readDevelopmentSecret(path string, size int) ([]byte, error) {
	value, err := os.ReadFile(path)
	if err != nil || len(value) != size {
		return nil, errors.New("development secret is unavailable")
	}
	return value, nil
}

func writeDevelopmentSecret(path string, value []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".development-secret-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(value); err != nil {
		file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return err
	}
	return syncHostDirectory(filepath.Dir(path))
}
