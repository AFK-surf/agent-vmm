//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
)

// fileApplianceSecrets uses the Host service user's private state directory.
// Agent VMM treats that OS user as one failure domain. Host disk encryption
// protects these files offline.
type fileApplianceSecrets struct{ directory string }

func NewSecretStore(directory string) (SecretStore, error) {
	directory = filepath.Join(directory, "appliance-secrets")
	if err := privatefs.EnsureDirectory(directory); err != nil {
		return nil, err
	}
	return &fileApplianceSecrets{directory: directory}, nil
}

func (store *fileApplianceSecrets) StoreBootstrap(_ context.Context, installationID, bootstrapID string, plaintext []byte) error {
	if len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return errors.New("opaque bootstrap seed is outside the allowed bound")
	}
	return atomicfile.PublishOnce(store.bootstrapPath(installationID, bootstrapID), plaintext, 0o600)
}

func (store *fileApplianceSecrets) LoadBootstrap(_ context.Context, installationID, bootstrapID string) ([]byte, error) {
	path := store.bootstrapPath(installationID, bootstrapID)
	value, err := readSecret(path, 1, 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errBootstrapNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read protected bootstrap envelope: %w", err)
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
	paths, err := filepath.Glob(filepath.Join(store.directory, applianceSecretID(installationID)+"-*.bootstrap"))
	if err != nil {
		return err
	}
	keyErr := os.Remove(filepath.Join(store.directory, applianceSecretID(installationID)+".mcp-token-key"))
	if errors.Is(keyErr, os.ErrNotExist) {
		keyErr = nil
	}
	var result error = keyErr
	for _, path := range paths {
		result = errors.Join(result, os.Remove(path))
	}
	return errors.Join(result, syncHostDirectory(store.directory))
}

func (store *fileApplianceSecrets) bootstrapPath(installationID, bootstrapID string) string {
	return filepath.Join(store.directory, applianceSecretID(installationID)+"-"+bootstrapID+".bootstrap")
}

func readSecret(path string, minimum, maximum int) ([]byte, error) {
	value, err := privatefs.ReadFile(path, minimum, maximum)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %w", errBootstrapStoreIntegrity, err)
	}
	return value, err
}

func (store *fileApplianceSecrets) LoadMCPTokenKey(_ context.Context, installationID string) ([]byte, error) {
	return privatefs.ReadFile(filepath.Join(store.directory, applianceSecretID(installationID)+".mcp-token-key"), 32, 32)
}

func (store *fileApplianceSecrets) StoreMCPTokenKey(_ context.Context, installationID string, key []byte) error {
	return atomicfile.ReplaceFile(filepath.Join(store.directory, applianceSecretID(installationID)+".mcp-token-key"), key, 0o600)
}
