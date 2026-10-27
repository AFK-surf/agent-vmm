//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"os"

	"github.com/AFK-surf/agent-vmm/internal/keychain"
)

type keychainApplianceSecrets struct{ backend keychain.PasswordStore }

func NewKeychainSecretStore(backend keychain.PasswordStore) SecretStore {
	return &keychainApplianceSecrets{backend: backend}
}

func bootstrapAccount(installationID, bootstrapID string) string {
	return applianceSecretID(installationID) + "-" + bootstrapID + ".bootstrap"
}

func (store *keychainApplianceSecrets) StoreBootstrap(ctx context.Context, installationID, bootstrapID string, seed []byte) error {
	if len(seed) == 0 || len(seed) > 1<<20 {
		return errors.New("opaque bootstrap seed is outside the allowed bound")
	}
	return store.backend.Put(ctx, bootstrapAccount(installationID, bootstrapID), seed)
}

func (store *keychainApplianceSecrets) LoadBootstrap(ctx context.Context, installationID, bootstrapID string) ([]byte, error) {
	value, err := store.backend.Get(ctx, bootstrapAccount(installationID, bootstrapID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, errBootstrapNotFound
	}
	return value, err
}

func (store *keychainApplianceSecrets) DeleteBootstrap(ctx context.Context, installationID, bootstrapID string) error {
	return store.backend.Delete(ctx, bootstrapAccount(installationID, bootstrapID))
}

func (store *keychainApplianceSecrets) DeleteInstallation(ctx context.Context, installationID string) error {
	return errors.Join(store.backend.DeletePrefix(ctx, applianceSecretID(installationID)+"-"), store.backend.Delete(ctx, "mcp-token-key/"+installationID))
}

func (store *keychainApplianceSecrets) LoadMCPTokenKey(ctx context.Context, installationID string) ([]byte, error) {
	return store.backend.Get(ctx, "mcp-token-key/"+installationID)
}

func (store *keychainApplianceSecrets) StoreMCPTokenKey(ctx context.Context, installationID string, key []byte) error {
	return store.backend.Replace(ctx, "mcp-token-key/"+installationID, key)
}
