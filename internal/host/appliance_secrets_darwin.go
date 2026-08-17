//go:build darwin && !development && !e2e

package host

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/keybase/go-keychain"
)

const applianceSecretService = "surf.afk.agent-vmm.appliance.v1"

type keychainApplianceSecrets struct{ directory string }

func newKeychainApplianceSecrets(directory string) (*keychainApplianceSecrets, error) {
	directory = filepath.Join(directory, "appliance-secrets")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	return &keychainApplianceSecrets{directory: directory}, nil
}

func (store *keychainApplianceSecrets) EnsureVolumeKey(ctx context.Context, installationID string) ([]byte, error) {
	return store.ensureKey(ctx, "volume", installationID)
}

func (store *keychainApplianceSecrets) ReplaceVolumeKey(_ context.Context, installationID string, value []byte) error {
	if len(value) != 32 {
		return errors.New("appliance recovery key has an invalid length")
	}
	query := applianceKeychainQuery("volume", installationID)
	update := keychain.NewItem()
	update.SetData(value)
	if err := keychain.UpdateItem(query, update); err == nil {
		return nil
	} else if !errors.Is(err, keychain.ErrorItemNotFound) {
		return errors.New("replace appliance platform key")
	}
	query.SetData(value)
	if err := keychain.AddItem(query); err != nil {
		return errors.New("store replacement appliance platform key")
	}
	return nil
}

func (store *keychainApplianceSecrets) StoreBootstrap(ctx context.Context, installationID, bootstrapID string, plaintext []byte) error {
	if len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return errors.New("opaque bootstrap seed is outside the allowed bound")
	}
	key, err := store.ensureKey(ctx, "bootstrap", installationID)
	if err != nil {
		return err
	}
	defer clearSecret(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	aad := []byte(installationID + "\x00" + bootstrapID)
	ciphertext := append(nonce, aead.Seal(nil, nonce, plaintext, aad)...)
	path := store.bootstrapPath(installationID, bootstrapID)
	temporary, err := os.CreateTemp(store.directory, ".bootstrap-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(ciphertext); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncHostDirectory(store.directory)
}

func (store *keychainApplianceSecrets) LoadBootstrap(ctx context.Context, installationID, bootstrapID string) ([]byte, error) {
	path := store.bootstrapPath(installationID, bootstrapID)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, errBootstrapNotFound
	} else if err != nil {
		return nil, fmt.Errorf("stat protected bootstrap envelope: %w", err)
	}
	key, err := store.loadKey("bootstrap", installationID)
	if err != nil {
		return nil, fmt.Errorf("load protected bootstrap key: %w", err)
	}
	defer clearSecret(key)
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read protected bootstrap envelope: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(ciphertext) < aead.NonceSize() {
		return nil, errors.New("protected bootstrap envelope is invalid")
	}
	nonce := ciphertext[:aead.NonceSize()]
	aad := []byte(installationID + "\x00" + bootstrapID)
	plaintext, err := aead.Open(nil, nonce, ciphertext[aead.NonceSize():], aad)
	if err != nil {
		return nil, errors.New("protected bootstrap envelope authentication failed")
	}
	return plaintext, nil
}

func (store *keychainApplianceSecrets) DeleteBootstrap(_ context.Context, installationID, bootstrapID string) error {
	var result error
	if err := os.Remove(store.bootstrapPath(installationID, bootstrapID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		result = err
	}
	if err := keychain.DeleteItem(applianceKeychainQuery("bootstrap", installationID)); err != nil && !errors.Is(err, keychain.ErrorItemNotFound) {
		result = errors.Join(result, err)
	}
	return errors.Join(result, syncHostDirectory(store.directory))
}

func (store *keychainApplianceSecrets) DeleteInstallation(ctx context.Context, installationID string) error {
	var result error
	for _, kind := range []string{"volume", "bootstrap"} {
		if err := keychain.DeleteItem(applianceKeychainQuery(kind, installationID)); err != nil && !errors.Is(err, keychain.ErrorItemNotFound) {
			result = errors.Join(result, err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(store.directory, applianceSecretID(installationID)+"-*.envelope"))
	for _, path := range files {
		result = errors.Join(result, os.Remove(path))
	}
	_ = ctx
	return result
}

func (store *keychainApplianceSecrets) ensureKey(_ context.Context, kind, installationID string) ([]byte, error) {
	if key, err := store.loadKey(kind, installationID); err == nil {
		return key, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	item := applianceKeychainQuery(kind, installationID)
	item.SetData(key)
	if err := keychain.AddItem(item); err != nil {
		clearSecret(key)
		if errors.Is(err, keychain.ErrorDuplicateItem) {
			return store.loadKey(kind, installationID)
		}
		return nil, errors.New("store appliance platform key")
	}
	return key, nil
}

func (store *keychainApplianceSecrets) loadKey(kind, installationID string) ([]byte, error) {
	query := applianceKeychainQuery(kind, installationID)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnData(true)
	results, err := keychain.QueryItem(query)
	if err != nil || len(results) != 1 || len(results[0].Data) != 32 {
		return nil, errors.New("appliance platform key is unavailable")
	}
	return append([]byte(nil), results[0].Data...), nil
}

func applianceKeychainQuery(kind, installationID string) keychain.Item {
	item := keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService(applianceSecretService)
	item.SetAccount(kind + ":" + applianceSecretID(installationID))
	item.SetAccessible(keychain.AccessibleWhenUnlockedThisDeviceOnly)
	return item
}

func (store *keychainApplianceSecrets) bootstrapPath(installationID, bootstrapID string) string {
	return filepath.Join(store.directory, applianceSecretID(installationID)+"-"+bootstrapID+".envelope")
}
