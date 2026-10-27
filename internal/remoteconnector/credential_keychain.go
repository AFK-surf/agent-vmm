package remoteconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/AFK-surf/agent-vmm/internal/keychain"
)

type keychainCredentialStore struct{ backend keychain.PasswordStore }

func NewKeychainCredentialStore(backend keychain.PasswordStore) CredentialStore {
	return &keychainCredentialStore{backend: backend}
}

func credentialName(account string) string {
	digest := sha256.Sum256([]byte(account))
	return hex.EncodeToString(digest[:16]) + ".credential"
}

func (store *keychainCredentialStore) Load(ctx context.Context, account string) ([]byte, error) {
	value, err := store.backend.Get(ctx, credentialName(account))
	if err != nil {
		return nil, errors.Join(ErrCredentialUnavailable, err)
	}
	return value, nil
}

func (store *keychainCredentialStore) Replace(ctx context.Context, account string, credential []byte) error {
	if len(credential) == 0 || len(credential) > MaxCredentialBytes {
		return errors.New("credential size is invalid")
	}
	return store.backend.Replace(ctx, credentialName(account), credential)
}

func (store *keychainCredentialStore) Delete(ctx context.Context, account string) error {
	return store.backend.Delete(ctx, credentialName(account))
}
