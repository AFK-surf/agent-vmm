package trustkeys

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/AFK-surf/agent-vmm/internal/keychain"
)

type keychainProvider struct {
	backend keychain.KeyStore
	custody string
	mu      sync.Mutex
}

func NewKeychainProvider(backend keychain.KeyStore, custody string) Provider {
	return &keychainProvider{backend: backend, custody: custody}
}

func keyAccount(revision uint64) string {
	if revision == 1 {
		return "device-root.key"
	}
	return "device-root.key." + strconv.FormatUint(revision, 10)
}

func (provider *keychainProvider) Ensure(ctx context.Context, revision uint64) ([]byte, string, error) {
	if revision == 0 {
		return nil, "", ErrKeyUnavailable
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	public, custody, err := provider.backend.EnsureKey(ctx, keyAccount(revision), provider.custody)
	if err != nil {
		return nil, "", errors.Join(ErrKeyUnavailable, err)
	}
	provider.custody = custody
	return public, custody, nil
}

func (provider *keychainProvider) Sign(ctx context.Context, revision uint64, digest []byte) ([]byte, error) {
	value, err := provider.backend.Sign(ctx, keyAccount(revision), digest)
	if err != nil {
		return nil, errors.Join(ErrKeyUnavailable, err)
	}
	return value, nil
}

func (provider *keychainProvider) Delete(ctx context.Context, revision uint64) error {
	return provider.backend.DeleteKey(ctx, keyAccount(revision))
}
