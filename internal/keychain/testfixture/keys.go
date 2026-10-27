package testfixture

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"math/big"
	"os"
	"sync"
)

type Keys struct {
	mu   sync.Mutex
	keys map[string]*ecdsa.PrivateKey
}

func NewKeys() *Keys { return &Keys{keys: make(map[string]*ecdsa.PrivateKey)} }
func (store *Keys) EnsureKey(_ context.Context, account, custody string) ([]byte, string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := store.keys[account]
	if key == nil {
		var err error
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, "", err
		}
		store.keys[account] = key
	}
	if custody == "" {
		custody = "keychain"
	}
	return elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y), custody, nil
}
func (store *Keys) Sign(_ context.Context, account string, digest []byte) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := store.keys[account]
	if key == nil {
		return nil, os.ErrNotExist
	}
	if len(digest) != 32 {
		return nil, errors.New("P-256 signing requires a SHA-256 digest")
	}
	r, s, err := ecdsa.Sign(rand.Reader, key, digest)
	if err != nil {
		return nil, err
	}
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if s.Cmp(halfOrder) > 0 {
		s.Sub(elliptic.P256().Params().N, s)
	}
	value := make([]byte, 64)
	r.FillBytes(value[:32])
	s.FillBytes(value[32:])
	return value, nil
}
func (store *Keys) DeleteKey(_ context.Context, account string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.keys, account)
	return nil
}
