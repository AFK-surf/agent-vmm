package testfixture

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math/big"

	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
)

type MemoryKeys struct {
	Keys        map[uint64]*ecdsa.PrivateKey
	Unavailable map[uint64]bool
}

func NewMemoryKeys() *MemoryKeys {
	return &MemoryKeys{Keys: make(map[uint64]*ecdsa.PrivateKey), Unavailable: make(map[uint64]bool)}
}

func (provider *MemoryKeys) Ensure(_ context.Context, revision uint64) ([]byte, string, error) {
	if provider.Unavailable[revision] {
		return nil, "", trustkeys.ErrKeyUnavailable
	}
	key := provider.Keys[revision]
	if key == nil {
		var err error
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, "", err
		}
		provider.Keys[revision] = key
	}
	return elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y), "test", nil
}

func (provider *MemoryKeys) Sign(_ context.Context, revision uint64, digest []byte) ([]byte, error) {
	if provider.Unavailable[revision] || provider.Keys[revision] == nil {
		return nil, trustkeys.ErrKeyUnavailable
	}
	return SignDigest(provider.Keys[revision], digest)
}

func (provider *MemoryKeys) Delete(_ context.Context, revision uint64) error {
	delete(provider.Keys, revision)
	return nil
}

func SignDigest(key *ecdsa.PrivateKey, digest []byte) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, key, digest)
	if err != nil {
		return nil, err
	}
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if s.Cmp(halfOrder) > 0 {
		s.Sub(elliptic.P256().Params().N, s)
	}
	result := make([]byte, 64)
	r.FillBytes(result[:32])
	s.FillBytes(result[32:])
	return result, nil
}
