//go:build darwin || linux

package trustkeys

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
)

type fileProvider struct {
	path string
	mu   sync.Mutex
}

func NewFileProvider(path string) Provider {
	return &fileProvider{path: path}
}

func (provider *fileProvider) Ensure(_ context.Context, revision uint64) ([]byte, string, error) {
	key, err := provider.key(revision, true)
	if err != nil {
		return nil, "", err
	}
	return elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y), "file", nil
}

func (provider *fileProvider) Sign(_ context.Context, revision uint64, digest []byte) ([]byte, error) {
	if len(digest) != 32 {
		return nil, errors.New("P-256 signing requires a SHA-256 digest")
	}
	key, err := provider.key(revision, false)
	if err != nil {
		return nil, err
	}
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

func (provider *fileProvider) Delete(_ context.Context, revision uint64) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	err := os.Remove(provider.revisionPath(revision))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (provider *fileProvider) key(revision uint64, create bool) (*ecdsa.PrivateKey, error) {
	if revision == 0 {
		return nil, ErrKeyUnavailable
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	path := provider.revisionPath(revision)
	if value, err := readPrivateKey(path); err == nil {
		return x509.ParseECPrivateKey(value)
	} else if !create || !errors.Is(err, os.ErrNotExist) {
		return nil, ErrKeyUnavailable
	}
	if err := privatefs.EnsureDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	value, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	if err := atomicfile.PublishOnce(path, value, 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

func (provider *fileProvider) revisionPath(revision uint64) string {
	if revision == 1 {
		return provider.path
	}
	return provider.path + "." + strconv.FormatUint(revision, 10)
}

func readPrivateKey(path string) ([]byte, error) {
	return privatefs.ReadFile(path, 1, 1<<20)
}
