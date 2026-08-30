//go:build darwin && (development || e2e)

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
)

type developmentFileProvider struct {
	path string
	mu   sync.Mutex
}

func NewDevelopmentFileProvider(path string) Provider { return &developmentFileProvider{path: path} }

func (provider *developmentFileProvider) Ensure(_ context.Context, revision uint64) ([]byte, string, error) {
	key, err := provider.key(revision, true)
	if err != nil {
		return nil, "", err
	}
	return elliptic.MarshalCompressed(elliptic.P256(), key.X, key.Y), "test", nil
}

func (provider *developmentFileProvider) Sign(_ context.Context, revision uint64, digest []byte) ([]byte, error) {
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

func (provider *developmentFileProvider) Delete(_ context.Context, revision uint64) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	err := os.Remove(provider.revisionPath(revision))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (provider *developmentFileProvider) key(revision uint64, create bool) (*ecdsa.PrivateKey, error) {
	if revision == 0 {
		return nil, ErrKeyUnavailable
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	path := provider.revisionPath(revision)
	if value, err := os.ReadFile(path); err == nil {
		return x509.ParseECPrivateKey(value)
	} else if !create || !errors.Is(err, os.ErrNotExist) {
		return nil, ErrKeyUnavailable
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
	if err := writeDevelopmentPrivateKey(path, value); err != nil {
		return nil, err
	}
	return key, nil
}

func (provider *developmentFileProvider) revisionPath(revision uint64) string {
	if revision == 1 {
		return provider.path
	}
	return provider.path + "." + strconv.FormatUint(revision, 10)
}

func writeDevelopmentPrivateKey(path string, value []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".development-device-root-*.tmp")
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
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
