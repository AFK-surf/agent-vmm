//go:build darwin || linux

package trustkeys

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
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
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
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
	if err := writePrivateKey(path, value); err != nil {
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

func ensurePrivateDirectory(directory string) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0o700 || int(stat.Uid) != os.Geteuid() {
		return ErrKeyUnavailable
	}
	return nil
}

func readPrivateKey(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 {
		return nil, ErrKeyUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrKeyUnavailable
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, ErrKeyUnavailable
	}
	value, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(value) == 0 || len(value) > 1<<20 {
		return nil, ErrKeyUnavailable
	}
	return value, nil
}

func writePrivateKey(path string, value []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".device-root-*.tmp")
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
	if err := PublishSecret(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
