//go:build darwin || linux

package hostapp

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
)

func ensureConnectorCapability(path string) ([]byte, error) {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("connector trust capability must be a regular file")
		}
		value, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(value) < 32 {
			return nil, errors.New("connector trust capability is invalid")
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, err
		}
		return value, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	randomValue := make([]byte, 32)
	if _, err := rand.Read(randomValue); err != nil {
		return nil, err
	}
	value := []byte(base64.RawURLEncoding.EncodeToString(randomValue))
	temporary, err := os.CreateTemp(filepath.Dir(path), ".connector-trust-*")
	if err != nil {
		return nil, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return nil, err
	}
	if _, err := temporary.Write(value); err != nil {
		temporary.Close()
		return nil, err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return nil, err
	}
	if err := temporary.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return nil, err
	}
	return value, nil
}
