//go:build darwin || linux

package remoteconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type fileCredentialStore struct{ directory string }

func NewCredentialStore(directory string) CredentialStore {
	return &fileCredentialStore{directory: directory}
}

func (store *fileCredentialStore) Load(_ context.Context, account string) ([]byte, error) {
	path := store.path(account)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrCredentialUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("load controller credential: %w", err)
	}
	stat, ok := infoSysStat(info)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 {
		return nil, ErrCredentialUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrCredentialUnavailable
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, ErrCredentialUnavailable
	}
	value, err := io.ReadAll(io.LimitReader(file, int64(MaxCredentialBytes+1)))
	if err != nil || len(value) == 0 || len(value) > MaxCredentialBytes {
		return nil, ErrCredentialUnavailable
	}
	return value, nil
}

func (store *fileCredentialStore) Replace(_ context.Context, account string, credential []byte) error {
	if len(credential) == 0 || len(credential) > MaxCredentialBytes {
		return errors.New("credential size is invalid")
	}
	if err := ensureCredentialDirectory(store.directory); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(store.directory, ".credential-*")
	if err != nil {
		return fmt.Errorf("create controller credential: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(credential)
	}
	if syncErr := temporary.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write controller credential: %w", err)
	}
	if err := os.Rename(temporaryName, store.path(account)); err != nil {
		return fmt.Errorf("commit controller credential: %w", err)
	}
	return syncCredentialDirectory(store.directory)
}

func (store *fileCredentialStore) Delete(_ context.Context, account string) error {
	err := os.Remove(store.path(account))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *fileCredentialStore) path(account string) string {
	digest := sha256.Sum256([]byte(account))
	return filepath.Join(store.directory, hex.EncodeToString(digest[:16])+".credential")
}

func ensureCredentialDirectory(directory string) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create controller credential directory: %w", err)
	}
	info, err := os.Lstat(directory)
	stat, ok := infoSysStat(info)
	if err != nil || !ok || !info.IsDir() || info.Mode().Perm() != 0o700 || int(stat.Uid) != os.Geteuid() {
		return errors.New("controller credential directory owner or mode is invalid")
	}
	return nil
}

func infoSysStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func syncCredentialDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
