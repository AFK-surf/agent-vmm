//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// fileApplianceSecrets uses the Host service user's private state directory.
// Agent VMM treats that OS user as one failure domain; FileVault, rather than
// an unavailable login Keychain, owns offline protection for a headless daemon.
type fileApplianceSecrets struct{ directory string }

func newApplianceSecretStore(directory string) (applianceSecretStore, error) {
	directory = filepath.Join(directory, "appliance-secrets")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	if err := validateSecretDirectory(directory); err != nil {
		return nil, err
	}
	return &fileApplianceSecrets{directory: directory}, nil
}

func (store *fileApplianceSecrets) StoreBootstrap(_ context.Context, installationID, bootstrapID string, plaintext []byte) error {
	if len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return errors.New("opaque bootstrap seed is outside the allowed bound")
	}
	return writeSecretOnce(store.bootstrapPath(installationID, bootstrapID), plaintext)
}

func (store *fileApplianceSecrets) LoadBootstrap(_ context.Context, installationID, bootstrapID string) ([]byte, error) {
	path := store.bootstrapPath(installationID, bootstrapID)
	value, err := readSecret(path, 1, 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errBootstrapNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read protected bootstrap envelope: %w", err)
	}
	return value, nil
}

func (store *fileApplianceSecrets) DeleteBootstrap(_ context.Context, installationID, bootstrapID string) error {
	err := os.Remove(store.bootstrapPath(installationID, bootstrapID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.Join(err, syncHostDirectory(store.directory))
}

func (store *fileApplianceSecrets) DeleteInstallation(_ context.Context, installationID string) error {
	paths, err := filepath.Glob(filepath.Join(store.directory, applianceSecretID(installationID)+"-*.bootstrap"))
	if err != nil {
		return err
	}
	var result error
	for _, path := range paths {
		result = errors.Join(result, os.Remove(path))
	}
	return errors.Join(result, syncHostDirectory(store.directory))
}

func (store *fileApplianceSecrets) bootstrapPath(installationID, bootstrapID string) string {
	return filepath.Join(store.directory, applianceSecretID(installationID)+"-"+bootstrapID+".bootstrap")
}

func validateSecretDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0o700 || int(stat.Uid) != os.Geteuid() {
		return errors.New("appliance secret directory owner or mode is invalid")
	}
	return nil
}

func readSecret(path string, minimum, maximum int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 {
		return nil, fmt.Errorf("%w: secret owner, mode, or link count is invalid", errBootstrapStoreIntegrity)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("%w: secret changed while opening", errBootstrapStoreIntegrity)
	}
	value, err := io.ReadAll(io.LimitReader(file, int64(maximum+1)))
	if err != nil {
		return nil, err
	}
	if len(value) < minimum || len(value) > maximum {
		clearSecret(value)
		return nil, fmt.Errorf("%w: secret length is invalid", errBootstrapStoreIntegrity)
	}
	return value, nil
}

func writeSecretOnce(path string, value []byte) error {
	temporaryPath, err := writeSecretTemporary(filepath.Dir(path), value)
	if err != nil {
		return err
	}
	defer os.Remove(temporaryPath)
	if err := unix.RenameatxNp(unix.AT_FDCWD, temporaryPath, unix.AT_FDCWD, path, unix.RENAME_EXCL); err != nil {
		return err
	}
	return syncHostDirectory(filepath.Dir(path))
}

func writeSecretTemporary(directory string, value []byte) (string, error) {
	file, err := os.CreateTemp(directory, ".appliance-secret-*.tmp")
	if err != nil {
		return "", err
	}
	temporaryPath := file.Name()
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		os.Remove(temporaryPath)
		return "", err
	}
	if _, err := file.Write(value); err != nil {
		file.Close()
		os.Remove(temporaryPath)
		return "", err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		os.Remove(temporaryPath)
		return "", err
	}
	return temporaryPath, nil
}
