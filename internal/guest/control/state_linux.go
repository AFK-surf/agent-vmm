//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
)

func directoryEntries(path string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return entries, err
}

func removeNamespaceState(namespaceID string) error {
	for _, path := range []string{
		volumePath(namespaceID, ""),
		workspacePath(namespaceID),
		filepath.Join(stateRoot, "logs", namespaceID),
		filepath.Join(stateRoot, "build-cache", namespaceID),
		filepath.Join(stateRoot, "build-tmp", namespaceID),
	} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}
