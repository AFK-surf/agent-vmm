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
		volumeGenerationPath(namespaceID, ""),
		workspacePath(namespaceID),
		filepath.Join(namespaceStateRoot(namespaceID), "logs", namespaceID),
		filepath.Join(namespaceStateRoot(namespaceID), "build-cache", namespaceID),
		filepath.Join(namespaceStateRoot(namespaceID), "build-tmp", namespaceID),
		filepath.Join(namespaceStateRoot(namespaceID), "imports", namespaceID),
	} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}
