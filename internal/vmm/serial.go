//go:build darwin || linux

package vmm

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

func rotateSerialLog(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return err
	}
	if info.Size() <= maxSerialLogBytes {
		return nil
	}
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()
	if _, err := input.Seek(-maxSerialLogBytes, 2); err != nil {
		return err
	}
	previous := path + ".previous"
	temporary, err := os.CreateTemp(filepath.Dir(path), ".serial-*.log")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(input, maxSerialLogBytes))
	if readErr != nil {
		temporary.Close()
		return readErr
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, previous); err != nil {
		return err
	}
	return os.Truncate(path, 0)
}
