package atomicfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// ReplaceFile writes a complete file and syncs its directory after replacement.
func ReplaceFile(path string, data []byte, perm fs.FileMode) error {
	return publish(path, data, perm, false)
}

// PublishOnce publishes a complete file, accepting an identical existing file.
func PublishOnce(path string, data []byte, perm fs.FileMode) error {
	return publish(path, data, perm, true)
}

func publish(path string, data []byte, perm fs.FileMode, once bool) error {
	parent := filepath.Dir(path)
	file, err := os.CreateTemp(parent, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(perm); err != nil {
		file.Close()
		return err
	}
	count, err := file.Write(data)
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	if once {
		if err := os.Link(temporary, path); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				return err
			}
			current, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if !bytes.Equal(current, data) {
				return fmt.Errorf("publish %s: contents differ: %w", path, fs.ErrExist)
			}
		}
	} else if err := os.Rename(temporary, path); err != nil {
		return err
	}
	if err := os.Remove(temporary); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	directory, err := os.Open(parent)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
