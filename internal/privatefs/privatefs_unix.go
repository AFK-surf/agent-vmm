//go:build darwin || linux

package privatefs

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func EnsureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm() != 0o700 || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("private directory %s has owner %d and mode %s; require owner %d and chmod 700 %s", path, stat.Uid, info.Mode(), os.Geteuid(), path)
	}
	return nil
}

func ReadFile(path string, minimum, maximum int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if err := validateFile(path, info, minimum, maximum); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, opened) {
		return nil, fmt.Errorf("private file %s changed while opening", path)
	}
	if err := validateFile(path, opened, minimum, maximum); err != nil {
		return nil, err
	}
	value, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(value) < minimum || len(value) > maximum {
		clear(value)
		return nil, fmt.Errorf("private file %s has invalid size", path)
	}
	return value, nil
}

func validateFile(path string, info os.FileInfo, minimum, maximum int) error {
	stat := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 || info.Size() < int64(minimum) || info.Size() > int64(maximum) {
		return fmt.Errorf("private file %s has invalid owner, mode, link count, or size", path)
	}
	return nil
}

func ValidateAncestors(path string, uid int) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for current := filepath.Dir(absolute); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || (stat.Uid != 0 && int(stat.Uid) != uid) || info.Mode().Perm()&0o022 != 0 {
			return &os.PathError{Op: "validate ancestor", Path: current, Err: fmt.Errorf("unsafe owner %d or mode %s", stat.Uid, info.Mode())}
		}
		if current == string(filepath.Separator) {
			return nil
		}
	}
}

func ListenUnix(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return net.Listen("unix", path)
}
