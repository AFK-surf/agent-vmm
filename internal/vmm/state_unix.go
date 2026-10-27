//go:build darwin || linux

package vmm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/hostdisk"
	"golang.org/x/sys/unix"
)

const hostEmergencyBytes = hostdisk.EmergencyBytes

func prepareState(directory string, newStateBytes int64) (bool, error) {
	statePath := filepath.Join(directory, "state.raw")
	markerPath := filepath.Join(directory, "state.initializing")
	partialPath := statePath + ".partial"
	if err := os.Remove(partialPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if _, err := os.Lstat(statePath); errors.Is(err, os.ErrNotExist) {
		return prepareNewState(directory, partialPath, statePath, markerPath, newStateBytes)
	} else if err != nil {
		return false, err
	}
	fd, err := unix.Open(statePath, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, errors.New("existing state disk is unavailable")
	}
	file := os.NewFile(uintptr(fd), statePath)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return false, errors.New("existing state disk has unexpected size")
	}
	size := info.Size()
	if err := restoreStateBacking(file, size, directory); err != nil {
		return false, err
	}
	_, err = os.Stat(markerPath)
	return err == nil, nil
}

func prepareNewState(directory, partialPath, statePath, markerPath string, size int64) (bool, error) {
	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return false, err
	}
	defer file.Close()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(partialPath)
		}
	}()
	free, err := freeBytes(directory)
	if err != nil {
		return false, err
	}
	if free < uint64(size+hostEmergencyBytes) {
		return false, errors.New("state disk requires full physical backing plus 10 GiB host emergency space")
	}
	if err := allocateState(file, size); err != nil {
		return false, fmt.Errorf("fully allocate state disk: %w", err)
	}
	if err := file.Sync(); err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	marker, err := os.OpenFile(markerPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return false, err
	}
	if err := marker.Sync(); err != nil {
		marker.Close()
		return false, err
	}
	if err := marker.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(partialPath, statePath); err != nil {
		return false, err
	}
	published = true
	if err := syncDirectory(directory); err != nil {
		return false, err
	}
	return true, nil
}

func freeBytes(path string) (uint64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, err
	}
	return stats.Bavail * uint64(stats.Bsize), nil
}

func allocatedBytes(info os.FileInfo) (uint64, error) {
	stats, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stats.Blocks < 0 {
		return 0, errors.New("state disk allocation metadata is unavailable")
	}
	return uint64(stats.Blocks) * 512, nil
}

func finishStateInitialization(directory string) error {
	if err := os.Remove(filepath.Join(directory, "state.initializing")); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
