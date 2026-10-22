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
	file, err := os.OpenFile(statePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		removeIncomplete := true
		defer func() {
			if removeIncomplete {
				_ = os.Remove(statePath)
				_ = os.Remove(markerPath)
			}
		}()
		freeBefore, err := freeBytes(directory)
		if err != nil {
			file.Close()
			return false, err
		}
		if freeBefore < uint64(newStateBytes+hostEmergencyBytes) {
			file.Close()
			return false, errors.New("state disk requires full physical backing plus 10 GiB host emergency space")
		}
		if err := allocateState(file, newStateBytes); err != nil {
			file.Close()
			return false, fmt.Errorf("fully allocate state disk: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return false, err
		}
		if err := file.Close(); err != nil {
			return false, err
		}
		marker, err := os.OpenFile(markerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return false, err
		}
		if err := marker.Close(); err != nil {
			return false, err
		}
		if err := syncDirectory(directory); err != nil {
			return false, err
		}
		removeIncomplete = false
		return true, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return false, err
	}
	fd, err := unix.Open(statePath, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, errors.New("existing state disk is unavailable")
	}
	file = os.NewFile(uintptr(fd), statePath)
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
