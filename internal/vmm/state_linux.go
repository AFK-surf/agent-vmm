package vmm

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func restoreStateBacking(file *os.File, size int64, directory string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	allocated, err := allocatedBytes(info)
	if err != nil {
		return err
	}
	missing := uint64(size) - min(uint64(size), allocated)
	available, err := freeBytes(directory)
	if err != nil {
		return err
	}
	if available < uint64(hostEmergencyBytes) || available-uint64(hostEmergencyBytes) < missing {
		return errors.New("state disk requires restored physical backing plus 10 GiB host emergency space")
	}
	// Fallocate reserves unwritten extents without overwriting existing data.
	if err := allocateState(file, size); err != nil {
		return fmt.Errorf("restore state disk physical backing: %w", err)
	}
	return file.Sync()
}

func allocateState(file *os.File, size int64) error {
	return unix.Fallocate(int(file.Fd()), 0, 0, size)
}
