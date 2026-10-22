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
	holeBytes, err := stateHoleBytes(file, size)
	if err != nil {
		return err
	}
	if holeBytes > 0 {
		freeBefore, err := freeBytes(directory)
		if err != nil {
			return err
		}
		if holeBytes > ^uint64(0)-uint64(hostEmergencyBytes) || freeBefore < holeBytes+uint64(hostEmergencyBytes) {
			return errors.New("state disk requires restored physical backing plus 10 GiB host emergency space")
		}
		if err := materializeStateHoles(file, size); err != nil {
			return fmt.Errorf("restore state disk physical backing: %w", err)
		}
		if err := file.Sync(); err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			return err
		}
		allocated, err = allocatedBytes(info)
		remainingHoles, holeErr := stateHoleBytes(file, size)
		if err != nil || holeErr != nil || allocated < uint64(size) || remainingHoles != 0 {
			return errors.New("state disk physical backing could not be restored")
		}
	}
	if allocated < uint64(size) {
		return errors.New("state disk is not fully physically backed")
	}
	return nil
}

func stateHoleBytes(file *os.File, size int64) (uint64, error) {
	var total uint64
	for offset := int64(0); offset < size; {
		hole, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_HOLE)
		if errors.Is(err, unix.ENXIO) {
			return 0, errors.New("state disk logical extent map is unavailable")
		}
		if err != nil {
			return 0, err
		}
		if hole >= size {
			return total, nil
		}
		if hole < offset {
			return 0, errors.New("state disk hole map moved backwards")
		}
		data, err := unix.Seek(int(file.Fd()), hole, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			data = size
		} else if err != nil {
			return 0, err
		}
		if data > size {
			data = size
		}
		if data <= hole {
			return 0, errors.New("state disk hole map is invalid")
		}
		length := uint64(data - hole)
		if length > ^uint64(0)-total {
			return 0, errors.New("state disk hole accounting overflow")
		}
		total += length
		offset = data
	}
	return total, nil
}

func materializeStateHoles(file *os.File, size int64) error {
	zeros := make([]byte, 4<<20)
	for offset := int64(0); offset < size; {
		hole, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_HOLE)
		if errors.Is(err, unix.ENXIO) {
			return errors.New("state disk logical extent map is unavailable")
		}
		if hole >= size {
			return nil
		}
		if err != nil {
			return err
		}
		if hole < offset {
			return errors.New("state disk hole map moved backwards")
		}
		data, err := unix.Seek(int(file.Fd()), hole, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			data = size
		} else if err != nil {
			return err
		}
		if data > size {
			data = size
		}
		if data <= hole {
			return errors.New("state disk hole map is invalid")
		}
		for position := hole; position < data; {
			length := min(int64(len(zeros)), data-position)
			written, err := file.WriteAt(zeros[:length], position)
			if err != nil {
				return err
			}
			if int64(written) != length {
				return errors.New("short write while restoring state disk physical backing")
			}
			position += length
		}
		offset = data
	}
	return nil
}

// allocateState preallocates a new disk and sets its size. Preallocation past
// EOF does not change the size, so the result is checked after the truncate.
func allocateState(file *os.File, size int64) error {
	if err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, &unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: size}); err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	allocated, err := allocatedBytes(info)
	if err != nil {
		return err
	}
	if allocated < uint64(size) {
		return errors.New("state disk is not fully physically backed")
	}
	return nil
}
