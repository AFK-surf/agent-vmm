//go:build darwin

package vmm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
	"golang.org/x/sys/unix"
)

const hostEmergencyBytes int64 = 10 << 30

func (manager *Manager) configuration(verified *bundle.Verified, serial *os.File, initializing bool) (*vz.VirtualMachineConfiguration, error) {
	artifact := verified.Manifest.Artifacts
	commandLine := "console=hvc0 panic=-1"
	if initializing {
		commandLine += " agent.state_init=1"
	}
	bootloader, err := vz.NewLinuxBootLoader(filepath.Join(verified.Directory, artifact["kernel"].Path), vz.WithInitrd(filepath.Join(verified.Directory, artifact["initramfs"].Path)), vz.WithCommandLine(commandLine))
	if err != nil {
		return nil, err
	}
	configuration, err := vz.NewVirtualMachineConfiguration(bootloader, manager.config.CPUCount, manager.config.MemoryBytes)
	if err != nil {
		return nil, err
	}
	serialAttachment, err := vz.NewFileSerialPortAttachment(serial.Name(), true)
	if err != nil {
		return nil, err
	}
	serialPort, err := vz.NewVirtioConsoleDeviceSerialPortConfiguration(serialAttachment)
	if err != nil {
		return nil, err
	}
	configuration.SetSerialPortsVirtualMachineConfiguration([]*vz.VirtioConsoleDeviceSerialPortConfiguration{serialPort})
	root, err := blockDevice(filepath.Join(verified.Directory, artifact["rootfs"].Path), true, "AGENT_ROOT")
	if err != nil {
		return nil, err
	}
	state, err := blockDevice(filepath.Join(manager.config.InstanceDirectory, "state.raw"), false, "AGENT_STATE")
	if err != nil {
		return nil, err
	}
	storage := []vz.StorageDeviceConfiguration{root, state}
	manager.mu.Lock()
	blocks := append([]ApplianceBlock(nil), manager.applianceBlocks...)
	manager.mu.Unlock()
	for _, block := range blocks {
		device, err := blockDevice(block.Path, false, block.DeviceIdentifier)
		if err != nil {
			return nil, err
		}
		storage = append(storage, device)
	}
	configuration.SetStorageDevicesVirtualMachineConfiguration(storage)
	entropy, err := vz.NewVirtioEntropyDeviceConfiguration()
	if err != nil {
		return nil, err
	}
	configuration.SetEntropyDevicesVirtualMachineConfiguration([]*vz.VirtioEntropyDeviceConfiguration{entropy})
	balloon, err := vz.NewVirtioTraditionalMemoryBalloonDeviceConfiguration()
	if err != nil {
		return nil, err
	}
	configuration.SetMemoryBalloonDevicesVirtualMachineConfiguration([]vz.MemoryBalloonDeviceConfiguration{balloon})
	socket, err := vz.NewVirtioSocketDeviceConfiguration()
	if err != nil {
		return nil, err
	}
	configuration.SetSocketDevicesVirtualMachineConfiguration([]vz.SocketDeviceConfiguration{socket})
	return configuration, nil
}

func blockDevice(path string, readOnly bool, identifier string) (*vz.VirtioBlockDeviceConfiguration, error) {
	attachment, err := vz.NewDiskImageStorageDeviceAttachment(path, readOnly)
	if err != nil {
		return nil, err
	}
	device, err := vz.NewVirtioBlockDeviceConfiguration(attachment)
	if err != nil {
		return nil, err
	}
	if err := device.SetBlockDeviceIdentifier(identifier); err != nil {
		return nil, err
	}
	return device, nil
}

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
		allocation := &unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: newStateBytes}
		if err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, allocation); err != nil {
			file.Close()
			return false, fmt.Errorf("fully allocate state disk: %w", err)
		}
		if err := file.Truncate(newStateBytes); err != nil {
			file.Close()
			return false, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return false, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return false, err
		}
		allocated, err := allocatedBytes(info)
		if err != nil {
			file.Close()
			return false, err
		}
		if allocated < uint64(newStateBytes) {
			file.Close()
			return false, errors.New("state disk is not fully physically backed")
		}
		if err := file.Close(); err != nil {
			return false, err
		}
		freeAfter, err := freeBytes(directory)
		if err != nil {
			return false, err
		}
		if freeAfter < uint64(hostEmergencyBytes) {
			return false, errors.New("state disk allocation consumed host emergency space")
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
	allocated, err := allocatedBytes(info)
	if err != nil {
		return false, err
	}
	holeBytes, err := stateHoleBytes(file, size)
	if err != nil {
		return false, err
	}
	if holeBytes > 0 {
		freeBefore, err := freeBytes(directory)
		if err != nil {
			return false, err
		}
		if holeBytes > ^uint64(0)-uint64(hostEmergencyBytes) || freeBefore < holeBytes+uint64(hostEmergencyBytes) {
			return false, errors.New("state disk requires restored physical backing plus 10 GiB host emergency space")
		}
		if err := materializeStateHoles(file, size); err != nil {
			return false, fmt.Errorf("restore state disk physical backing: %w", err)
		}
		if err := file.Sync(); err != nil {
			return false, err
		}
		info, err = file.Stat()
		if err != nil {
			return false, err
		}
		allocated, err = allocatedBytes(info)
		remainingHoles, holeErr := stateHoleBytes(file, size)
		if err != nil || holeErr != nil || allocated < uint64(size) || remainingHoles != 0 {
			return false, errors.New("state disk physical backing could not be restored")
		}
	}
	if allocated < uint64(size) {
		return false, errors.New("state disk is not fully physically backed")
	}
	freeAfter, err := freeBytes(directory)
	if err != nil {
		return false, err
	}
	if freeAfter < uint64(hostEmergencyBytes) {
		return false, errors.New("state disk backing restoration consumed host emergency space")
	}
	_, err = os.Stat(markerPath)
	return err == nil, nil
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

func parseKernelCommandLine(data string) map[string]string {
	result := map[string]string{}
	for _, field := range strings.Fields(data) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}
