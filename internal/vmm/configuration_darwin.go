//go:build darwin

package vmm

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
)

func (manager *Manager) configuration(verified *bundle.Verified, serial *os.File, initializing bool) (*vz.VirtualMachineConfiguration, error) {
	artifact := verified.Manifest.Artifacts
	commandLine := "console=hvc0 panic=-1 agent.manifest_sha256=" + verified.ManifestDigest
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
	serialAttachment, err := vz.NewFileHandleSerialPortAttachment(serial, serial)
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

func prepareState(directory string, size int64) (bool, error) {
	statePath := filepath.Join(directory, "state.raw")
	markerPath := filepath.Join(directory, "state.initializing")
	file, err := os.OpenFile(statePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		if err := file.Truncate(size); err != nil {
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
		return true, syncDirectory(directory)
	}
	if !errors.Is(err, os.ErrExist) {
		return false, err
	}
	info, err := os.Stat(statePath)
	if err != nil || info.Size() != size {
		return false, errors.New("existing state disk has unexpected size")
	}
	_, err = os.Stat(markerPath)
	return err == nil, nil
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
