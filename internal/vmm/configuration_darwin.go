//go:build darwin

package vmm

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
)

const hostDirectoryShareTag = "agent.hostshares"

func (manager *Manager) configuration(verified *bundle.Verified, serial *os.File, initializing bool, observation startObservation) (*vz.VirtualMachineConfiguration, error) {
	artifact := verified.Manifest.Artifacts
	commandLine := "console=hvc0 panic=-1 psi=1"
	if initializing {
		commandLine += " agent.state_init=1"
	}
	if manager.config.LegacyStatePath != "" {
		if !initializing {
			return nil, errors.New("state migration requires a new target disk")
		}
		commandLine += " agent.state_migrate=1"
	}
	if kernelArgument(observation.OperationID) {
		commandLine += " agent.operation_id=" + observation.OperationID
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
	if manager.config.LegacyStatePath != "" {
		legacy, err := blockDevice(manager.config.LegacyStatePath, true, "AGENT_STATE_LEGACY")
		if err != nil {
			return nil, err
		}
		storage = append(storage, legacy)
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
	sharing, err := vz.NewVirtioFileSystemDeviceConfiguration(hostDirectoryShareTag)
	if err != nil {
		return nil, err
	}
	share, err := vz.NewMultipleDirectoryShare(manager.hostDirectoryShares)
	if err != nil {
		return nil, err
	}
	sharing.SetDirectoryShare(share)
	configuration.SetDirectorySharingDevicesVirtualMachineConfiguration([]vz.DirectorySharingDeviceConfiguration{sharing})
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
