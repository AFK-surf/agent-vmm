//go:build linux

package vmm

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/vmmprocess"
	"golang.org/x/sys/unix"
)

const processFDDirectory = "/proc/self/fd"

type platformConfig struct{}

func HostPhysicalMemoryBytes() (uint64, error) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, err
	}
	return info.Totalram * uint64(info.Unit), nil
}

func (manager *Manager) startMachine(ctx context.Context, verified *bundle.Verified, serial *os.File, initializing bool, observation startObservation) (machineBackend, error) {
	commandLine := "console=ttyS0 panic=-1 psi=1"
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
	artifacts := verified.Manifest.Artifacts
	return vmmprocess.Start(ctx, vmmprocess.Config{
		KernelPath: filepath.Join(verified.Directory, artifacts["kernel"].Path), InitramfsPath: filepath.Join(verified.Directory, artifacts["initramfs"].Path), RootFSPath: filepath.Join(verified.Directory, artifacts["rootfs"].Path),
		StatePath: filepath.Join(manager.config.InstanceDirectory, "state.raw"), LegacyStatePath: manager.config.LegacyStatePath, InstanceDirectory: manager.config.InstanceDirectory, SerialLog: serial.Name(), CommandLine: commandLine, GuestManifestDigest: manager.config.GuestManifestDigest, CPUCount: manager.config.CPUCount, MemoryBytes: manager.config.MemoryBytes,
	})
}

func (manager *Manager) preparePlatformState() (bool, bool, error) {
	attached, err := vmmprocess.Running(manager.config.InstanceDirectory)
	if err != nil {
		return false, false, err
	}
	if !attached {
		initializing, err := prepareState(manager.config.InstanceDirectory, manager.config.StateBytes)
		return initializing, false, err
	}
	_, err = os.Stat(filepath.Join(manager.config.InstanceDirectory, "state.initializing"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, true, err
	}
	return err == nil, true, nil
}

// abortMachineStart stops a VMM that this start spawned. A live adopted VMM
// keeps running with this Host's session, so the caller retains it for a forced
// Stop.
func (manager *Manager) abortMachineStart(machine machineBackend) bool {
	client := machine.(*vmmprocess.Client)
	if !client.Spawned() {
		state := client.State()
		return state != hostv1.VMState_VM_STATE_STOPPED && state != hostv1.VMState_VM_STATE_FAILED
	}
	_ = client.Stop()
	return false
}
