//go:build darwin

package vmm

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
	"golang.org/x/sys/unix"
)

const processFDDirectory = "/dev/fd"

type platformConfig struct {
	hostDirectoryShares map[string]*vz.SharedDirectory
}
type darwinMachine struct {
	*vz.VirtualMachine
	states <-chan hostv1.VMState
}

func HostPhysicalMemoryBytes() (uint64, error) { return unix.SysctlUint64("hw.memsize") }

func (manager *Manager) startMachine(ctx context.Context, verified *bundle.Verified, serial *os.File, initializing bool, observation startObservation) (machineBackend, error) {
	configuration, err := manager.configuration(verified, serial, initializing, observation)
	if err != nil {
		return nil, err
	}
	valid, err := configuration.Validate()
	if !valid || err != nil {
		return nil, fmt.Errorf("validate VM configuration: valid=%t: %w", valid, err)
	}
	vm, err := vz.NewVirtualMachine(configuration)
	if err != nil {
		return nil, err
	}
	if err = vm.Start(); err != nil {
		return nil, err
	}
	states := make(chan hostv1.VMState, 1)
	go func() {
		defer close(states)
		for state := range vm.StateChangedNotify() {
			mapped := darwinMachineState(state)
			select {
			case states <- mapped:
			default:
				select {
				case <-states:
				default:
				}
				states <- mapped
			}
			if mapped == hostv1.VMState_VM_STATE_STOPPED || mapped == hostv1.VMState_VM_STATE_FAILED {
				return
			}
		}
	}()
	return &darwinMachine{VirtualMachine: vm, states: states}, nil
}

func darwinMachineState(state vz.VirtualMachineState) hostv1.VMState {
	switch state {
	case vz.VirtualMachineStateStopped:
		return hostv1.VMState_VM_STATE_STOPPED
	case vz.VirtualMachineStateRunning:
		return hostv1.VMState_VM_STATE_RUNNING
	case vz.VirtualMachineStateError:
		return hostv1.VMState_VM_STATE_FAILED
	default:
		return hostv1.VMState_VM_STATE_STARTING
	}
}

func (machine *darwinMachine) State() hostv1.VMState {
	return darwinMachineState(machine.VirtualMachine.State())
}

func (machine *darwinMachine) StateChangedNotify() <-chan hostv1.VMState { return machine.states }

func (machine *darwinMachine) Dial(ctx context.Context, port uint32) (net.Conn, error) {
	return awaitVMMDConnection(ctx, func() (net.Conn, error) { return machine.SocketDevices()[0].Connect(port) })
}

func (machine *darwinMachine) Listen(port uint32) (net.Listener, error) {
	return machine.SocketDevices()[0].Listen(port)
}

func (manager *Manager) preparePlatformState() (bool, bool, error) {
	initializing, err := prepareState(manager.config.InstanceDirectory, manager.config.StateBytes)
	return initializing, false, err
}

func (manager *Manager) abortMachineStart(machine machineBackend) bool {
	if machine.CanStop() {
		_ = machine.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = waitState(ctx, machine, hostv1.VMState_VM_STATE_STOPPED)
	return false
}
