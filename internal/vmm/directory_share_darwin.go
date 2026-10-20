//go:build darwin

package vmm

import (
	"errors"
	"maps"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/Code-Hex/vz/v3"
)

func (manager *Manager) SetHostDirectoryShares(directories map[string]*vz.SharedDirectory) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if (manager.state != hostv1.VMState_VM_STATE_STOPPED && manager.state != hostv1.VMState_VM_STATE_FAILED) || manager.machine != nil {
		return errors.New("host directory shares require a stopped VM")
	}
	manager.hostDirectoryShares = maps.Clone(directories)
	return nil
}
