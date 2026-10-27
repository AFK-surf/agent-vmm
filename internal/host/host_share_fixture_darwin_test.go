package host

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/hostshare"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"github.com/Code-Hex/vz/v3"
)

type hostShareTestManager struct {
	manager     *vmm.Manager
	snapshot    func() *hostv1.VM
	directories map[string]*vz.SharedDirectory
}

func (manager *hostShareTestManager) Snapshot() *hostv1.VM {
	if manager.snapshot != nil {
		return manager.snapshot()
	}
	if manager.manager != nil {
		return manager.manager.Snapshot()
	}
	return &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
}
func (manager *hostShareTestManager) HasMachineAttachment() bool {
	return manager.manager != nil && manager.manager.HasMachineAttachment()
}
func (manager *hostShareTestManager) SetHostDirectoryShares(directories map[string]*vz.SharedDirectory) error {
	manager.directories = directories
	if manager.manager != nil {
		return manager.manager.SetHostDirectoryShares(directories)
	}
	return nil
}

func setupTestHostShareOwner(controller *Controller, snapshot func() *hostv1.VM) *hostShareTestManager {
	manager := &hostShareTestManager{manager: controller.manager, snapshot: snapshot}
	controller.hostShares = hostshare.New(hostshare.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: manager, Inventory: &controller.inventory})
	controller.workloads = assembleWorkloads(controller)
	controller.hostShares.BindWorkloads(controller.workloads)
	controller.services.BindWorkloads(controller.workloads)
	controller.remote = assembleRemote(controller, controller.remote.Capacity().Resources())
	controller.cleanup = assembleCleanup(controller)
	controller.remote.BindCleanup(controller.cleanup)
	if controller.vmLifecycle != nil {
		controller.remote.BindLifecycle(controller.vmLifecycle)
	}
	controller.appliances.BindRuntime(controller.services, controller.sessions, controller.workloads)
	controller.appliances.BindRemote(controller.remote)
	controller.appliances.BindCleanup(controller.cleanup)
	return manager
}
