//go:build darwin || linux

package host

import "github.com/AFK-surf/agent-vmm/internal/host/cleanup"

func assembleCleanup(controller *Controller) *cleanup.Owner {
	owner := cleanup.New(cleanup.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: controller.manager, Inventory: &controller.inventory, Workloads: controller.workloads, Services: controller.services, Sessions: controller.sessions, HostShares: controller.hostShares, Egress: controller.egress})
	owner.BindRemote(controller.remote)
	return owner
}
