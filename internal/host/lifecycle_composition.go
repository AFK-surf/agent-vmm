//go:build darwin || linux

package host

import (
	"log/slog"

	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func assembleVMLifecycle(controller *Controller, manager *vmm.Manager) *hostlifecycle.Owner {
	return hostlifecycle.New(hostlifecycle.Config{Manager: manager, Store: controller.store, Guest: &controller.guestConnection, Inventory: &controller.inventory, Workloads: controller.workloads, Services: controller.services, Remote: controller.remote, Sessions: controller.sessions, Appliances: controller.appliances, HostShares: controller.hostShares, ApplianceStarter: controller.appliances, Logger: slog.Default()})
}
