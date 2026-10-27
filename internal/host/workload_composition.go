//go:build darwin || linux

package host

import (
	"log/slog"

	"github.com/AFK-surf/agent-vmm/internal/host/workload"
)

func assembleWorkloads(controller *Controller) *workload.Owner {
	return workload.New(workload.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: controller.manager, Projection: controller.services.ProjectionLock(), Services: controller.services, HostShares: controller.hostShares, Logger: slog.Default()})
}
