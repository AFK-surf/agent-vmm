//go:build darwin || linux

package host

import (
	"log/slog"

	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
)

func assembleServices(controller *Controller) *servicenet.Owner {
	return servicenet.New(servicenet.Config{Store: controller.store, Manager: controller.manager, Guest: &controller.guestConnection, Egress: controller.egress, Trust: controller.trust, ServiceDirectory: controller.serviceDirectory, DataWorkerSocket: controller.dataWorkerSocket, Sources: controller.appliances, DisablePersonalMeshPairing: controller.personalMeshPairingDisabled, Logger: slog.Default()})
}
