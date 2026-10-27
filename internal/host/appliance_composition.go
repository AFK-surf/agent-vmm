//go:build darwin || linux

package host

import (
	"log/slog"

	"github.com/AFK-surf/agent-vmm/internal/host/appliance"
)

func assembleAppliances(controller *Controller, secrets appliance.SecretStore) *appliance.Owner {
	var manager appliance.Manager
	if controller.manager != nil {
		manager = controller.manager
	}
	return appliance.New(appliance.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: manager, Inventory: &controller.inventory, Egress: controller.egress, Trust: controller.trust, TrustKeyProvider: controller.trustKeyProvider, SystemCapacity: controller.systemCapacity, SupportDirectory: controller.supportDirectory, RemoteComputeSocket: controller.remoteComputeSocket, Secrets: secrets, Logger: slog.Default()})
}
