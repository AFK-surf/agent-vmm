//go:build darwin || linux

package host

import (
	"context"
	"log/slog"

	"github.com/AFK-surf/agent-vmm/internal/host/remote"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func assembleRemote(controller *Controller, capacity resourcev2.Contract) *remote.Owner {
	return remote.New(remote.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: controller.manager, Inventory: &controller.inventory, Workloads: controller.workloads, Services: controller.services, Sessions: controller.sessions, Trust: controller.trust, Capacity: capacity, Credentials: controller.credentials, RemoteComputeSocket: controller.remoteComputeSocket, TrustSocket: controller.trustSocket, DataWorkerSocket: controller.dataWorkerSocket, ServiceTunnelSocket: controller.serviceTunnelSocket, MeshControlSocket: controller.meshControlSocket, Logger: slog.Default()})
}

func (controller *Controller) StartExternalConnectors(ctx context.Context) error {
	return controller.remote.StartExternalConnectors(ctx)
}
