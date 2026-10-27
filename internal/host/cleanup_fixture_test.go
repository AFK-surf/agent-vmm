//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
)

type testGuestConnectionDialer struct{ connection *grpc.ClientConn }

func (value testGuestConnectionDialer) DialBoot(string) (*grpc.ClientConn, error) {
	return value.connection, nil
}

type cleanupTestManager struct{}

func (cleanupTestManager) Snapshot() *hostv1.VM {
	return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING}
}

type cleanupTestServices struct {
	*servicenet.Owner
	published *int
}

func (value cleanupTestServices) ReconcileEnvironment(context.Context, store.Environment) error {
	return nil
}
func (value cleanupTestServices) PublishSourcePoliciesLocked(context.Context, string) error {
	*value.published++
	return nil
}
func configureCleanupTestOwner(controller *Controller, published *int) {
	controller.cleanup = cleanup.New(cleanup.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: controller.manager, Inventory: &controller.inventory, Workloads: controller.workloads, Services: cleanupTestServices{controller.services, published}, Sessions: controller.sessions, HostShares: controller.hostShares, Egress: controller.egress})
	controller.cleanup.BindRemote(controller.remote)
	controller.remote.BindCleanup(controller.cleanup)
	controller.appliances.BindCleanup(controller.cleanup)
}
