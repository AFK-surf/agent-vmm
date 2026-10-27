//go:build darwin || linux

package appliance

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/remote"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
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
func configureCleanupTestOwner(controller *Owner, published *int) {
	controller.cleanup = cleanup.New(cleanup.Config{Store: controller.store, Guest: controller.guestConnection, Manager: controller.manager, Inventory: controller.inventory, Workloads: controller.workloads.(*workload.Owner), Services: cleanupTestServices{controller.services.(*servicenet.Owner), published}, Sessions: controller.sessions.(*session.Owner), HostShares: applianceTestShares{}, Egress: controller.egress})
	controller.cleanup.(*cleanup.Owner).BindRemote(controller.remote.(*remote.Owner))
	controller.remote.(*remote.Owner).BindCleanup(controller.cleanup.(*cleanup.Owner))
	controller.BindCleanup(controller.cleanup.(*cleanup.Owner))
}
