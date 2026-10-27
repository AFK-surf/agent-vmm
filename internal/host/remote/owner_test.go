//go:build darwin || linux

package remote

import (
	"context"
	"log/slog"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

type testLifecycle struct{ open bool }

func (value *testLifecycle) AdmitsRemoteWork() bool { return value.open }

type testSessions struct{ Sessions }

func (testSessions) ActiveSessions(string) uint32                         { return 0 }
func (testSessions) AllocationSessionIDs(string, string, uint64) []string { return nil }

type testServices struct {
	Services
	projection   locks.RWMutex
	publishError error
}

func (value *testServices) ProjectionLock() *locks.RWMutex { return &value.projection }
func (value *testServices) PublishSourcePoliciesLocked(context.Context, string) error {
	return value.publishError
}

type testWorkloads struct {
	Workloads
	environments locks.Keyed[string]
	locking      chan string
}

func (value *testWorkloads) LockEnvironment(ctx context.Context, id string) (func(), error) {
	if value.locking != nil {
		value.locking <- id
	}
	return value.environments.Lock(ctx, id)
}
func (*testWorkloads) StopWorkloadContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, _ string, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	return client.StopContainer(ctx, request)
}
func (*testWorkloads) ForgetContainers(string) {}

func newTestOwner(owner *Owner) *Owner {
	if owner.guestConnection == nil {
		owner.guestConnection = &guestconn.Connection{}
	}
	if owner.inventory == nil {
		owner.inventory = &lifecycle.InventoryFence{}
	}
	if owner.workloads == nil {
		owner.workloads = &testWorkloads{}
	}
	if owner.services == nil {
		owner.services = &testServices{}
	}
	if owner.sessions == nil {
		owner.sessions = testSessions{}
	}
	if owner.logger == nil {
		owner.logger = slog.Default()
	}
	if owner.remoteConnectionEpochs == nil {
		owner.remoteConnectionEpochs = make(map[string]uint64)
	}
	if owner.domainMetrics == nil {
		owner.domainMetrics = make(map[string]*controllerDomainMetricState)
	}
	if owner.externalConnectors == nil {
		owner.externalConnectors = make(map[string]*externalConnectorRoute)
	}
	if owner.allocationRetirementWake == nil {
		owner.allocationRetirementWake = make(chan struct{}, 1)
	}
	if owner.externalConnectorRunner == nil {
		owner.externalConnectorRunner = remoteconnector.Run
	}
	return owner
}
func openTestStore(path string) (*store.Store, error) { return store.Open(path) }
