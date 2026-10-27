//go:build darwin || linux

package appliance

import (
	"context"
	"github.com/AFK-surf/agent-vmm/internal/host/testfixture"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"log/slog"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/remote"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc"
)

func openTestStore(path string) (*store.Store, error) { return store.Open(path) }

type applianceTestShares struct{}

func (applianceTestShares) AuthorizeHostShareMounts(context.Context, string, []*vmmdv1.VolumeMount) error {
	return nil
}
func (applianceTestShares) ReconcileHostShares(context.Context, vmmdv1.VMMDServiceClient) error {
	return nil
}
func (applianceTestShares) SettleEnvironmentHostShares(context.Context, string) error { return nil }

type applianceTestRuntime struct{}

func (applianceTestRuntime) RuntimeService(context.Context) hostv1.AgentRuntimeServiceServer {
	return &hostv1.UnimplementedAgentRuntimeServiceServer{}
}
func newTestOwner(owner *Owner) *Owner {
	if owner.trustKeyProvider == nil {
		owner.trustKeyProvider = testfixture.NewMemoryKeys()
	}
	if owner.store != nil && owner.trust == nil {
		var err error
		owner.trust, err = hosttrust.New(context.Background(), owner.store, owner.trustKeyProvider)
		if err != nil {
			panic(err)
		}
	}

	if owner.guestConnection == nil {
		owner.guestConnection = &guestconn.Connection{}
	}
	if owner.inventory == nil {
		owner.inventory = &hostlifecycle.InventoryFence{}
	}
	if owner.egress == nil {
		owner.egress = egress.New()
	}
	if owner.logger == nil {
		owner.logger = slog.Default()
	}
	if owner.applianceHealth == nil {
		owner.applianceHealth = make(map[string]*appliancev1.ApplianceHealth)
	}
	if owner.pendingApplianceHealth == nil {
		owner.pendingApplianceHealth = make(map[string]*appliancev1.ApplianceHealth)
	}
	if owner.applianceTrustRetry == nil {
		owner.applianceTrustRetry = make(map[string]time.Time)
	}
	if owner.privateControllers == nil {
		owner.privateControllers = make(map[string]*privateControllerRoute)
	}
	if owner.applianceFrontends == nil {
		owner.applianceFrontends = make(map[string]*applianceFrontendRuntime)
	}
	if owner.services == nil {
		owner.services = servicenet.New(servicenet.Config{Store: owner.store, Guest: owner.guestConnection, Egress: owner.egress, Trust: owner.trust, Sources: owner})
	}
	if owner.workloads == nil {
		owner.workloads = assembleWorkloads(owner)
	}
	owner.services.(*servicenet.Owner).BindWorkloads(owner.workloads.(*workload.Owner))
	if owner.sessions == nil {
		owner.sessions = session.New(session.Config{Store: owner.store, Services: owner.services.(*servicenet.Owner), Runtime: applianceTestRuntime{}, MaxMessageBytes: 16 << 20})
	}
	if owner.remote == nil {
		owner.remote = assembleRemote(owner, resourcev2.Contract{PIDMax: 2048})
	}
	if owner.cleanup == nil {
		owner.cleanup = cleanup.New(cleanup.Config{Store: owner.store, Guest: owner.guestConnection, Manager: owner.manager, Inventory: owner.inventory, Workloads: owner.workloads.(*workload.Owner), Services: owner.services.(*servicenet.Owner), Sessions: owner.sessions.(*session.Owner), HostShares: applianceTestShares{}, Egress: owner.egress})
	}
	owner.cleanup.(*cleanup.Owner).BindRemote(owner.remote.(*remote.Owner))
	owner.remote.(*remote.Owner).BindCleanup(owner.cleanup.(*cleanup.Owner))
	if owner.vmLifecycle != nil {
		owner.remote.(*remote.Owner).BindLifecycle(owner.vmLifecycle)
	}
	return owner
}
func assembleWorkloads(owner *Owner) *workload.Owner {
	return workload.New(workload.Config{Store: owner.store, Guest: owner.guestConnection, Manager: owner.manager, Projection: owner.services.(*servicenet.Owner).ProjectionLock(), Services: owner.services.(*servicenet.Owner), HostShares: applianceTestShares{}})
}
func assembleRemote(owner *Owner, capacity resourcev2.Contract) *remote.Owner {
	return remote.New(remote.Config{Store: owner.store, Guest: owner.guestConnection, Manager: owner.manager, Inventory: owner.inventory, Workloads: owner.workloads.(*workload.Owner), Services: owner.services.(*servicenet.Owner), Sessions: owner.sessions.(*session.Owner), Trust: owner.trust, Capacity: capacity})
}
func withTestRemoteCapacity(owner *Owner, capacity store.Environment) *Owner {
	owner.remote = assembleRemote(owner, capacity.Resources())
	owner.remote.(*remote.Owner).BindCleanup(owner.cleanup.(*cleanup.Owner))
	owner.cleanup.(*cleanup.Owner).BindRemote(owner.remote.(*remote.Owner))
	return owner
}
func closeTestOwner(owner *Owner) error {
	owner.InvalidateAppliances()
	owner.sessions.(*session.Owner).Close()
	return owner.store.Close()
}

type testApplianceServer struct {
	appliancev1.UnsafeApplianceAdminServiceServer
	*Owner
}

func newTestOperatorServer(owner *Owner) *grpc.Server {
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})), grpc.StreamInterceptor(auth.StreamInterceptor(auth.Identity{Role: auth.Operator})))
	appliancev1.RegisterApplianceAdminServiceServer(server, testApplianceServer{Owner: owner})
	return server
}
func hostLockWatchdog(t *testing.T) func() {
	t.Helper()
	timer := time.AfterFunc(5*time.Second, func() { panic(t.Name() + ": owner operation did not finish") })
	return func() { timer.Stop() }
}
