//go:build darwin || linux

package servicenet

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
)

type Owner struct {
	store                       *store.Store
	manager                     Manager
	guestConnection             *guestconn.Connection
	egress                      *egress.Subsystem
	trust                       *hosttrust.Manager
	workloads                   Workloads
	serviceDirectory            string
	dataWorkerSocket            string
	sources                     EgressSourceProvider
	personalMeshPairingDisabled bool
	pairingMu                   sync.Mutex
	pairings                    map[[16]byte]*personalMeshPairingRuntime
	serviceLocks                locks.Keyed[string]
	logger                      *slog.Logger
	mu                          sync.Mutex
	projectionMu                locks.RWMutex
	egressCancel                context.CancelFunc
	egressAcceptWG              sync.WaitGroup
	egressSessionWG             sync.WaitGroup
	policyRevision              atomic.Uint64
	routeBudgets                RouteBudgetTracker
	serviceListenerMu           sync.Mutex
	serviceListeners            map[string]*serviceListenerRuntime
}

type Manager interface {
	Snapshot() *hostv1.VM
	ListenEgress() (net.Listener, string, error)
	CloseEgress()
}
type Workloads interface {
	Instance(string, string) (workload.Instance, bool)
	LockEnvironment(context.Context, string) (func(), error)
	RememberContainers(string, []*hostv1.Container)
	RememberLocalResourceSample(store.Environment, *hostv1.AgentEnvironment)
	UnknownContainers(string) []*hostv1.Container
	StartWorkloadContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, *vmmdv1.StartContainerRequest, ...string) (*vmmdv1.StartContainerResponse, error)
	StopWorkloadContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error)
}
type EgressSourceProvider interface {
	EgressSources(context.Context, vmmdv1.VMMDServiceClient, string) ([]egress.Source, error)
	HostTCPInfrastructureEndpoints() []netip.AddrPort
}
type Config struct {
	Store                              *store.Store
	Manager                            Manager
	Guest                              *guestconn.Connection
	Egress                             *egress.Subsystem
	Trust                              *hosttrust.Manager
	ServiceDirectory, DataWorkerSocket string
	Sources                            EgressSourceProvider
	DisablePersonalMeshPairing         bool
	Logger                             *slog.Logger
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{store: config.Store, manager: config.Manager, guestConnection: config.Guest, egress: config.Egress, trust: config.Trust, serviceDirectory: config.ServiceDirectory, dataWorkerSocket: config.DataWorkerSocket, sources: config.Sources, personalMeshPairingDisabled: config.DisablePersonalMeshPairing, pairings: make(map[[16]byte]*personalMeshPairingRuntime), logger: logger.With("component", "host.servicenet"), serviceListeners: make(map[string]*serviceListenerRuntime)}
}
func (owner *Owner) BindWorkloads(workloads Workloads) { owner.workloads = workloads }
func (owner *Owner) ProjectionLock() *locks.RWMutex    { return &owner.projectionMu }
func (owner *Owner) LockServiceDomain(ctx context.Context, registrationID string) (func(), error) {
	return owner.serviceLocks.Lock(ctx, "service-domain:"+registrationID)
}
func (owner *Owner) AcquireRouteBudget(capability *trustv1.RouteCapability, now time.Time) (*RouteBudgetLease, error) {
	return owner.routeBudgets.Acquire(capability, now)
}
