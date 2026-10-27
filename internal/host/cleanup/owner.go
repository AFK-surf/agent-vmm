//go:build darwin || linux

package cleanup

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

type Manager interface{ Snapshot() *hostv1.VM }
type Workloads interface {
	LockEnvironment(context.Context, string) (func(), error)
	ForgetContainers(string)
	StopWorkloadContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error)
}
type Services interface {
	ProjectionLock() *locks.RWMutex
	ReconcileEnvironment(context.Context, store.Environment) error
	PublishSourcePoliciesLocked(context.Context, string) error
}
type Sessions interface {
	RevokeEnvironmentSessions(context.Context, string) error
	CloseDisposed(string)
}
type HostShares interface {
	SettleEnvironmentHostShares(context.Context, string) error
}
type Remote interface {
	LockAdmission() func()
	StopExternalConnector(string)
	DeleteCredential(context.Context, string) error
	ReadAllocation(context.Context, string, string, string) (store.RemoteRegistration, store.Environment, error)
	RetainEnvironmentLocked(context.Context, store.Environment, bool) (store.Environment, error)
	Allocation(store.Environment) *hostv1.RemoteAllocation
}

type Owner struct {
	store             *store.Store
	guestConnection   *guestconn.Connection
	manager           Manager
	inventory         *hostlifecycle.InventoryFence
	workloads         Workloads
	services          Services
	sessions          Sessions
	hostShares        HostShares
	remote            Remote
	egress            *egress.Subsystem
	localDisposalWake chan struct{}
}

type Config struct {
	Store      *store.Store
	Guest      *guestconn.Connection
	Manager    Manager
	Inventory  *hostlifecycle.InventoryFence
	Workloads  Workloads
	Services   Services
	Sessions   Sessions
	HostShares HostShares
	Egress     *egress.Subsystem
}

func New(config Config) *Owner {
	return &Owner{store: config.Store, guestConnection: config.Guest, manager: config.Manager, inventory: config.Inventory, workloads: config.Workloads, services: config.Services, sessions: config.Sessions, hostShares: config.HostShares, egress: config.Egress, localDisposalWake: make(chan struct{}, 1)}
}
func (owner *Owner) BindRemote(remote Remote) { owner.remote = remote }
