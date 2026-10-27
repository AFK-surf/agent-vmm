//go:build darwin || linux

package appliance

import (
	"context"
	"log/slog"
	"sync"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

type Manager interface {
	Snapshot() *hostv1.VM
	ReadVerifiedBundledApplianceMaterial() (vmm.BundledApplianceMaterial, bool, error)
}
type Workloads interface {
	WaitRegistrationQuiesced(context.Context, string) error
}
type Sessions interface {
	RevokeRegistration(context.Context, string) error
}
type Remote interface {
	LockAdmission() func()
	RetireRegistration(context.Context, string) error
	Capacity() store.Environment
}
type Cleanup interface {
	DiscardRegistrationEnvironments(context.Context, vmmdv1.VMMDServiceClient, string, string) error
}
type Services interface {
	ProjectionLock() *locks.RWMutex
	PublishSourcePoliciesLocked(context.Context, string) error
	LockServiceDomain(context.Context, string) (func(), error)
	DeleteServiceProjection(context.Context, string, string) error
	ProjectServiceContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, string, string, string) error
	ValidateContainerInstance(context.Context, string, string, string) error
	ServiceContainerIdentity(context.Context, string, string, string) (*vmmdv1.Container, error)
	RevalidateServiceExportLocked(context.Context, vmmdv1.VMMDServiceClient, *servicev1.ServiceExport, string) (bool, error)
}
type Owner struct {
	appliancev1.UnimplementedApplianceAdminServiceServer
	store                                 *store.Store
	guestConnection                       *guestconn.Connection
	manager                               Manager
	inventory                             *hostlifecycle.InventoryFence
	services                              Services
	workloads                             Workloads
	sessions                              Sessions
	remote                                Remote
	cleanup                               Cleanup
	vmLifecycle                           *hostlifecycle.Owner
	egress                                *egress.Subsystem
	trust                                 *hosttrust.Manager
	trustKeyProvider                      trustkeys.Provider
	systemCapacity                        store.Environment
	supportDirectory, remoteComputeSocket string
	logger                                *slog.Logger
	applianceSecrets                      SecretStore
	applianceContentStage                 locks.RWMutex
	applianceLocks                        locks.Keyed[string]
	applianceUpdates                      locks.Keyed[string]
	applianceHealthMu                     sync.Mutex
	applianceTrustRetry                   map[string]time.Time
	applianceHealth                       map[string]*appliancev1.ApplianceHealth
	pendingApplianceHealth                map[string]*appliancev1.ApplianceHealth
	privateControllerMu                   sync.Mutex
	privateControllers                    map[string]*privateControllerRoute
	applianceFrontendMu                   sync.Mutex
	applianceFrontends                    map[string]*applianceFrontendRuntime
}

func (owner *Owner) BindRuntime(services Services, sessions Sessions, workloads Workloads) {
	owner.services = services
	owner.sessions = sessions
	owner.workloads = workloads
}
func (owner *Owner) BindRemote(remote Remote)    { owner.remote = remote }
func (owner *Owner) BindCleanup(cleanup Cleanup) { owner.cleanup = cleanup }
func (owner *Owner) BindLifecycle(lifecycle *hostlifecycle.Owner) {
	owner.vmLifecycle = lifecycle
}

type Config struct {
	Store                                 *store.Store
	Guest                                 *guestconn.Connection
	Manager                               Manager
	Inventory                             *hostlifecycle.InventoryFence
	Egress                                *egress.Subsystem
	Trust                                 *hosttrust.Manager
	TrustKeyProvider                      trustkeys.Provider
	SystemCapacity                        store.Environment
	SupportDirectory, RemoteComputeSocket string
	Secrets                               SecretStore
	Logger                                *slog.Logger
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{store: config.Store, guestConnection: config.Guest, manager: config.Manager, inventory: config.Inventory, egress: config.Egress, trust: config.Trust, trustKeyProvider: config.TrustKeyProvider, systemCapacity: config.SystemCapacity, supportDirectory: config.SupportDirectory, remoteComputeSocket: config.RemoteComputeSocket, logger: logger.With("component", "host.appliance"), applianceSecrets: config.Secrets, applianceHealth: make(map[string]*appliancev1.ApplianceHealth), pendingApplianceHealth: make(map[string]*appliancev1.ApplianceHealth), applianceTrustRetry: make(map[string]time.Time), privateControllers: make(map[string]*privateControllerRoute), applianceFrontends: make(map[string]*applianceFrontendRuntime)}
}
func (owner *Owner) InvalidateAppliances() {
	owner.StopAllFrontends()
	owner.ClearAllHealth()
	owner.StopAllPrivateControllers()
}
