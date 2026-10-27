//go:build darwin || linux

package remote

import (
	"context"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"log/slog"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Workloads interface {
	LockEnvironment(context.Context, string) (func(), error)
	RemoteWorkloadUsage(context.Context, string, *timestamppb.Timestamp) *hostv1.Usage
	ReclaimCandidate(string) *connectorv1.WorkloadReclaimCandidate
	StopWorkloadContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error)
	ForgetContainers(string)
}

type Services interface {
	ReconcileEnvironment(context.Context, store.Environment) error
	ProjectionLock() *locks.RWMutex
	PublishSourcePoliciesLocked(context.Context, string) error
	HostEgressHealth(*hostv1.VM, *timestamppb.Timestamp) *hostv1.ComponentHealth
}

type Sessions interface {
	Create(context.Context, string, string, uint64, int32, auth.Identity) (store.Session, store.Environment, error)
	Revoke(context.Context, string) error
	AllocationSessionIDs(string, string, uint64) []string
	MatchesAllocation(string, string, string, uint64) bool
	ActiveSessions(string) uint32
}

type Cleanup interface {
	RequireUndisposedRegistration(context.Context, string, string) error
	WakeLocalDisposal()
}

type Lifecycle interface{ AdmitsRemoteWork() bool }
type Manager interface{ Snapshot() *hostv1.VM }

type Config struct {
	Store               *store.Store
	Guest               *guestconn.Connection
	Manager             Manager
	Inventory           *lifecycle.InventoryFence
	Workloads           Workloads
	Services            Services
	Sessions            Sessions
	Trust               *trust.Manager
	Capacity            resourcev2.Contract
	Credentials         remoteconnector.CredentialStore
	RemoteComputeSocket string
	TrustSocket         string
	DataWorkerSocket    string
	ServiceTunnelSocket string
	MeshControlSocket   string
	Logger              *slog.Logger
}

type Owner struct {
	store                    *store.Store
	guestConnection          *guestconn.Connection
	manager                  Manager
	inventory                *lifecycle.InventoryFence
	workloads                Workloads
	services                 Services
	sessions                 Sessions
	trust                    *trust.Manager
	lifecycle                Lifecycle
	cleanup                  Cleanup
	capacity                 resourcev2.Contract
	remoteCredentials        remoteconnector.CredentialStore
	remoteComputeSocket      string
	trustSocket              string
	dataWorkerSocket         string
	serviceTunnelSocket      string
	meshControlSocket        string
	logger                   *slog.Logger
	remoteAdmissionMu        sync.Mutex
	remoteEnrollmentLocks    locks.Keyed[string]
	remoteConnectionEpochMu  sync.Mutex
	remoteConnectionEpochs   map[string]uint64
	domainMetricsMu          sync.Mutex
	domainMetrics            map[string]*controllerDomainMetricState
	allocationRetirementWake chan struct{}
	externalConnectorMu      sync.Mutex
	externalConnectors       map[string]*externalConnectorRoute
	externalConnectorContext context.Context
	externalConnectorRunner  func(context.Context, remoteconnector.Config) error
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{store: config.Store, guestConnection: config.Guest, manager: config.Manager, inventory: config.Inventory, workloads: config.Workloads, services: config.Services, sessions: config.Sessions, trust: config.Trust,
		capacity: config.Capacity, remoteCredentials: config.Credentials,
		remoteComputeSocket: config.RemoteComputeSocket, trustSocket: config.TrustSocket, dataWorkerSocket: config.DataWorkerSocket, serviceTunnelSocket: config.ServiceTunnelSocket, meshControlSocket: config.MeshControlSocket,
		logger: logger.With("component", "host.remote"), remoteConnectionEpochs: make(map[string]uint64), domainMetrics: make(map[string]*controllerDomainMetricState), externalConnectors: make(map[string]*externalConnectorRoute), allocationRetirementWake: make(chan struct{}, 1), externalConnectorRunner: remoteconnector.Run}
}

func (owner *Owner) BindLifecycle(lifecycle Lifecycle) { owner.lifecycle = lifecycle }
func (owner *Owner) BindCleanup(cleanup Cleanup)       { owner.cleanup = cleanup }
func (owner *Owner) LockAdmission() func() {
	owner.remoteAdmissionMu.Lock()
	return owner.remoteAdmissionMu.Unlock
}
func (owner *Owner) Capacity() store.Environment {
	return store.Environment{PIDs: uint32(owner.capacity.PIDMax), DiskBytes: owner.capacity.WritableQuotaBytes}
}
func (owner *Owner) DeleteCredential(ctx context.Context, registrationID string) error {
	return owner.remoteCredentials.Delete(ctx, registrationID)
}
func (owner *Owner) requireRemoteAdmission() error {
	if owner.lifecycle.AdmitsRemoteWork() {
		return nil
	}
	return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, "VM runtime admission is temporarily unavailable", hostv1.WithRetry(time.Second))
}
