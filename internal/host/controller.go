//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/host/appliance"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/hostshare"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/remote"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

const (
	maxMessageBytes = 16 << 20
)

type Config struct {
	ServiceDirectory           string
	Manager                    *vmm.Manager
	DatabasePath               string
	SessionDirectory           string
	InstanceID                 string
	BundleDirectory            string
	InstanceDirectory          string
	StateDiskIdentity          string
	CPUCount                   uint
	MemoryBytes                uint64
	StateBytes                 int64
	SupportDirectory           string
	TrustKeyProvider           trustkeys.Provider
	Secrets                    appliance.SecretStore
	Credentials                remoteconnector.CredentialStore
	DisablePersonalMeshPairing bool
	DataWorkerSocket           string
	RemoteComputeSocket        string
	TrustSocket                string
	ServiceTunnelSocket        string
	MeshControlSocket          string
}

// Controller assembles the Host owners. A session integration lock precedes
// all other locks. Transitions release its waiters by canceling the session
// context. Lock order is lifecycle operation,
// inventory fence, environment or installation operation, projection, then
// component data. Projection may span Guest RPCs. Component data locks cover
// memory or SQLite work only. They do not wait for earlier locks or peer I/O.
type Controller struct {
	componentMu      sync.Mutex
	started          bool
	closed           bool
	closeErr         error
	serviceDirectory string
	hostShares       *hostshare.Owner
	appliances       *appliance.Owner
	remote           *remote.Owner

	cleanup                     *cleanup.Owner
	manager                     *vmm.Manager
	store                       *store.Store
	trust                       *hosttrust.Manager
	personalMeshPairingDisabled bool
	sessionDirectory            string
	supportDirectory            string
	dataWorkerSocket            string
	remoteComputeSocket         string
	trustSocket                 string
	serviceTunnelSocket         string
	meshControlSocket           string
	instanceDirectory           string
	trustKeyProvider            trustkeys.Provider
	credentials                 remoteconnector.CredentialStore
	workloads                   *workload.Owner
	services                    *servicenet.Owner
	sessions                    *session.Owner
	guestConnection             guestconn.Connection
	inventory                   hostlifecycle.InventoryFence
	egress                      *egress.Subsystem
	// registryRoundTripper replaces the public registry transport in tests.
	registryRoundTripper     http.RoundTripper
	systemCapacity           store.Environment
	applianceReconcileCancel context.CancelFunc
	applianceReconcileWG     sync.WaitGroup
	vmLifecycle              *hostlifecycle.Owner
}

func New(config Config) (result *Controller, resultErr error) {
	if config.Manager == nil || config.DatabasePath == "" || config.SessionDirectory == "" || config.TrustKeyProvider == nil || config.Secrets == nil || config.Credentials == nil {
		return nil, errors.New("host manager, database, session directory, trust key provider, and secret stores are required")
	}
	if err := os.MkdirAll(config.SessionDirectory, 0o700); err != nil {
		return nil, err
	}
	database, err := store.Open(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			database.Close()
		}
	}()
	trustManager, err := hosttrust.New(context.Background(), database, config.TrustKeyProvider)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(config.SessionDirectory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sock" {
			if err := os.Remove(filepath.Join(config.SessionDirectory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
		return nil, err
	}
	if err := database.PutVMConfig(context.Background(), config.InstanceID, config.BundleDirectory, config.InstanceDirectory, config.StateDiskIdentity, config.CPUCount, config.MemoryBytes, config.StateBytes); err != nil {
		return nil, err
	}
	supportDirectory := config.SupportDirectory
	if supportDirectory == "" {
		supportDirectory = filepath.Join(config.InstanceDirectory, "support")
	}
	if err := os.MkdirAll(supportDirectory, 0o700); err != nil {
		return nil, err
	}
	controller := &Controller{
		manager: config.Manager, store: database, trust: trustManager, personalMeshPairingDisabled: config.DisablePersonalMeshPairing, sessionDirectory: config.SessionDirectory,
		serviceDirectory: config.ServiceDirectory, supportDirectory: supportDirectory, dataWorkerSocket: config.DataWorkerSocket, remoteComputeSocket: config.RemoteComputeSocket,
		trustSocket:         config.TrustSocket,
		serviceTunnelSocket: config.ServiceTunnelSocket, meshControlSocket: config.MeshControlSocket, instanceDirectory: config.InstanceDirectory,
		trustKeyProvider: config.TrustKeyProvider, credentials: config.Credentials,
		egress:         egress.New(),
		systemCapacity: systemResourceCapacity(),
	}
	controller.appliances = assembleAppliances(controller, config.Secrets)
	controller.services = assembleServices(controller)
	controller.hostShares = hostshare.New(hostshare.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: controller.manager, Inventory: &controller.inventory})
	controller.workloads = assembleWorkloads(controller)
	controller.services.BindWorkloads(controller.workloads)
	controller.hostShares.BindWorkloads(controller.workloads)
	controller.sessions = assembleSessions(controller)
	controller.appliances.BindRuntime(controller.services, controller.sessions, controller.workloads)
	controller.remote = assembleRemote(controller, remoteWorkloadCapacity(config.StateBytes))
	controller.cleanup = assembleCleanup(controller)
	controller.appliances.BindCleanup(controller.cleanup)
	controller.appliances.BindRemote(controller.remote)
	controller.remote.BindCleanup(controller.cleanup)
	controller.vmLifecycle = assembleVMLifecycle(controller, config.Manager)
	controller.appliances.BindLifecycle(controller.vmLifecycle)
	controller.remote.BindLifecycle(controller.vmLifecycle)
	controller.egress.HostTCPInfrastructure = controller.services.HostTCPInfrastructure
	config.Manager.SetInvalidationHandler(func() {
		controller.vmLifecycle.CloseAdmission()
		controller.guestConnection.Disconnect()
		controller.inventory.CancelReaders()
	})
	config.Manager.SetAdmissionFence(controller.vmLifecycle.CloseAdmission)
	config.Manager.SetUnexpectedStopHandler(func(previousBootID string) {
		controller.vmLifecycle.RecoverAsync(previousBootID)
	})
	// Appliance operation receipts outlive the host process. Recover them
	// before exposing the controller so reconciliation cannot treat an
	// interrupted lifecycle call as an active owner forever.
	if err := controller.appliances.RecoverInterruptedApplianceAdminOperations(context.Background()); err != nil {
		return nil, fmt.Errorf("recover interrupted appliance operations: %w", err)
	}
	if err := controller.appliances.ReconcileBundledArtifact(context.Background()); err != nil {
		slog.Warn("bundled appliance artifact unavailable", "error", err)
	}
	return controller, nil
}

func (controller *Controller) Start(ctx context.Context) error {
	controller.componentMu.Lock()
	defer controller.componentMu.Unlock()
	if controller.closed {
		return errors.New("host controller is closed")
	}
	if controller.started {
		return nil
	}
	controller.started = true
	reconcileContext, reconcileCancel := context.WithCancel(ctx)
	controller.applianceReconcileCancel = reconcileCancel
	controller.applianceReconcileWG.Add(4)
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.workloads.WorkloadObservationLoop(reconcileContext)
	}()
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.appliances.ReconcileLoop(reconcileContext)
	}()
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.remote.AllocationRetirementLoop(reconcileContext)
	}()
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.cleanup.LocalDisposalLoop(reconcileContext)
	}()
	return nil
}

func remoteWorkloadCapacity(stateBytes int64) resourcev2.Contract {
	pids := uint64(2048)
	writable := uint64(0)
	reserved := guestdisk.ControlPartitionBytes + guestdisk.DefaultEmergencyBytes + guestdisk.DefaultMetadataBytes
	if stateBytes > 0 && uint64(stateBytes) > reserved {
		writable = uint64(stateBytes) - reserved
	}

	return resourcev2.Contract{PIDMax: pids, WritableQuotaBytes: writable}
}

func systemResourceCapacity() store.Environment {
	return store.Environment{
		PIDs: 2048, DiskBytes: guestdisk.ControlSystemBytes}
}

func (controller *Controller) Close() error {
	controller.componentMu.Lock()
	if controller.closed {
		err := controller.closeErr
		controller.componentMu.Unlock()
		return err
	}
	controller.closed = true
	if !controller.started {
		controller.closeErr = controller.store.Close()
		controller.componentMu.Unlock()
		return controller.closeErr
	}
	if controller.applianceReconcileCancel != nil {
		controller.applianceReconcileCancel()
	}
	controller.componentMu.Unlock()
	controller.applianceReconcileWG.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = controller.Shutdown(ctx)
	controller.appliances.StopAllFrontends()
	controller.appliances.StopAllPrivateControllers()
	controller.remote.StopAllExternalConnectors()
	controller.services.StopEgress()
	controller.services.CloseAllServiceListeners()
	controller.sessions.Close()
	controller.guestConnection.Disconnect()
	err := controller.store.Close()
	controller.componentMu.Lock()
	controller.closeErr = err
	controller.componentMu.Unlock()
	return err
}

func (controller *Controller) Shutdown(ctx context.Context) error {
	return controller.vmLifecycle.Shutdown(ctx)
}

func (controller *Controller) NewOperatorServer() *grpc.Server {
	identity := auth.Identity{Role: auth.Operator}
	limiter := newRPCRateLimiter(200, 400)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(128),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), auditStream(controller), limiter.stream, deadlineStream, errorDetailsStream),
	)
	hostv1.RegisterOperatorServiceServer(server, &operatorServer{controller: controller})
	appliancev1.RegisterApplianceAdminServiceServer(server, controller.appliances)
	return server
}
