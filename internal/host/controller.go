//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

const (
	defaultSessionLifetime = 8 * time.Hour
	maxMessageBytes        = 16 << 20
)

type Config struct {
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
	DisablePersonalMeshPairing bool
	DataWorkerSocket           string
	RemoteComputeSocket        string
	TrustSocket                string
	TrustCapabilityFile        string
	ServiceTunnelSocket        string
	MeshControlSocket          string
}

type Controller struct {
	hostShareMu     sync.Mutex
	hostShares      map[string]store.HostShare
	hostSharesDirty bool

	localDisposalWake           chan struct{}
	manager                     *vmm.Manager
	store                       *store.Store
	trust                       *hosttrust.Manager
	personalMeshPairingDisabled bool
	sessionDirectory            string
	supportDirectory            string
	dataWorkerSocket            string
	remoteComputeSocket         string
	trustSocket                 string
	trustCapabilityFile         string
	serviceTunnelSocket         string
	meshControlSocket           string
	instanceDirectory           string
	applianceSecrets            applianceSecretStore
	trustKeyProvider            trustkeys.Provider
	remoteCredentials           remoteconnector.CredentialStore

	mu                       sync.Mutex
	sessions                 map[string]*sessionServer
	observed                 map[string][]*hostv1.Container
	localResourceCache       map[string]localResourceSample
	workloadRuns             map[string]*workloadRun
	workloadRunOrder         []string
	workloadRunIndex         map[string]int
	workloadRunsRecovered    bool
	stoppedWorkloadInstances map[string]string
	workloadPressureAt       time.Time
	workloadMemoryPressure   bool
	workloadOOMKills         uint64
	workloadOOMEvents        uint64
	workloadPressureHigh     int
	workloadPressureLow      int
	workloadSampleOffset     int
	workloadReclaim          *workloadReclaimCandidate
	environmentLocks         sync.Map
	integrationLocks         sync.Map
	sessionActivity          sync.Map
	remoteAdmissionMu        sync.Mutex
	projectionMu             sync.Mutex
	inventoryTransition      sync.RWMutex
	egress                   *egress.Subsystem
	// registryRoundTripper replaces the public registry transport in tests.
	registryRoundTripper     http.RoundTripper
	sourcePolicyPublisher    func(context.Context, string) error
	egressCancel             context.CancelFunc
	egressAcceptWG           sync.WaitGroup
	egressSessionWG          sync.WaitGroup
	policyRevision           atomic.Uint64
	remoteCapacity           store.Environment
	remoteCapacityV2         resourcev2.Contract
	systemCapacity           store.Environment
	hostFrontendGeneration   atomic.Uint64
	hostFrontends            *hostLoopbackFrontendManager
	remoteConnectionEpochs   map[string]uint64
	routeBudgets             routeBudgetTracker
	applianceHealthMu        sync.Mutex
	applianceTrustRetry      map[string]time.Time
	applianceHealth          map[string]*appliancev1.ApplianceHealth
	pendingApplianceHealth   map[string]*appliancev1.ApplianceHealth
	serviceListenerMu        sync.Mutex
	serviceListeners         map[string]*serviceListenerRuntime
	pairingMu                sync.Mutex
	pairings                 map[string]*personalMeshPairingRuntime
	privateControllerMu      sync.Mutex
	privateControllers       map[string]*privateControllerRoute
	applianceFrontendMu      sync.Mutex
	applianceFrontends       map[string]*applianceFrontendRuntime
	domainMetricsMu          sync.Mutex
	domainMetrics            map[string]*controllerDomainMetricState
	applianceReconcileCancel context.CancelFunc
	applianceReconcileWG     sync.WaitGroup
	allocationRetirementWake chan struct{}
	externalConnectorMu      sync.Mutex
	externalConnectors       map[string]*externalConnectorRoute
	externalConnectorContext context.Context
	externalConnectorRunner  func(context.Context, remoteconnector.Config) error
	vmLifecycle              *VMLifecycleOwner
}

func (controller *Controller) lockIntegration(id string) func() {
	lock := controller.integrationLock(id)
	lock.Lock()
	return lock.Unlock
}

func (controller *Controller) integrationLock(id string) *sync.RWMutex {
	value, _ := controller.integrationLocks.LoadOrStore(id, &sync.RWMutex{})
	return value.(*sync.RWMutex)
}

func (controller *Controller) lockInventoryMutation(ctx context.Context) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if controller.inventoryTransition.TryRLock() {
			if err := ctx.Err(); err != nil {
				controller.inventoryTransition.RUnlock()
				return nil, err
			}
			return controller.inventoryTransition.RUnlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (controller *Controller) lockInventoryTransition(ctx context.Context) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if controller.inventoryTransition.TryLock() {
			if err := ctx.Err(); err != nil {
				controller.inventoryTransition.Unlock()
				return nil, err
			}
			return controller.inventoryTransition.Unlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (controller *Controller) lockRemoteAdmission(ctx context.Context) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if controller.remoteAdmissionMu.TryLock() {
			if err := ctx.Err(); err != nil {
				controller.remoteAdmissionMu.Unlock()
				return nil, err
			}
			return controller.remoteAdmissionMu.Unlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func waitForControllerLock(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func New(config Config) (*Controller, error) {
	if config.Manager == nil || config.DatabasePath == "" || config.SessionDirectory == "" {
		return nil, errors.New("host manager, database, and session directory are required")
	}
	if err := os.MkdirAll(config.SessionDirectory, 0o700); err != nil {
		return nil, err
	}
	database, err := store.Open(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	if err := database.RevokeActiveSessions(context.Background()); err != nil {
		database.Close()
		return nil, err
	}
	var trustManager *hosttrust.Manager
	if config.TrustKeyProvider != nil {
		trustManager, err = hosttrust.New(context.Background(), database, config.TrustKeyProvider)
		if err != nil {
			database.Close()
			return nil, err
		}
	}
	entries, err := os.ReadDir(config.SessionDirectory)
	if err != nil {
		database.Close()
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sock" {
			if err := os.Remove(filepath.Join(config.SessionDirectory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				database.Close()
				return nil, err
			}
		}
	}
	if err := database.PutVMConfig(context.Background(), config.InstanceID, config.BundleDirectory, config.InstanceDirectory, config.StateDiskIdentity, config.CPUCount, config.MemoryBytes, config.StateBytes); err != nil {
		database.Close()
		return nil, err
	}
	supportDirectory := config.SupportDirectory
	if supportDirectory == "" {
		supportDirectory = filepath.Join(config.InstanceDirectory, "support")
	}
	if err := os.MkdirAll(supportDirectory, 0o700); err != nil {
		database.Close()
		return nil, err
	}
	if err := os.RemoveAll(filepath.Join(supportDirectory, "appliance-frontends")); err != nil {
		database.Close()
		return nil, err
	}
	applianceSecrets, err := newApplianceSecretStore(config.InstanceDirectory)
	if err != nil {
		database.Close()
		return nil, err
	}
	controller := &Controller{
		manager: config.Manager, store: database, trust: trustManager, personalMeshPairingDisabled: config.DisablePersonalMeshPairing, sessionDirectory: config.SessionDirectory,
		supportDirectory: supportDirectory, dataWorkerSocket: config.DataWorkerSocket, remoteComputeSocket: config.RemoteComputeSocket,
		trustSocket: config.TrustSocket, trustCapabilityFile: config.TrustCapabilityFile,
		serviceTunnelSocket: config.ServiceTunnelSocket, meshControlSocket: config.MeshControlSocket, instanceDirectory: config.InstanceDirectory,
		applianceSecrets: applianceSecrets, trustKeyProvider: config.TrustKeyProvider, sessions: make(map[string]*sessionServer), observed: make(map[string][]*hostv1.Container),
		remoteCredentials: remoteconnector.NewCredentialStore(filepath.Join(supportDirectory, "remote-credentials")),
		egress:            egress.New(),
		applianceHealth:   make(map[string]*appliancev1.ApplianceHealth), pendingApplianceHealth: make(map[string]*appliancev1.ApplianceHealth),
		serviceListeners:         make(map[string]*serviceListenerRuntime),
		pairings:                 make(map[string]*personalMeshPairingRuntime),
		privateControllers:       make(map[string]*privateControllerRoute),
		applianceFrontends:       make(map[string]*applianceFrontendRuntime),
		domainMetrics:            make(map[string]*controllerDomainMetricState),
		externalConnectors:       make(map[string]*externalConnectorRoute),
		localDisposalWake:        make(chan struct{}, 1),
		workloadRuns:             make(map[string]*workloadRun),
		workloadRunIndex:         make(map[string]int),
		allocationRetirementWake: make(chan struct{}, 1),
		remoteCapacity:           environmentFromContract(remoteWorkloadCapacity(config.CPUCount, config.MemoryBytes, config.StateBytes)),
		remoteCapacityV2:         remoteWorkloadCapacity(config.CPUCount, config.MemoryBytes, config.StateBytes),
		systemCapacity:           systemResourceCapacity(config.CPUCount, config.MemoryBytes),
	}
	controller.vmLifecycle = newVMLifecycleOwner(controller, config.Manager)
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	controller.egress.HostTCPInfrastructure = controller.hostTCPInfrastructure
	controller.remoteConnectionEpochs = make(map[string]uint64)
	config.Manager.SetInvalidationHandler(func() {
		controller.prepareVMTransition(context.Background())
	})
	config.Manager.SetAdmissionFence(controller.vmLifecycle.closeAdmission)
	config.Manager.SetUnexpectedStopHandler(func(previousBootID string) {
		controller.vmLifecycle.RecoverAsync(previousBootID)
	})
	// Appliance operation receipts outlive the host process. Recover them
	// before exposing the controller so reconciliation cannot treat an
	// interrupted lifecycle call as an active owner forever.
	if err := controller.recoverInterruptedApplianceAdminOperations(context.Background()); err != nil {
		database.Close()
		return nil, fmt.Errorf("recover interrupted appliance operations: %w", err)
	}
	if err := controller.reconcileBundledApplianceArtifact(context.Background()); err != nil {
		log.Printf("bundled appliance artifact unavailable: %v", err)
	}
	reconcileContext, reconcileCancel := context.WithCancel(context.Background())
	controller.applianceReconcileCancel = reconcileCancel
	controller.applianceReconcileWG.Add(4)
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.workloadObservationLoop(reconcileContext)
	}()
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.applianceReconcileLoop(reconcileContext)
	}()
	go func() {
		defer controller.applianceReconcileWG.Done()
		controller.allocationRetirementLoop(reconcileContext)
	}()
	go func() { defer controller.applianceReconcileWG.Done(); controller.localDisposalLoop(reconcileContext) }()
	return controller, nil
}

const unexpectedVMRecoveryTimeout = 10 * time.Minute

func remoteWorkloadCapacity(cpuCount uint, memoryBytes uint64, stateBytes int64) resourcev2.Contract {
	totalCPU := uint64(cpuCount) * 1000
	pids := uint64(2048)
	writable := uint64(0)
	reserved := guestdisk.ControlPartitionBytes + guestdisk.DefaultEmergencyBytes + guestdisk.DefaultMetadataBytes
	if stateBytes > 0 && uint64(stateBytes) > reserved {
		writable = uint64(stateBytes) - reserved
	}

	return resourcev2.Contract{CPUMaxMillis: totalCPU, MemoryMaxBytes: memoryBytes, PIDMax: pids, WritableQuotaBytes: writable}
}

func environmentFromContract(capacity resourcev2.Contract) store.Environment {
	return store.Environment{CPUMillis: uint32(capacity.CPUMaxMillis), MemoryBytes: capacity.MemoryMaxBytes,
		PIDs: uint32(capacity.PIDMax), DiskBytes: capacity.WritableQuotaBytes}
}

func systemResourceCapacity(cpuCount uint, memoryBytes uint64) store.Environment {
	totalCPU := uint64(cpuCount) * 1000
	return store.Environment{CPUMillis: uint32(totalCPU), MemoryBytes: memoryBytes,
		PIDs: 2048, DiskBytes: guestdisk.ControlSystemBytes}
}

func (controller *Controller) invalidateRuntimeCapabilities(ctx context.Context) {
	controller.prepareVMTransition(ctx)
}

func (controller *Controller) prepareVMTransition(ctx context.Context) {
	controller.mu.Lock()
	controller.workloadRunsRecovered = false
	controller.mu.Unlock()
	if controller.vmLifecycle != nil {
		controller.vmLifecycle.closeAdmission()
	}
	controller.stopAllExternalConnectors()
	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
	controller.invalidateRuntimeCapabilitiesLocked(ctx)
}

func (controller *Controller) commitRunningVM(ctx context.Context, excludedInstallationID string, generation uint64, bootID string) error {
	if err := controller.startEgress(); err != nil {
		return err
	}
	return controller.finishRunningVM(ctx, generation, bootID)
}

func (controller *Controller) finishRunningVM(ctx context.Context, generation uint64, bootID string) error {
	if err := controller.recoverWorkloadRuns(ctx); err != nil {
		return fmt.Errorf("recover workload namespaces, inventory, and managed services: %w", err)
	}
	if err := controller.reconcilePolicies(ctx); err != nil {
		return fmt.Errorf("restore VM policy and service routes: %w", err)
	}
	if err := controller.ReconcileExternalConnectors(ctx); err != nil {
		return fmt.Errorf("restore external connectors: %w", err)
	}
	if !controller.vmLifecycle.openAdmission(generation, bootID) {
		return errors.New("VM lifecycle is shutting down")
	}
	return nil
}

func (controller *Controller) requireRemoteAdmission() error {
	if controller.vmLifecycle != nil && controller.vmLifecycle.admitsRemoteWork() {
		return nil
	}
	return retryable("VM_ADMISSION_CLOSED", "VM runtime admission is temporarily unavailable", time.Second)
}

func (controller *Controller) invalidateRuntimeCapabilitiesLocked(ctx context.Context) {
	controller.hostFrontends.closeAll(hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	controller.stopAllApplianceFrontends()
	controller.clearAllApplianceHealth()
	controller.stopAllPrivateControllerRoutes()
	controller.mu.Lock()
	sessionIDs := make([]string, 0, len(controller.sessions))
	for id := range controller.sessions {
		sessionIDs = append(sessionIDs, id)
	}
	controller.mu.Unlock()
	for _, id := range sessionIDs {
		_ = controller.revokeSession(ctx, id)
	}
	for cursor := ""; ; {
		registrations, err := controller.store.ListRemoteRegistrationPage(ctx, cursor, 32)
		if err != nil || len(registrations) == 0 {
			return
		}
		for _, registration := range registrations {
			cursor = registration.ID
			if registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
				_, _ = controller.store.SetRemoteConnection(ctx, registration.ID, registration.ConnectionInstance, int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
			}
		}
	}
}

func (controller *Controller) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = controller.Shutdown(ctx)
	if controller.applianceReconcileCancel != nil {
		controller.applianceReconcileCancel()
		controller.applianceReconcileWG.Wait()
	}
	controller.hostFrontends.closeAll(hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	controller.stopAllApplianceFrontends()
	controller.stopAllPrivateControllerRoutes()
	controller.stopAllExternalConnectors()
	controller.stopEgress()
	controller.closeAllServiceListeners()
	controller.mu.Lock()
	sessions := controller.sessions
	controller.sessions = make(map[string]*sessionServer)
	controller.mu.Unlock()
	for _, session := range sessions {
		session.server.Stop()
		session.listener.Close()
		_ = os.Remove(session.path)
	}
	_ = controller.store.RevokeActiveSessions(context.Background())
	return controller.store.Close()
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
	appliancev1.RegisterApplianceAdminServiceServer(server, &applianceAdminServer{controller: controller})
	return server
}
