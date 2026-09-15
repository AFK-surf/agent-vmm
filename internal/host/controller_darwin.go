//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	workloadRuns             map[string]*workloadRun
	workloadRunsRecovered    bool
	workloadPressureAt       time.Time
	workloadMemoryPressure   bool
	workloadOOMKills         uint64
	environmentLocks         sync.Map
	integrationLocks         sync.Map
	sessionActivity          sync.Map
	remoteAdmissionMu        sync.Mutex
	projectionMu             sync.Mutex
	inventoryTransition      sync.RWMutex
	egress                   *egress.Subsystem
	sourcePolicyPublisher    func(context.Context, string) error
	egressCancel             context.CancelFunc
	egressAcceptWG           sync.WaitGroup
	egressSessionWG          sync.WaitGroup
	policyRevision           atomic.Uint64
	remoteCapacity           store.Environment
	remoteCapacityV2         resourcev2.Contract
	systemCapacity           store.Environment
	remoteLeases             map[string]*remoteLease
	allocationLeases         map[string]string
	remoteLeaseGeneration    atomic.Uint64
	hostFrontendGeneration   atomic.Uint64
	hostFrontends            *hostLoopbackFrontendManager
	remoteConnectionEpochs   map[string]uint64
	routeBudgets             routeBudgetTracker
	applianceHealthMu        sync.Mutex
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
	value, _ := controller.integrationLocks.LoadOrStore(id, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
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
		egress:            egress.New(), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string),
		applianceHealth: make(map[string]*appliancev1.ApplianceHealth), pendingApplianceHealth: make(map[string]*appliancev1.ApplianceHealth),
		serviceListeners:         make(map[string]*serviceListenerRuntime),
		pairings:                 make(map[string]*personalMeshPairingRuntime),
		privateControllers:       make(map[string]*privateControllerRoute),
		applianceFrontends:       make(map[string]*applianceFrontendRuntime),
		domainMetrics:            make(map[string]*controllerDomainMetricState),
		externalConnectors:       make(map[string]*externalConnectorRoute),
		allocationRetirementWake: make(chan struct{}, 1),
		remoteCapacity:           environmentFromContract(remoteWorkloadCapacity(config.CPUCount, config.MemoryBytes, config.StateBytes)),
		remoteCapacityV2:         remoteWorkloadCapacity(config.CPUCount, config.MemoryBytes, config.StateBytes),
		systemCapacity:           systemResourceCapacity(config.CPUCount, config.MemoryBytes),
	}
	controller.vmLifecycle = newVMLifecycleOwner(controller, config.Manager)
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
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
	if err := controller.recoverPendingApplianceRemovals(context.Background()); err != nil {
		database.Close()
		return nil, fmt.Errorf("recover pending appliance removals: %w", err)
	}
	if err := controller.reconcileBundledApplianceArtifact(context.Background()); err != nil {
		database.Close()
		return nil, fmt.Errorf("reconcile bundled appliance artifact: %w", err)
	}
	reconcileContext, reconcileCancel := context.WithCancel(context.Background())
	controller.applianceReconcileCancel = reconcileCancel
	controller.applianceReconcileWG.Add(3)
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
	return controller, nil
}

const unexpectedVMRecoveryTimeout = 10 * time.Minute

func remoteWorkloadCapacity(cpuCount uint, memoryBytes uint64, stateBytes int64) resourcev2.Contract {
	totalCPU := uint64(cpuCount) * 1000
	maxCPU := max(uint64(500), totalCPU-min(totalCPU, uint64(1000)))
	pids := uint64(2048)
	writable := uint64(8 << 30)
	if stateBytes > 0 && uint64(stateBytes)/4 < writable {
		writable = uint64(stateBytes) / 4
	}
	return resourcev2.Contract{CPUMaxMillis: maxCPU, MemoryMaxBytes: memoryBytes / 4 * 3, PIDMax: pids, WritableQuotaBytes: writable}
}

func environmentFromContract(capacity resourcev2.Contract) store.Environment {
	return store.Environment{CPUMillis: uint32(capacity.CPUMaxMillis), MemoryBytes: capacity.MemoryMaxBytes,
		PIDs: uint32(capacity.PIDMax), DiskBytes: capacity.WritableQuotaBytes}
}

func systemResourceCapacity(cpuCount uint, memoryBytes uint64) store.Environment {
	totalCPU := uint64(cpuCount) * 1000
	workload := remoteWorkloadCapacity(cpuCount, memoryBytes, 0)
	return store.Environment{CPUMillis: uint32(totalCPU - workload.CPUMaxMillis), MemoryBytes: memoryBytes - workload.MemoryMaxBytes,
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
	var err error
	if excludedInstallationID == "" {
		err = controller.reconcileAppliances(ctx)
	} else {
		err = controller.reconcileAppliancesExcept(ctx, excludedInstallationID)
	}
	if err != nil {
		return err
	}
	return controller.finishRunningVM(ctx, generation, bootID)
}

func (controller *Controller) finishRunningVM(ctx context.Context, generation uint64, bootID string) error {
	if err := controller.reconcilePolicies(ctx); err != nil {
		return err
	}
	if err := controller.recoverWorkloadRuns(ctx); err != nil {
		return err
	}
	if err := controller.ReconcileExternalConnectors(ctx); err != nil {
		return err
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
	leases := make([]remoteLease, 0, len(controller.remoteLeases))
	for _, lease := range controller.remoteLeases {
		leases = append(leases, *lease)
	}
	controller.mu.Unlock()
	for _, lease := range leases {
		unlockEnvironment := controller.lockEnvironment(lease.environmentID)
		_, _ = controller.finishLeaseLocked(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED, false, false, false)
		unlockEnvironment()
	}
	controller.mu.Lock()
	sessionIDs := make([]string, 0, len(controller.sessions))
	for id := range controller.sessions {
		sessionIDs = append(sessionIDs, id)
	}
	controller.mu.Unlock()
	for _, id := range sessionIDs {
		_ = controller.revokeSession(ctx, id)
	}
	registrations, err := controller.store.ListRemoteRegistrations(ctx)
	if err != nil {
		return
	}
	for _, registration := range registrations {
		if registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
			_, _ = controller.store.SetRemoteConnection(ctx, registration.ID, registration.ConnectionInstance, int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
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

func (controller *Controller) createSession(ctx context.Context, environmentID, integrationID string, ownerKind int32, remoteIdentity auth.Identity) (store.Session, store.Environment, error) {
	if environmentID == "" {
		return store.Session{}, store.Environment{}, errors.New("environment ID is required")
	}
	if ownerKind == store.OwnerLocal {
		if _, err := controller.vmLifecycle.Start(ctx); err != nil {
			return store.Session{}, store.Environment{}, err
		}
	} else if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return store.Session{}, store.Environment{}, errors.New("VM is not ready for remote admission")
	}
	unlock := controller.lockEnvironment(environmentID)
	defer unlock()
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	if environment.OwnerKind != ownerKind {
		return store.Session{}, store.Environment{}, errors.New("environment owner does not match session endpoint")
	}
	if err := controller.reconcileEnvironment(ctx, environment); err != nil {
		return store.Session{}, store.Environment{}, err
	}
	sessionID, err := randomID()
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	socketPath := filepath.Join(controller.sessionDirectory, sessionID+".sock")
	listener, err := listenUnix(socketPath)
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	serving := &readyListener{Listener: listener, ready: make(chan struct{})}
	identity := auth.Identity{Role: auth.Agent, SessionID: sessionID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID}
	identity.RegistrationID = remoteIdentity.RegistrationID
	identity.AllocationID = remoteIdentity.AllocationID
	identity.LeaseID = remoteIdentity.LeaseID
	limiter := newRPCRateLimiter(100, 200)
	tracked := &sessionServer{listener: serving, path: socketPath}
	statsHandler := &sessionStats{controller: controller, sessionID: sessionID, connections: &tracked.connections}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(64),
		grpc.StatsHandler(statsHandler),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), sessionActivityUnary(controller, sessionID), remoteRuntimeUnary(ownerKind), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), sessionActivityStream(controller, sessionID), remoteRuntimeStream(ownerKind), auditStream(controller), limiter.stream, deadlineStream, errorDetailsStream),
	)
	tracked.server = server
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller})
	origin := int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION)
	if ownerKind == store.OwnerRemoteRegistration {
		origin = int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_REMOTE_LEASE)
	}
	now := time.Now()
	session := store.Session{ID: sessionID, IntegrationID: integrationID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID,
		SocketPath: socketPath, Origin: origin, CreatedAt: now, ExpiresAt: now.Add(defaultSessionLifetime)}
	var saveErr error
	if integrationID == "" {
		saveErr = controller.store.CreateSession(ctx, session)
	} else {
		saveErr = controller.store.CreateAuthorizedSession(ctx, session)
	}
	if saveErr != nil {
		listener.Close()
		_ = os.Remove(socketPath)
		return store.Session{}, store.Environment{}, saveErr
	}
	controller.mu.Lock()
	controller.sessions[sessionID] = tracked
	controller.mu.Unlock()
	go func() {
		if err := server.Serve(serving); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			_ = controller.store.Audit(context.Background(), sessionID, environment.ID, "session.serve", "", "", int32(codes.Unavailable), "SESSION_LISTENER_FAILED")
		}
	}()
	<-serving.ready
	time.AfterFunc(time.Until(session.ExpiresAt), func() { _ = controller.revokeSession(context.Background(), sessionID) })
	return session, environment, nil
}

func (controller *Controller) ensureLocalEnvironment(ctx context.Context, environmentID, displayName string) (store.Environment, error) {
	if environmentID == "" {
		return store.Environment{}, errors.New("environment ID is required")
	}
	if _, err := controller.vmLifecycle.Start(ctx); err != nil {
		return store.Environment{}, err
	}
	releaseInventory, err := controller.lockInventoryMutation(ctx)
	if err != nil {
		return store.Environment{}, err
	}
	defer releaseInventory()
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return store.Environment{}, errors.New("VM stopped before the environment mutation could begin")
	}
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err == nil {
		if environment.OwnerKind != store.OwnerLocal {
			return store.Environment{}, errors.New("environment is owned by a remote registration")
		}
		return environment, controller.reconcileEnvironmentLocked(ctx, environment)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.Environment{}, err
	}
	if displayName == "" {
		displayName = environmentID
	}
	namespaceID, err := randomID()
	if err != nil {
		return store.Environment{}, err
	}
	environment, err = controller.store.CreateEnvironment(ctx, store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: displayName,
		CPUMillis: 2000, MemoryBytes: 2 << 30, PIDs: 512, DiskBytes: 16 << 30,
		EgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), OwnerKind: store.OwnerLocal,
	})
	if err != nil {
		return store.Environment{}, err
	}
	return environment, controller.reconcileEnvironmentLocked(ctx, environment)
}

func (controller *Controller) lockEnvironment(environmentID string) func() {
	value, _ := controller.environmentLocks.LoadOrStore(environmentID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func (controller *Controller) lockEnvironmentContext(ctx context.Context, environmentID string) (func(), error) {
	value, _ := controller.environmentLocks.LoadOrStore(environmentID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lock.TryLock() {
			if err := ctx.Err(); err != nil {
				lock.Unlock()
				return nil, err
			}
			return lock.Unlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (controller *Controller) reconcileEnvironment(ctx context.Context, environment store.Environment) error {
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.reconcileEnvironmentLocked(ctx, environment)
}

func (controller *Controller) reconcileEnvironmentLocked(ctx context.Context, environment store.Environment) error {
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return errors.New("VM is not running")
	}
	controller.egress.Policies.Clear()
	if err := controller.projectEnvironment(ctx, environment); err != nil {
		return err
	}
	return controller.publishSourcePoliciesLocked(ctx, "")
}

func (controller *Controller) projectEnvironment(ctx context.Context, environment store.Environment) error {
	if environment.OwnerKind == store.OwnerRemoteRegistration && environment.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return nil
	}
	connection, err := controller.manager.DialVMMD(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	client := vmmdv1.NewVMMDServiceClient(connection)
	requestID, err := randomID()
	if err != nil {
		return err
	}
	_, err = client.EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{
		RequestId: requestID, NamespaceId: environment.NamespaceID,
		Limits:     &vmmdv1.NamespaceLimits{CpuMillis: environment.CPUMillis, MemoryBytes: environment.MemoryBytes, Pids: environment.PIDs, DiskBytes: environment.DiskBytes},
		EgressMode: vmmdv1.EgressMode(environment.EgressMode),
	})
	if err != nil {
		return err
	}
	if environment.OwnerKind == store.OwnerRemoteRegistration {
		return controller.store.CommitNamespaceProjection(ctx, environment)
	}
	return nil
}

func (controller *Controller) reconcilePolicies(ctx context.Context) error {
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.reconcilePoliciesLocked(ctx)
}

func (controller *Controller) reconcilePoliciesLocked(ctx context.Context) error {
	controller.egress.Policies.Clear()
	environments, err := controller.store.ListEnvironments(ctx)
	if err != nil {
		return err
	}
	for _, environment := range environments {
		if err := controller.projectEnvironment(ctx, environment); err != nil {
			return err
		}
	}
	grants, err := controller.store.AllGrants(ctx)
	if err != nil {
		return err
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	for _, grant := range grants {
		if grant.Kind == "network-link" && grant.Revoked {
			if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID("delete-" + grant.ID), LinkId: grant.ID}); err != nil {
				return err
			}
			if err := controller.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
				return err
			}
		}
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.Revoked {
			continue
		}
		link, err := guestLinkFromPayload(grant.Payload, grant.SourceNamespaceID, grant.TargetNamespaceID)
		if err != nil {
			return err
		}
		_, applyErr := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: derivedRequestID(grant.ID), Link: link})
		if applyErr != nil {
			return applyErr
		}
	}
	serviceProjections, err := controller.store.ListActiveServiceProjections(ctx, time.Now())
	if err != nil {
		return err
	}
	for _, projection := range serviceProjections {
		_, err := client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{
			RequestId: derivedRequestID(projection.Import.GetImportId()),
			Projection: &vmmdv1.ServiceProjection{Id: projection.Import.GetImportId(), SourceNamespaceId: projection.SourceNamespace,
				SourceContainerId: projection.Import.GetSourceContainerId(), SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(),
				DestinationNamespaceId: projection.DestinationNamespace, DestinationContainerId: projection.Export.GetContainerId(),
				DestinationContainerInstanceId: projection.Export.GetContainerInstanceId(), DestinationPort: projection.Export.GetPort(),
				DnsAlias: projection.Import.GetVirtualServiceName(), ExpiresUnixMillis: projection.ExpiresAt.UnixMilli()},
		})
		if err != nil {
			return err
		}
	}
	if controller.trust != nil {
		identity, identityErr := controller.trust.Identity(ctx)
		if identityErr != nil {
			return identityErr
		}
		remoteProjections, remoteErr := controller.store.ListActiveRemoteServiceProjections(ctx, identity.Identity.GetDeviceId(), time.Now())
		if remoteErr != nil {
			return remoteErr
		}
		for _, projection := range remoteProjections {
			endpoint, endpointErr := controller.trust.GetEndpointObservation(ctx, projection.Import.GetDestinationDeviceId())
			if endpointErr != nil {
				return endpointErr
			}
			virtualAddress := remoteServiceAddress(projection.Import.GetImportId())
			response, applyErr := client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{
				RequestId: derivedRequestID(projection.Import.GetImportId()),
				Projection: &vmmdv1.ServiceProjection{Id: projection.Import.GetImportId(), SourceNamespaceId: projection.SourceNamespace,
					SourceContainerId: projection.Import.GetSourceContainerId(), SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(),
					DestinationPort: projection.Import.GetDestinationPort(), DnsAlias: projection.Import.GetVirtualServiceName(),
					ExpiresUnixMillis: projection.Lease.GetExpiresAt().AsTime().UnixMilli(), Remote: true, VirtualAddress: virtualAddress.String()},
			})
			if applyErr != nil {
				return applyErr
			}
			sourceAddress, parseErr := netip.ParseAddr(response.GetSourceAddress())
			if parseErr != nil {
				return parseErr
			}
			header := &servicev1.OpenServiceForwardHeader{RouteId: projection.Route.GetRouteId(), LeaseId: projection.Lease.GetLeaseId(),
				RouteGeneration: projection.Route.GetGeneration(), SourceEnvironmentId: projection.Import.GetSourceEnvironmentId(),
				SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(), Capability: projection.Capability}
			if err := controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: projection.Import.GetImportId(), EnvironmentID: projection.Import.GetSourceEnvironmentId(),
				SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(virtualAddress, uint16(projection.Import.GetDestinationPort())), ExpiresAt: projection.Lease.GetExpiresAt().AsTime(),
				Dial: func(routeContext context.Context) (net.Conn, error) {
					return controller.dialRemoteService(routeContext, header, endpoint)
				}}); err != nil {
				return err
			}
		}
	}
	return controller.publishSourcePoliciesLocked(ctx, "")
}

func derivedRequestID(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "reconcile-" + hex.EncodeToString(digest[:26])
}

func (controller *Controller) guestClient(ctx context.Context) (vmmdv1.VMMDServiceClient, *grpc.ClientConn, error) {
	connection, err := controller.manager.DialVMMD(ctx)
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "guest control service is unavailable")
	}
	return vmmdv1.NewVMMDServiceClient(connection), connection, nil
}

func (controller *Controller) environmentSnapshot(ctx context.Context, environment store.Environment) *hostv1.AgentEnvironment {
	result := environmentFromStore(environment)
	result.InventoryObservedAt = timestamppb.Now()
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "guest runtime is unavailable"}
		result.Containers = controller.unknownContainers(environment.ID)
		if controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_STOPPED {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_VM_STOPPED
			result.InventoryMessage = "Start the virtual machine to inspect containers, images, and volumes."
		} else {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_UNAVAILABLE
			result.InventoryMessage = "Runtime inventory is temporarily unavailable."
		}
		return result
	}
	defer connection.Close()
	containers, containerErr := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	volumes, volumeErr := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	usage, usageErr := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
	if containerErr != nil || volumeErr != nil || usageErr != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_DEGRADED}
		result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_STALE
		result.InventoryMessage = "Some runtime inventory could not be refreshed."
	} else {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}
		result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_AVAILABLE
	}
	if containerErr == nil {
		for _, container := range containers.GetContainers() {
			result.Containers = append(result.Containers, containerFromGuest(container))
		}
		controller.rememberContainers(environment.ID, result.Containers)
	} else {
		result.Containers = controller.unknownContainers(environment.ID)
	}
	if volumeErr == nil {
		for _, volume := range volumes.GetVolumes() {
			result.Volumes = append(result.Volumes, volumeFromGuest(volume))
		}
	}
	if usageErr == nil {
		result.Usage = usageFromGuest(usage.GetUsage())
	} else {
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "environment metrics are unavailable"}
	}
	return result
}

func (controller *Controller) rememberContainers(environmentID string, containers []*hostv1.Container) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.observed[environmentID] = cloneContainers(containers, false)
}

func (controller *Controller) unknownContainers(environmentID string) []*hostv1.Container {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return cloneContainers(controller.observed[environmentID], true)
}

func (controller *Controller) forgetContainers(environmentID string) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	delete(controller.observed, environmentID)
}

func cloneContainers(containers []*hostv1.Container, unknown bool) []*hostv1.Container {
	result := make([]*hostv1.Container, 0, len(containers))
	for _, container := range containers {
		copy := proto.Clone(container).(*hostv1.Container)
		if unknown {
			copy.State = hostv1.ContainerState_CONTAINER_STATE_UNKNOWN
		}
		result = append(result, copy)
	}
	return result
}

func (controller *Controller) revokeSession(ctx context.Context, id string) error {
	if err := controller.store.RevokeSession(ctx, id); err != nil {
		return err
	}
	controller.mu.Lock()
	session := controller.sessions[id]
	delete(controller.sessions, id)
	controller.mu.Unlock()
	if session != nil {
		session.server.Stop()
		session.listener.Close()
		_ = os.Remove(session.path)
	}
	return nil
}

func (controller *Controller) closeSession(id, reason string) {
	if err := controller.store.EndSession(context.Background(), id, reason, true); err != nil {
		return
	}
	controller.mu.Lock()
	session := controller.sessions[id]
	delete(controller.sessions, id)
	controller.mu.Unlock()
	if session != nil {
		session.server.Stop()
		session.listener.Close()
		_ = os.Remove(session.path)
	}
}

func (controller *Controller) revokeSessionAfterResponse(id string, environmentDeleted bool) {
	time.Sleep(10 * time.Millisecond)
	if !environmentDeleted {
		_ = controller.store.RevokeSession(context.Background(), id)
	}
	controller.mu.Lock()
	session := controller.sessions[id]
	delete(controller.sessions, id)
	controller.mu.Unlock()
	if session != nil {
		session.server.Stop()
		session.listener.Close()
		_ = os.Remove(session.path)
	}
}
