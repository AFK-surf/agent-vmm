//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
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
	Manager           *vmm.Manager
	DatabasePath      string
	SessionDirectory  string
	InstanceID        string
	BundleDirectory   string
	InstanceDirectory string
	StateDiskIdentity string
	CPUCount          uint
	MemoryBytes       uint64
	StateBytes        int64
	SupportDirectory  string
}

type Controller struct {
	manager          *vmm.Manager
	store            *store.Store
	sessionDirectory string
	supportDirectory string

	mu                    sync.Mutex
	sessions              map[string]*sessionServer
	observed              map[string][]*hostv1.Container
	environmentLocks      sync.Map
	projectionMu          sync.Mutex
	egress                *egress.Subsystem
	egressCancel          context.CancelFunc
	egressAcceptWG        sync.WaitGroup
	egressSessionWG       sync.WaitGroup
	policyRevision        atomic.Uint64
	remoteCapacity        store.Environment
	remoteLeases          map[string]*remoteLease
	allocationLeases      map[string]string
	remoteLeaseGeneration atomic.Uint64
}

type sessionServer struct {
	server   *grpc.Server
	listener net.Listener
	path     string
}

type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (listener *readyListener) Accept() (net.Conn, error) {
	listener.once.Do(func() { close(listener.ready) })
	return listener.Listener.Accept()
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
	return &Controller{
		manager: config.Manager, store: database, sessionDirectory: config.SessionDirectory,
		supportDirectory: supportDirectory, sessions: make(map[string]*sessionServer), observed: make(map[string][]*hostv1.Container),
		egress: egress.New(), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string),
		remoteCapacity: store.Environment{CPUMillis: uint32(config.CPUCount) * 1000, MemoryBytes: config.MemoryBytes, PIDs: 1 << 20, DiskBytes: uint64(max(config.StateBytes, 0))},
	}, nil
}

func (controller *Controller) Close() error {
	controller.stopEgress()
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
	return server
}

func (controller *Controller) createSession(ctx context.Context, environmentID string, ownerKind int32, remoteIdentity auth.Identity) (store.Session, store.Environment, error) {
	if environmentID == "" {
		return store.Session{}, store.Environment{}, errors.New("environment ID is required")
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
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(64),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), remoteRuntimeUnary(ownerKind), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), remoteRuntimeStream(ownerKind), auditStream(controller), limiter.stream, deadlineStream, errorDetailsStream),
	)
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller})
	session := store.Session{ID: sessionID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID, SocketPath: socketPath, ExpiresAt: time.Now().Add(defaultSessionLifetime)}
	if err := controller.store.CreateSession(ctx, session); err != nil {
		listener.Close()
		_ = os.Remove(socketPath)
		return store.Session{}, store.Environment{}, err
	}
	controller.mu.Lock()
	controller.sessions[sessionID] = &sessionServer{server: server, listener: serving, path: socketPath}
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

func (controller *Controller) reconcileEnvironment(ctx context.Context, environment store.Environment) error {
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.reconcileEnvironmentLocked(ctx, environment)
}

func (controller *Controller) reconcileEnvironmentLocked(ctx context.Context, environment store.Environment) error {
	started := false
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		if _, err := controller.manager.Start(ctx); err != nil {
			return err
		}
		started = true
	}
	if started {
		if err := controller.startEgress(); err != nil {
			return err
		}
		return controller.reconcilePoliciesLocked(ctx)
	}
	controller.egress.Policies.Clear()
	if err := controller.projectEnvironment(ctx, environment); err != nil {
		return err
	}
	return controller.publishSourcePolicies(ctx)
}

func (controller *Controller) projectEnvironment(ctx context.Context, environment store.Environment) error {
	connection, err := controller.manager.DialVMMD(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	requestID, err := randomID()
	if err != nil {
		return err
	}
	_, err = vmmdv1.NewVMMDServiceClient(connection).EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{
		RequestId: requestID, NamespaceId: environment.NamespaceID,
		Limits:     &vmmdv1.NamespaceLimits{CpuMillis: environment.CPUMillis, MemoryBytes: environment.MemoryBytes, Pids: environment.PIDs, DiskBytes: environment.DiskBytes},
		EgressMode: vmmdv1.EgressMode(environment.EgressMode),
	})
	return err
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
			if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: randomRequestIDFor("delete-" + grant.ID), LinkId: grant.ID}); err != nil {
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
		_, applyErr := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: randomRequestIDFor(grant.ID), Link: link})
		if applyErr != nil {
			return applyErr
		}
	}
	return controller.publishSourcePolicies(ctx)
}

func randomRequestIDFor(value string) string {
	return "reconcile-" + value
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
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true}
		result.Containers = controller.unknownContainers(environment.ID)
		return result
	}
	defer connection.Close()
	containers, containerErr := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	volumes, volumeErr := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	usage, usageErr := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
	if containerErr != nil || volumeErr != nil || usageErr != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_DEGRADED}
	} else {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}
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
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true}
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

func listenUnix(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func randomID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
