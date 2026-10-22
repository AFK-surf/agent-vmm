//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (controller *Controller) createSession(ctx context.Context, environmentID, integrationID string, integrationRevision uint64, ownerKind int32, remoteIdentity auth.Identity) (store.Session, store.Environment, error) {
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
	identity.AllocationGeneration = remoteIdentity.AllocationGeneration
	identity.ExecutionOwnerID = remoteIdentity.ExecutionOwnerID
	tracked := &sessionServer{listener: serving, path: socketPath, registrationID: remoteIdentity.RegistrationID,
		allocationID: remoteIdentity.AllocationID, allocationGeneration: remoteIdentity.AllocationGeneration, stopDone: make(chan struct{}), integrationID: integrationID, integrationRevision: integrationRevision}
	statsHandler := &sessionStats{controller: controller, sessionID: sessionID, connections: &tracked.connections}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(64),
		grpc.StatsHandler(statsHandler),
		grpc.WaitForHandlers(true),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), sessionActivityUnary(controller, sessionID), remoteRuntimeUnary(ownerKind), auditUnary(controller), deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), sessionActivityStream(controller, sessionID), remoteRuntimeStream(ownerKind), auditStream(controller), deadlineStream, errorDetailsStream),
	)
	tracked.server = server
	buildLifetime, cancelBuilds := context.WithCancel(context.Background())
	tracked.cancelBuilds = cancelBuilds
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller, buildLifetime: buildLifetime})
	origin := int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION)
	if ownerKind == store.OwnerRemoteRegistration {
		origin = int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_REMOTE_ALLOCATION)
	}
	now := time.Now()
	session := store.Session{ID: sessionID, IntegrationID: integrationID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID,
		SocketPath: socketPath, Origin: origin, CreatedAt: now, ExpiresAt: now.Add(defaultSessionLifetime)}
	var saveErr error
	if integrationID == "" {
		saveErr = controller.store.CreateSession(ctx, session)
	} else {
		saveErr = controller.store.CreateAuthorizedSession(ctx, session, integrationRevision)
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

func (controller *Controller) revokeSession(ctx context.Context, id string) error {
	if err := controller.store.RevokeSession(ctx, id); err != nil {
		return err
	}
	return controller.stopSession(ctx, id)
}

func (controller *Controller) closeSession(id, reason string) {
	if err := controller.store.EndSession(context.Background(), id, reason, true); err != nil {
		return
	}
	if reason == "CLIENT_DISCONNECTED" {
		_ = controller.stopSessionHandlers(context.Background(), id)
	} else {
		_ = controller.stopSession(context.Background(), id)
	}
}

func (controller *Controller) revokeSessionAfterResponse(id string, environmentDeleted bool) {
	time.Sleep(10 * time.Millisecond)
	if !environmentDeleted {
		_ = controller.store.RevokeSession(context.Background(), id)
	}
	_ = controller.stopSession(context.Background(), id)
}
