//go:build darwin || linux

package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (owner *Owner) Create(ctx context.Context, environmentID, integrationID string, integrationRevision uint64, ownerKind int32, remoteIdentity auth.Identity) (store.Session, store.Environment, error) {
	environment, err := owner.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	if environment.OwnerKind != ownerKind {
		return store.Session{}, store.Environment{}, errors.New("environment owner does not match session endpoint")
	}
	if err := owner.services.ReconcileEnvironment(ctx, environment); err != nil {
		return store.Session{}, store.Environment{}, err
	}
	sessionID, err := operation.NewID()
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	socketPath := filepath.Join(owner.directory, sessionID+".sock")
	listener, err := privatefs.ListenUnix(socketPath)
	if err != nil {
		return store.Session{}, store.Environment{}, err
	}
	identity := auth.Identity{Role: auth.Agent, SessionID: sessionID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID}
	identity.RegistrationID = remoteIdentity.RegistrationID
	identity.AllocationID = remoteIdentity.AllocationID
	identity.AllocationGeneration = remoteIdentity.AllocationGeneration
	identity.ExecutionOwnerID = remoteIdentity.ExecutionOwnerID
	tracked := &server{listener: listener, path: socketPath, registrationID: remoteIdentity.RegistrationID,
		allocationID: remoteIdentity.AllocationID, allocationGeneration: remoteIdentity.AllocationGeneration, stopDone: make(chan struct{}), integrationID: integrationID, integrationRevision: integrationRevision}
	statsHandler := &sessionStats{owner: owner, sessionID: sessionID, connections: &tracked.connections}
	unary := append([]grpc.UnaryServerInterceptor{auth.UnaryInterceptor(identity), owner.unary(sessionID)}, owner.unaryInterceptors...)
	stream := append([]grpc.StreamServerInterceptor{auth.StreamInterceptor(identity), owner.stream(sessionID)}, owner.streamInterceptors...)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(owner.maxMessageBytes), grpc.MaxSendMsgSize(owner.maxMessageBytes), grpc.MaxConcurrentStreams(64), grpc.StatsHandler(statsHandler), grpc.WaitForHandlers(true), grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
	tracked.server = server
	buildLifetime, cancelBuilds := context.WithCancel(context.Background())
	tracked.lifetime, tracked.cancel = buildLifetime, cancelBuilds
	hostv1.RegisterAgentRuntimeServiceServer(server, owner.runtime.RuntimeService(buildLifetime))
	origin := int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION)
	if ownerKind == store.OwnerRemoteRegistration {
		origin = int32(hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_REMOTE_ALLOCATION)
	}
	now := time.Now()
	session := store.Session{ID: sessionID, IntegrationID: integrationID, EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID,
		SocketPath: socketPath, Origin: origin, CreatedAt: now, ExpiresAt: now.Add(defaultSessionLifetime)}
	var saveErr error
	if integrationID == "" {
		saveErr = owner.store.CreateSession(ctx, session)
	} else {
		saveErr = owner.store.CreateAuthorizedSession(ctx, session, integrationRevision)
	}
	if saveErr != nil {
		listener.Close()
		_ = os.Remove(socketPath)
		return store.Session{}, store.Environment{}, saveErr
	}
	owner.mu.Lock()
	tracked.expiresAt, tracked.environmentID = session.ExpiresAt, environment.ID
	if integrationID != "" {
		owner.integrationRevisions[integrationID] = integrationRevision
	}
	owner.servers[sessionID] = tracked
	tracked.timer = time.AfterFunc(time.Until(session.ExpiresAt), func() { _ = owner.Revoke(context.Background(), sessionID) })
	owner.mu.Unlock()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			_ = owner.store.Audit(context.Background(), sessionID, environment.ID, "session.serve", "", "", int32(codes.Unavailable), "SESSION_LISTENER_FAILED")
		}
	}()
	return session, environment, nil
}

func (owner *Owner) Revoke(ctx context.Context, id string) error {
	owner.mu.Lock()
	session := owner.servers[id]
	if session != nil {
		session.revoked = true
	}
	owner.mu.Unlock()
	if session != nil {
		session.cancel()
	}
	persistErr := owner.store.RevokeSession(ctx, id)
	return errors.Join(persistErr, owner.stopSession(ctx, id))
}

func (owner *Owner) closeSession(id, reason string) {
	if err := owner.store.EndSession(context.Background(), id, reason, true); err != nil {
		owner.logger.Warn("end session", "session_id", id, "error", err)
	}
	if reason == "CLIENT_DISCONNECTED" {
		_ = owner.drainSession(context.Background(), id)
	} else {
		_ = owner.stopSession(context.Background(), id)
	}
}

type sessionResponseEnd struct {
	requested bool
}
type sessionResponseEndKey struct{}

func EndAfterResponse(ctx context.Context) {
	if end, ok := ctx.Value(sessionResponseEndKey{}).(*sessionResponseEnd); ok {
		end.requested = true
	}
}
