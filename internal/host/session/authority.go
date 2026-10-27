//go:build darwin || linux

package session

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (owner *Owner) authorizeSession(_ context.Context, id string) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	session := owner.servers[id]
	if session == nil || session.revoked || !session.expiresAt.After(time.Now()) {
		return status.Error(codes.PermissionDenied, "session authority has ended")
	}
	if session.integrationID != "" && owner.integrationRevisions[session.integrationID] != session.integrationRevision {
		return status.Error(codes.PermissionDenied, "session belongs to an ended integration intent")
	}
	return nil
}

func (owner *Owner) sessionContext(parent context.Context, id string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	owner.mu.Lock()
	session := owner.servers[id]
	owner.mu.Unlock()
	if session == nil {
		return ctx, cancel
	}
	stop := context.AfterFunc(session.lifetime, cancel)
	return ctx, func() { stop(); cancel() }
}

func (owner *Owner) lockSessionMutation(ctx context.Context, sessionID, method string) (func(), error) {
	if owner.mutationExempt[method] {
		return func() {}, nil
	}
	owner.mu.Lock()
	tracked := owner.servers[sessionID]
	owner.mu.Unlock()
	if tracked == nil || tracked.integrationID == "" {
		return func() {}, nil
	}
	return owner.LockIntegrationRead(ctx, tracked.integrationID)
}

func (owner *Owner) LockIntegrationRead(ctx context.Context, id string) (func(), error) {
	return owner.integrationLocks.RLock(ctx, id)
}

func (owner *Owner) LockIntegrationCommit(ctx context.Context, id string, revision uint64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owner.revokeIntegrationRevisionSessions(ctx, id, revision); err != nil {
		return nil, err
	}
	unlock, err := owner.LockIntegration(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := owner.revokeIntegrationRevisionSessions(ctx, id, revision); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

func (owner *Owner) revokeIntegrationRevisionSessions(ctx context.Context, id string, revision uint64) error {
	owner.mu.Lock()
	if owner.integrationRevisions[id] == revision {
		delete(owner.integrationRevisions, id)
	}
	owner.mu.Unlock()
	return owner.revokeWhere(ctx, func(tracked *server) bool {
		return tracked.integrationID == id && tracked.integrationRevision == revision
	})
}

func (owner *Owner) LockIntegration(ctx context.Context, id string) (func(), error) {
	return owner.integrationLocks.Lock(ctx, id)
}
