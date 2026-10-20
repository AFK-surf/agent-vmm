//go:build darwin

package host

import (
	"context"
	"path"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
)

func (controller *Controller) authorizeSession(ctx context.Context, id string) error {
	session, err := controller.store.GetSession(ctx, id)
	if err != nil {
		return resourceError(codes.PermissionDenied, "SESSION_UNAVAILABLE", "session", id, "session authority is unavailable")
	}
	if session.RevokedAt != nil || session.ClosedAt != nil || !session.ExpiresAt.After(time.Now()) {
		return resourceError(codes.PermissionDenied, "SESSION_REVOKED", "session", id, "session authority has ended")
	}
	if session.IntegrationID != "" {
		controller.mu.Lock()
		tracked := controller.sessions[id]
		controller.mu.Unlock()
		integration, err := controller.store.GetAgentIntegration(ctx, session.IntegrationID)
		if err != nil || tracked == nil || tracked.integrationID != session.IntegrationID || tracked.integrationRevision != integration.Revision || integration.EnvironmentID != session.EnvironmentID || integration.DesiredState != int32(hostv1.IntegrationDesiredState_INTEGRATION_DESIRED_STATE_ALLOWED) || (integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY) && integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_NEEDS_RESTART)) {
			return resourceError(codes.PermissionDenied, "INTEGRATION_REVISION_CONFLICT", "integration", session.IntegrationID, "session belongs to an ended integration intent")
		}
	}
	return controller.requireUndisposedEnvironment(ctx, session.EnvironmentID)
}

func (controller *Controller) lockSessionMutation(ctx context.Context, sessionID, method string) (func(), error) {
	if !isMutation(method) || path.Base(method) == "Exec" || path.Base(method) == "ForwardTCP" || method == hostv1.AgentRuntimeService_ReleaseSession_FullMethodName {
		return func() {}, nil
	}
	controller.mu.Lock()
	tracked := controller.sessions[sessionID]
	controller.mu.Unlock()
	if tracked == nil || tracked.integrationID == "" {
		return func() {}, nil
	}
	return controller.lockIntegrationRead(ctx, tracked.integrationID)
}

func (controller *Controller) lockIntegrationRead(ctx context.Context, id string) (func(), error) {
	lock := controller.integrationLock(id)
	for !lock.TryRLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
	return lock.RUnlock, nil
}

func (controller *Controller) lockIntegrationCommit(ctx context.Context, id string, revision uint64) (func(), error) {
	lock := controller.integrationLock(id)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		locked := lock.TryLock()
		if err := controller.revokeIntegrationRevisionSessions(ctx, id, revision); err != nil {
			if locked {
				lock.Unlock()
			}
			return nil, err
		}
		if locked {
			return lock.Unlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (controller *Controller) revokeIntegrationRevisionSessions(ctx context.Context, id string, revision uint64) error {
	controller.mu.Lock()
	var sessions []string
	for sessionID, tracked := range controller.sessions {
		if tracked.integrationID == id && tracked.integrationRevision == revision {
			sessions = append(sessions, sessionID)
		}
	}
	controller.mu.Unlock()
	for _, sessionID := range sessions {
		if err := controller.revokeSession(ctx, sessionID); err != nil {
			return err
		}
	}
	return nil
}
