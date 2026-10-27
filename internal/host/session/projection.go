//go:build darwin || linux

package session

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (owner *Owner) RevokeIntegration(ctx context.Context, integrationID string) error {
	owner.mu.Lock()
	delete(owner.integrationRevisions, integrationID)
	owner.mu.Unlock()
	return owner.revokeWhere(ctx, func(session *server) bool { return session.integrationID == integrationID })
}

func (owner *Owner) ToProto(session store.Session) *hostv1.AgentSession {
	state := hostv1.AgentSessionState_AGENT_SESSION_STATE_LISTENING
	connections := uint32(0)
	owner.mu.Lock()
	if current := owner.servers[session.ID]; current != nil {
		connections = current.connections.Load()
	}
	owner.mu.Unlock()
	switch {
	case session.ClosedAt != nil:
		state = hostv1.AgentSessionState_AGENT_SESSION_STATE_CLOSED
	case session.RevokedAt != nil:
		state = hostv1.AgentSessionState_AGENT_SESSION_STATE_REVOKED
	case time.Now().After(session.ExpiresAt):
		state = hostv1.AgentSessionState_AGENT_SESSION_STATE_EXPIRED
	case connections > 0:
		state = hostv1.AgentSessionState_AGENT_SESSION_STATE_CONNECTED
	}
	result := &hostv1.AgentSession{
		Id: session.ID, IntegrationId: session.IntegrationID, EnvironmentId: session.EnvironmentID,
		Origin: hostv1.AgentSessionOrigin(session.Origin), State: state, CreatedAt: timestamppb.New(session.CreatedAt),
		ExpiresAt: timestamppb.New(session.ExpiresAt), ActiveConnectionCount: connections, TerminalReason: session.TerminalReason,
	}
	if session.ConnectedAt != nil {
		result.ConnectedAt = timestamppb.New(*session.ConnectedAt)
	}
	if session.LastActivity != nil {
		result.LastActivityAt = timestamppb.New(*session.LastActivity)
	}
	return result
}
