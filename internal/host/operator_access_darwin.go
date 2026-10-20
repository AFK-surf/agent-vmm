//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) ListAgentIntegrations(ctx context.Context, request *hostv1.ListAgentIntegrationsRequest) (*hostv1.ListAgentIntegrationsResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	items, err := server.controller.store.ListAgentIntegrations(ctx, request.GetIncludeRemoved())
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListAgentIntegrationsResponse{}
	for _, item := range items {
		response.Integrations = append(response.Integrations, integrationToProto(item))
	}
	return response, nil
}

func (server *operatorServer) PrepareAgentIntegration(ctx context.Context, request *hostv1.PrepareAgentIntegrationRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if err := validateIntegrationInput(request.GetRequestId(), request.GetAdapterId(), request.GetDisplayName(), request.GetConfigTarget(), request.GetScope()); err != nil {
		return nil, err
	}
	if existing, err := server.controller.store.AgentIntegrationByPrepareRequest(ctx, request.GetRequestId()); err == nil {
		return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(existing)}, nil
	}
	integrationID := stableID("integration", request.GetRequestId())
	environmentID := request.GetEnvironmentId()
	if environmentID == "" {
		environmentID = "integration-" + integrationID
	}
	if len(environmentID) > 128 {
		return nil, invalidField("environment_id", "environment ID is too long")
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	unlock := server.controller.lockEnvironment(environmentID)
	defer unlock()
	environment, err := server.controller.prepareLocalEnvironment(ctx, environmentID, request.GetDisplayName(), request.GetLimits())
	if err != nil {
		return nil, err
	}
	created, err := server.controller.store.CreateAgentIntegration(ctx, store.AgentIntegration{
		ID: integrationID, AdapterID: request.GetAdapterId(), DisplayName: request.GetDisplayName(), Scope: int32(request.GetScope()),
		ConfigTarget: request.GetConfigTarget(), EnvironmentID: environment.ID,
		DesiredState: int32(hostv1.IntegrationDesiredState_INTEGRATION_DESIRED_STATE_ALLOWED),
		ConfigState:  int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_PENDING), PrepareRequestID: request.GetRequestId(),
	})
	if err != nil {
		if existing, lookupErr := server.controller.store.AgentIntegrationByPrepareRequest(ctx, request.GetRequestId()); lookupErr == nil {
			return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(existing)}, nil
		}
		return nil, err
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(created)}, nil
}

func (server *operatorServer) CommitAgentIntegration(ctx context.Context, request *hostv1.CommitAgentIntegrationRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 || request.GetConfigFingerprint() == "" || len(request.GetConfigFingerprint()) > 128 {
		return nil, invalidField("request_id/integration_id/config_fingerprint", "integration commit is invalid")
	}
	state := hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY
	if request.GetNeedsRestart() {
		state = hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_NEEDS_RESTART
	}
	prior, replay, err := server.controller.store.CheckAgentIntegrationCommit(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision(), request.GetConfigFingerprint(), int32(state))
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	if replay {
		return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(prior)}, nil
	}
	unlock, err := server.controller.lockIntegrationCommit(ctx, request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, err
	}
	defer unlock()
	updated, err := server.controller.store.CommitAgentIntegration(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision(), request.GetConfigFingerprint(), int32(state))
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) BeginAgentIntegrationConfiguration(ctx context.Context, request *hostv1.BeginAgentIntegrationConfigurationRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/integration_id/expected_revision", "request ID, integration ID, and revision are required")
	}
	unlock := server.controller.lockIntegration(request.GetIntegrationId())
	defer unlock()
	updated, err := server.controller.store.BeginAgentIntegrationConfiguration(ctx, request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) FailAgentIntegration(ctx context.Context, request *hostv1.FailAgentIntegrationRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 || !boundedError(request.GetErrorCode(), request.GetErrorMessage()) {
		return nil, invalidField("integration failure", "failure details are invalid")
	}
	unlock := server.controller.lockIntegration(request.GetIntegrationId())
	defer unlock()
	updated, err := server.controller.store.FailAgentIntegration(ctx, request.GetIntegrationId(), request.GetExpectedRevision(), request.GetErrorCode(), request.GetErrorMessage())
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) BeginAgentIntegrationRemoval(ctx context.Context, request *hostv1.BeginAgentIntegrationRemovalRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/integration_id/expected_revision", "request ID, integration ID, and revision are required")
	}
	unlock := server.controller.lockIntegration(request.GetIntegrationId())
	defer unlock()
	updated, err := server.controller.store.BeginAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	shareErr := server.controller.settleEnvironmentHostShares(ctx, updated.EnvironmentID)
	server.controller.projectionMu.Lock()
	listenerErr := server.controller.restoreLocalServiceListeners(ctx)
	server.controller.projectionMu.Unlock()
	sessionErr := server.controller.revokeIntegrationSessions(ctx, request.GetIntegrationId())
	if err := errors.Join(shareErr, listenerErr, sessionErr); err != nil {
		return nil, err
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) CompleteAgentIntegrationRemoval(ctx context.Context, request *hostv1.CompleteAgentIntegrationRemovalRequest) (*hostv1.AgentIntegrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/integration_id/expected_revision", "request ID, integration ID, and revision are required")
	}
	unlock := server.controller.lockIntegration(request.GetIntegrationId())
	defer unlock()
	prior, replay, err := server.controller.store.CheckAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	if replay {
		return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(prior)}, nil
	}
	if err := server.controller.revokeIntegrationSessions(ctx, request.GetIntegrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.settleEnvironmentHostShares(ctx, prior.EnvironmentID); err != nil {
		return nil, err
	}
	updated, err := server.controller.store.CompleteAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(request.GetIntegrationId(), err)
	}
	return &hostv1.AgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (controller *Controller) prepareLocalEnvironment(ctx context.Context, environmentID, displayName string, limits *hostv1.ResourceLimits) (store.Environment, error) {
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err == nil {
		if environment.OwnerKind != store.OwnerLocal {
			return store.Environment{}, resourceError(codes.FailedPrecondition, "ENVIRONMENT_REMOTE_OWNED", "environment", environmentID, "environment is owned by a remote registration")
		}
		return environment, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.Environment{}, err
	}
	if err := validatePolicy(limits, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		return store.Environment{}, err
	}
	namespaceID, err := randomID()
	if err != nil {
		return store.Environment{}, err
	}
	return controller.store.CreateEnvironment(ctx, store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: displayName,
		CPUMillis: limits.GetCpuMillis(), MemoryBytes: limits.GetMemoryBytes(), PIDs: limits.GetPids(), DiskBytes: limits.GetDiskBytes(),
		EgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), OwnerKind: store.OwnerLocal,
	})
}

func (server *operatorServer) ListAgentSessions(ctx context.Context, request *hostv1.ListAgentSessionsRequest) (*hostv1.ListAgentSessionsResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	sessions, err := server.controller.store.ListSessions(ctx, request.GetIntegrationId(), request.GetIncludeTerminal())
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListAgentSessionsResponse{}
	for _, session := range sessions {
		response.Sessions = append(response.Sessions, server.controller.sessionToProto(session))
	}
	return response, nil
}

func (server *operatorServer) RevokeAgentSessions(ctx context.Context, request *hostv1.RevokeAgentSessionsRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" {
		return nil, invalidField("request_id/integration_id", "request and integration IDs are required")
	}
	if err := server.controller.revokeIntegrationSessions(ctx, request.GetIntegrationId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func validateIntegrationInput(requestID, adapterID, displayName, configTarget string, scope hostv1.IntegrationScope) error {
	if requestID == "" || adapterID == "" || displayName == "" || configTarget == "" {
		return invalidField("integration", "complete integration details are required")
	}
	if len(requestID) > 128 || len(adapterID) > 64 || len(displayName) > 128 || len(configTarget) > 4096 || strings.ContainsAny(adapterID, "\r\n\t") {
		return invalidField("integration", "integration details exceed supported bounds")
	}
	if scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_USER && scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_WORKSPACE && scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL {
		return invalidField("scope", "integration scope is invalid")
	}
	return nil
}

func boundedError(code, message string) bool {
	return code != "" && len(code) <= 64 && len(message) <= 512 && !strings.ContainsAny(code, "\r\n\t")
}

func integrationConflict(id string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return resourceError(codes.NotFound, "INTEGRATION_NOT_FOUND", "integration", id, "agent integration does not exist")
	}
	return resourceError(codes.Aborted, "INTEGRATION_REVISION_CONFLICT", "integration", id, err.Error())
}

func integrationToProto(integration store.AgentIntegration) *hostv1.AgentIntegration {
	result := &hostv1.AgentIntegration{
		Id: integration.ID, AdapterId: integration.AdapterID, DisplayName: integration.DisplayName,
		Scope: hostv1.IntegrationScope(integration.Scope), ConfigTarget: integration.ConfigTarget, EnvironmentId: integration.EnvironmentID,
		DesiredState: hostv1.IntegrationDesiredState(integration.DesiredState), ConfigState: hostv1.IntegrationConfigState(integration.ConfigState),
		ConfigFingerprint: integration.ConfigFingerprint, Revision: integration.Revision, LastErrorCode: integration.LastErrorCode,
		LastErrorMessage: integration.LastErrorMessage, CreatedAt: timestamppb.New(integration.CreatedAt), UpdatedAt: timestamppb.New(integration.UpdatedAt),
	}
	if integration.LastVerifiedAt != nil {
		result.LastVerifiedAt = timestamppb.New(*integration.LastVerifiedAt)
	}
	return result
}

func (controller *Controller) revokeIntegrationSessions(ctx context.Context, integrationID string) error {
	sessions, err := controller.store.ListSessions(ctx, integrationID, true)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := controller.revokeSession(ctx, session.ID); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) sessionToProto(session store.Session) *hostv1.AgentSession {
	state := hostv1.AgentSessionState_AGENT_SESSION_STATE_LISTENING
	connections := uint32(0)
	controller.mu.Lock()
	if current := controller.sessions[session.ID]; current != nil {
		connections = current.connections.Load()
	}
	controller.mu.Unlock()
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
