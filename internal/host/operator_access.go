//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) ListAgentIntegrations(ctx context.Context, request *hostv1.ListAgentIntegrationsRequest) (*hostv1.ListAgentIntegrationsResponse, error) {
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

func (server *operatorServer) PrepareAgentIntegration(ctx context.Context, request *hostv1.PrepareAgentIntegrationRequest) (*hostv1.PrepareAgentIntegrationResponse, error) {
	if err := validateIntegrationInput(request.GetRequestId(), request.GetAdapterId(), request.GetDisplayName(), request.GetConfigTarget(), request.GetScope()); err != nil {
		return nil, err
	}
	if existing, err := server.controller.store.AgentIntegrationByPrepareRequest(ctx, request.GetRequestId()); err == nil {
		return &hostv1.PrepareAgentIntegrationResponse{Integration: integrationToProto(existing)}, nil
	}
	integrationID := opreceipt.StableID("integration", request.GetRequestId())
	environmentID := request.GetEnvironmentId()
	if environmentID == "" {
		environmentID = "integration-" + integrationID
	}
	if len(environmentID) > 128 {
		return nil, hostv1.InvalidField("environment_id", "environment ID is too long")
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, environmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	environment, err := server.controller.workloads.PrepareLocalEnvironment(ctx, environmentID, request.GetDisplayName(), request.GetLimits())
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
			return &hostv1.PrepareAgentIntegrationResponse{Integration: integrationToProto(existing)}, nil
		}
		return nil, err
	}
	return &hostv1.PrepareAgentIntegrationResponse{Integration: integrationToProto(created)}, nil
}

func (server *operatorServer) CommitAgentIntegration(ctx context.Context, request *hostv1.CommitAgentIntegrationRequest) (*hostv1.CommitAgentIntegrationResponse, error) {
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 || request.GetConfigFingerprint() == "" || len(request.GetConfigFingerprint()) > 128 {
		return nil, hostv1.InvalidField("request_id/integration_id/config_fingerprint", "integration commit is invalid")
	}
	state := hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY
	if request.GetNeedsRestart() {
		state = hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_NEEDS_RESTART
	}
	prior, replay, err := server.controller.store.CheckAgentIntegrationCommit(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision(), request.GetConfigFingerprint(), int32(state))
	if err != nil {
		return nil, integrationConflict(err)
	}
	if replay {
		return &hostv1.CommitAgentIntegrationResponse{Integration: integrationToProto(prior)}, nil
	}
	unlock, err := server.controller.sessions.LockIntegrationCommit(ctx, request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, err
	}
	defer unlock()
	updated, err := server.controller.store.CommitAgentIntegration(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision(), request.GetConfigFingerprint(), int32(state))
	if err != nil {
		return nil, integrationConflict(err)
	}
	return &hostv1.CommitAgentIntegrationResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) BeginAgentIntegrationRemoval(ctx context.Context, request *hostv1.BeginAgentIntegrationRemovalRequest) (*hostv1.BeginAgentIntegrationRemovalResponse, error) {
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/integration_id/expected_revision", "request ID, integration ID, and revision are required")
	}
	unlock, lockErr := server.controller.sessions.LockIntegration(ctx, request.GetIntegrationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	updated, err := server.controller.store.BeginAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(err)
	}
	shareErr := server.controller.hostShares.SettleEnvironmentHostShares(ctx, updated.EnvironmentID)
	if err := server.controller.services.ProjectionLock().Lock(ctx); err != nil {
		return nil, err
	}

	listenerErr := server.controller.services.RestoreLocalServiceListeners(ctx)
	server.controller.services.ProjectionLock().Unlock()
	sessionErr := server.controller.sessions.RevokeIntegration(ctx, request.GetIntegrationId())
	if err := errors.Join(shareErr, listenerErr, sessionErr); err != nil {
		return nil, err
	}
	return &hostv1.BeginAgentIntegrationRemovalResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) CompleteAgentIntegrationRemoval(ctx context.Context, request *hostv1.CompleteAgentIntegrationRemovalRequest) (*hostv1.CompleteAgentIntegrationRemovalResponse, error) {
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/integration_id/expected_revision", "request ID, integration ID, and revision are required")
	}
	unlock, lockErr := server.controller.sessions.LockIntegration(ctx, request.GetIntegrationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	prior, replay, err := server.controller.store.CheckAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(err)
	}
	if replay {
		return &hostv1.CompleteAgentIntegrationRemovalResponse{Integration: integrationToProto(prior)}, nil
	}
	if err := server.controller.sessions.RevokeIntegration(ctx, request.GetIntegrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.hostShares.SettleEnvironmentHostShares(ctx, prior.EnvironmentID); err != nil {
		return nil, err
	}
	updated, err := server.controller.store.CompleteAgentIntegrationRemoval(ctx, request.GetRequestId(), request.GetIntegrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, integrationConflict(err)
	}
	return &hostv1.CompleteAgentIntegrationRemovalResponse{Integration: integrationToProto(updated)}, nil
}

func (server *operatorServer) ListAgentSessions(ctx context.Context, request *hostv1.ListAgentSessionsRequest) (*hostv1.ListAgentSessionsResponse, error) {
	sessions, err := server.controller.store.ListSessions(ctx, request.GetIntegrationId(), request.GetIncludeTerminal())
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListAgentSessionsResponse{}
	for _, session := range sessions {
		response.Sessions = append(response.Sessions, server.controller.sessions.ToProto(session))
	}
	return response, nil
}

func (server *operatorServer) RevokeAgentSessions(ctx context.Context, request *hostv1.RevokeAgentSessionsRequest) (*hostv1.RevokeAgentSessionsResponse, error) {
	if request.GetRequestId() == "" || request.GetIntegrationId() == "" {
		return nil, hostv1.InvalidField("request_id/integration_id", "request and integration IDs are required")
	}
	if err := server.controller.sessions.RevokeIntegration(ctx, request.GetIntegrationId()); err != nil {
		return nil, err
	}
	return &hostv1.RevokeAgentSessionsResponse{}, nil
}

func validateIntegrationInput(requestID, adapterID, displayName, configTarget string, scope hostv1.IntegrationScope) error {
	if requestID == "" || adapterID == "" || displayName == "" || configTarget == "" {
		return hostv1.InvalidField("integration", "complete integration details are required")
	}
	if len(requestID) > 128 || len(adapterID) > 64 || len(displayName) > 128 || len(configTarget) > 4096 || strings.ContainsAny(adapterID, "\r\n\t") {
		return hostv1.InvalidField("integration", "integration details exceed supported bounds")
	}
	if scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_USER && scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_WORKSPACE && scope != hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL {
		return hostv1.InvalidField("scope", "integration scope is invalid")
	}
	return nil
}

func integrationConflict(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return status.Error(codes.NotFound, "agent integration does not exist")
	}
	return status.Error(codes.Aborted, err.Error())
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
