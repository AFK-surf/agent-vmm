//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

const (
	cleanupPlanMaxInventory = 100
	cleanupPlanReadTimeout  = 30 * time.Second
	cleanupExecutionTimeout = 2 * time.Minute
	cleanupTerminalTimeout  = 5 * time.Second
)

var errCleanupTerminalPersistence = errors.New("cleanup terminal persistence failed")

func (server *operatorServer) ExecuteCleanup(ctx context.Context, request *hostv1.ExecuteCleanupRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetPlan() == nil || request.GetPlan().GetRevision() == 0 {
		return nil, invalidField("request_id/plan", "request ID and revision-bound plan are required")
	}
	if err := validateCleanupKind(request.GetPlan().GetKind()); err != nil {
		return nil, err
	}
	operation, err := server.executeCleanup(ctx, request.GetRequestId(), request.GetPlan(), request.GetConfirmedVolumeIds(), "")
	if err != nil {
		return nil, err
	}
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) executeCleanup(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string) (operation store.Operation, err error) {
	return server.executeCleanupWithVolumeAuthorization(ctx, requestID, requested, confirmedVolumes, currentSessionID, false)
}

func (server *operatorServer) executeAgentCleanup(ctx context.Context, requestID, environmentID string, kind hostv1.CleanupKind, revision uint64, currentSessionID string) (operation store.Operation, err error) {
	requested := &hostv1.CleanupPlan{EnvironmentId: environmentID, Kind: kind, Revision: revision}
	return server.executeCleanupWithVolumeAuthorization(ctx, requestID, requested, nil, currentSessionID, kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
}

func (server *operatorServer) executeCleanupWithVolumeAuthorization(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes bool) (operation store.Operation, err error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer releaseAdmission()
	return server.executeCleanupAdmissionLocked(ctx, requestID, requested, confirmedVolumes, currentSessionID, authorizeCurrentVolumes)
}

func (server *operatorServer) executeCleanupAdmissionLocked(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes bool) (operation store.Operation, err error) {
	return server.executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmedVolumes, currentSessionID, authorizeCurrentVolumes, false, nil)
}

func (server *operatorServer) executeCleanupAdmissionLockedWithFence(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes, remoteOwner bool, beforeCleanup func(context.Context, *hostv1.CleanupPlan) error) (operation store.Operation, err error) {
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer connection.Close()
	return server.executeCleanupAdmissionLockedWithClient(ctx, client, requestID, requested, confirmedVolumes, currentSessionID, authorizeCurrentVolumes, remoteOwner, beforeCleanup)
}

func (server *operatorServer) executeCleanupAdmissionLockedWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes, remoteOwner bool, beforeCleanup func(context.Context, *hostv1.CleanupPlan) error) (operation store.Operation, err error) {
	if err := validateCleanupKind(requested.GetKind()); err != nil {
		return operation, err
	}
	unlock, err := server.controller.lockEnvironmentContext(ctx, requested.GetEnvironmentId())
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer unlock()
	prior, err := server.controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return operation, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", prior.ID, "request ID belongs to a different operation")
		}
		if prior.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			prior.State = int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED)
			prior.Stage = "interrupted"
			prior.ErrorReason = "CLEANUP_TERMINAL_RESULT_UNKNOWN"
			prior.ErrorMessage = "the accepted cleanup did not save a terminal result; request a new cleanup plan"
			repairCtx, cancelRepair := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
			err := server.controller.store.UpdateOperation(repairCtx, prior)
			cancelRepair()
			if err != nil {
				return operation, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_TERMINAL_PERSIST_FAILED", "operation", prior.ID,
					"cleanup replay could not mark the orphaned operation interrupted",
					map[string]string{"operation_id": prior.ID, "request_id": prior.RequestID})
			}
		}
		return prior, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		if ctx.Err() != nil {
			return server.reconcileCleanupAdmissionFailure(ctx.Err(), requestID, requested)
		}
		return operation, err
	}
	current, err := server.planCleanupWithClient(ctx, client, requested.GetEnvironmentId(), requested.GetKind(), remoteOwner)
	if err != nil {
		if ctx.Err() != nil {
			return operation, cleanupAdmissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if current.GetRevision() != requested.GetRevision() {
		return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", requested.GetEnvironmentId(), "environment resources changed; request a new cleanup plan")
	}
	if requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		expected := make([]string, 0, len(confirmedVolumes))
		for _, impact := range current.GetImpacts() {
			if impact.GetResource().GetKind() == "volume" {
				expected = append(expected, impact.GetResource().GetResourceId())
			}
		}
		sort.Strings(expected)
		confirmed := append([]string(nil), confirmedVolumes...)
		if authorizeCurrentVolumes {
			confirmed = append([]string(nil), expected...)
		}
		sort.Strings(confirmed)
		if !equalStrings(expected, confirmed) {
			return operation, resourceError(codes.FailedPrecondition, "VOLUME_CONFIRMATION_REQUIRED", "environment", requested.GetEnvironmentId(), "the exact current named volume set must be explicitly confirmed")
		}
	}
	if err := ctx.Err(); err != nil {
		return operation, cleanupAdmissionError(err, requested.GetEnvironmentId())
	}
	operation, existing, err := server.controller.beginOperation(ctx, "environment-"+requested.GetKind().String(), requested.GetEnvironmentId(), requestID)
	if err != nil {
		if ctx.Err() != nil {
			return operation, cleanupAdmissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if existing {
		return operation, nil
	}
	accepted, executionErr := server.executeAcceptedCleanup(client, operation, current, currentSessionID, beforeCleanup)
	if errors.Is(executionErr, errCleanupTerminalPersistence) {
		return accepted, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_TERMINAL_PERSIST_FAILED", "operation", accepted.ID,
			"cleanup was accepted but its terminal result could not be saved",
			map[string]string{"operation_id": accepted.ID, "request_id": accepted.RequestID})
	}
	return accepted, executionErr
}

func (server *operatorServer) reconcileCleanupAdmissionFailure(admissionErr error, requestID string, requested *hostv1.CleanupPlan) (store.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	prior, err := server.controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return store.Operation{}, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", prior.ID, "request ID belongs to a different operation")
		}
		return prior, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return store.Operation{}, cleanupAdmissionError(admissionErr, requested.GetEnvironmentId())
	}
	return store.Operation{}, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_ADMISSION_OUTCOME_UNKNOWN", "environment", requested.GetEnvironmentId(),
		"cleanup admission timed out and the request result could not be read",
		map[string]string{"request_id": requestID})
}

func (server *operatorServer) validateEnvironmentDiscard(ctx context.Context, environmentID string) error {
	if err := server.controller.store.ValidateEnvironmentDiscard(ctx, environmentID); err != nil {
		if errors.Is(err, store.ErrEnvironmentHasAgentAccess) {
			return resourceError(codes.FailedPrecondition, "AGENT_ACCESS_ACTIVE", "environment", environmentID,
				"remove agent access before deleting the environment")
		}
		if errors.Is(err, store.ErrRemoteEnvironmentActive) {
			return resourceError(codes.FailedPrecondition, "REMOTE_ALLOCATION_ACTIVE", "environment", environmentID,
				"release the remote allocation before deleting the environment")
		}
		return err
	}
	return nil
}

func validateCleanupKind(kind hostv1.CleanupKind) error {
	if kind != hostv1.CleanupKind_CLEANUP_KIND_RESET && kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return invalidField("plan.kind", "cleanup kind must be reset or discard")
	}
	return nil
}

func (controller *Controller) revokeEnvironmentSessions(ctx context.Context, environmentID, exceptID string) error {
	controller.mu.Lock()
	ids := make([]string, 0, len(controller.sessions))
	for id := range controller.sessions {
		ids = append(ids, id)
	}
	controller.mu.Unlock()
	for _, id := range ids {
		if id == exceptID {
			continue
		}
		session, err := controller.store.GetSession(ctx, id)
		if err != nil {
			continue
		}
		if session.EnvironmentID == environmentID {
			if err := controller.revokeSession(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (server *agentServer) PlanResetEnvironment(ctx context.Context, _ *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_RESET)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *agentServer) PlanDiscardEnvironment(ctx context.Context, _ *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *agentServer) RequestReset(ctx context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := (&operatorServer{controller: server.controller}).executeAgentCleanup(ctx, request.GetRequestId(), identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_RESET, request.GetPlanRevision(), identity.SessionID)
	if err != nil {
		return nil, err
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, false)
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *agentServer) RequestDiscard(ctx context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := (&operatorServer{controller: server.controller}).executeAgentCleanup(ctx, request.GetRequestId(), identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, request.GetPlanRevision(), identity.SessionID)
	if err != nil {
		return nil, err
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, true)
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}
