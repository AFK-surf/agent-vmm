//go:build darwin || linux

package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

const (
	cleanupPlanMaxInventory = 100
	planReadTimeout         = 30 * time.Second
	cleanupExecutionTimeout = 2 * time.Minute
	terminalTimeout         = 5 * time.Second
)

var errCleanupTerminalPersistence = errors.New("cleanup terminal persistence failed")

func (server *Owner) ExecuteCleanup(ctx context.Context, request *hostv1.ExecuteCleanupRequest) (*hostv1.ExecuteCleanupResponse, error) {
	if request.GetRequestId() == "" || request.GetPlan() == nil || request.GetPlan().GetRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/plan", "request ID and revision-bound plan are required")
	}
	operation, err := server.execute(ctx, cleanupActor{confirmedVolumes: request.GetConfirmedVolumeIds()}, cleanupRequest{id: request.GetRequestId(), plan: request.GetPlan()})
	if err != nil {
		return nil, err
	}
	return &hostv1.ExecuteCleanupResponse{Operation: opreceipt.FromStore(operation)}, nil
}

type cleanupActor struct {
	remoteOwner      bool
	confirmedVolumes []string
	beforeCleanup    func(context.Context, *hostv1.CleanupPlan) error
}
type cleanupRequest struct {
	id   string
	plan *hostv1.CleanupPlan
}

func (server *Owner) execute(ctx context.Context, actor cleanupActor, request cleanupRequest) (store.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, planReadTimeout)
	defer cancel()
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, request.id, request.plan)
	}
	defer release()
	return server.executeWithClient(ctx, nil, actor, request)
}

func (server *Owner) executeWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, actor cleanupActor, request cleanupRequest) (operation store.Operation, err error) {
	requestID, requested := request.id, request.plan
	confirmedVolumes := actor.confirmedVolumes
	remoteOwner, beforeCleanup := actor.remoteOwner, actor.beforeCleanup
	if err := validateKind(requested.GetKind()); err != nil {
		return operation, err
	}
	unlock, lockErr := server.workloads.LockEnvironment(ctx, requested.GetEnvironmentId())
	if lockErr != nil {
		return server.reconcileCleanupAdmissionFailure(lockErr, requestID, requested)
	}
	defer unlock()
	prior, err := server.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return operation, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID belongs to a different operation", hostv1.WithResource("operation", prior.ID))
		}
		if prior.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			err := opreceipt.InterruptOrphaned(server.store, &prior, "CLEANUP_TERMINAL_RESULT_UNKNOWN")
			if err != nil {
				return operation, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_CLEANUP_TERMINAL_PERSIST_FAILED, "cleanup replay could not mark the orphaned operation interrupted", hostv1.WithResource("operation", prior.ID), hostv1.WithMetadata(map[string]string{"operation_id": prior.ID, "request_id": prior.RequestID}))
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
	if client == nil {
		client, err = server.guestConnection.Client()
		if err != nil {
			return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
		}
	}
	current, err := server.planCleanupWithClient(ctx, client, requested.GetEnvironmentId(), requested.GetKind(), remoteOwner)
	if err != nil {
		if ctx.Err() != nil {
			return operation, admissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if current.GetRevision() != requested.GetRevision() {
		return operation, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_CLEANUP_PLAN_STALE, "environment resources changed; request a new cleanup plan", hostv1.WithResource("environment", requested.GetEnvironmentId()))
	}
	if requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		expected := cleanupPlanVolumeIDs(current)
		sort.Strings(expected)
		confirmed := append([]string(nil), confirmedVolumes...)
		sort.Strings(confirmed)
		if !slices.Equal(expected, confirmed) {
			return operation, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VOLUME_CONFIRMATION_REQUIRED, "the exact current named volume set must be explicitly confirmed", hostv1.WithResource("environment", requested.GetEnvironmentId()))
		}
	}
	if err := ctx.Err(); err != nil {
		return operation, admissionError(err, requested.GetEnvironmentId())
	}
	operation, existing, err := opreceipt.Begin(ctx, server.store, "environment-"+requested.GetKind().String(), requested.GetEnvironmentId(), requestID)
	if err != nil {
		if ctx.Err() != nil {
			return operation, admissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if existing {
		return operation, nil
	}
	accepted, executionErr := server.executeAcceptedCleanup(client, operation, current, beforeCleanup)
	if errors.Is(executionErr, errCleanupTerminalPersistence) {
		return accepted, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_CLEANUP_TERMINAL_PERSIST_FAILED, "cleanup was accepted but its terminal result could not be saved", hostv1.WithResource("operation", accepted.ID), hostv1.WithMetadata(map[string]string{"operation_id": accepted.ID, "request_id": accepted.RequestID}))
	}
	return accepted, executionErr
}

func (server *Owner) reconcileCleanupAdmissionFailure(admissionErr error, requestID string, requested *hostv1.CleanupPlan) (store.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalTimeout)
	defer cancel()
	prior, err := server.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return store.Operation{}, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID belongs to a different operation", hostv1.WithResource("operation", prior.ID))
		}
		return prior, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return store.Operation{}, admissionError(admissionErr, requested.GetEnvironmentId())
	}
	return store.Operation{}, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_CLEANUP_ADMISSION_OUTCOME_UNKNOWN, "cleanup admission timed out and the request result could not be read", hostv1.WithResource("environment", requested.GetEnvironmentId()), hostv1.WithMetadata(map[string]string{"request_id": requestID}))
}

func (server *Owner) validateEnvironmentDiscard(ctx context.Context, environmentID string) error {
	if err := server.store.ValidateEnvironmentDiscard(ctx, environmentID); err != nil {
		if errors.Is(err, store.ErrEnvironmentHasAgentAccess) {
			return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_AGENT_ACCESS_ACTIVE, "remove agent access before deleting the environment", hostv1.WithResource("environment", environmentID))
		}
		if errors.Is(err, store.ErrRemoteEnvironmentActive) {
			return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_REMOTE_ALLOCATION_ACTIVE, "release the remote allocation before deleting the environment", hostv1.WithResource("environment", environmentID))
		}
		return err
	}
	return nil
}

func validateKind(kind hostv1.CleanupKind) error {
	if kind != hostv1.CleanupKind_CLEANUP_KIND_RESET && kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return hostv1.InvalidField("plan.kind", "cleanup kind must be reset or discard")
	}
	return nil
}

func cleanupPlanVolumeIDs(plan *hostv1.CleanupPlan) []string {
	var result []string
	for _, impact := range plan.GetImpacts() {
		if resource := impact.GetResource(); resource.GetKind() == "volume" {
			result = append(result, resource.GetResourceId())
		}
	}
	return result
}
