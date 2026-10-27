//go:build darwin || linux

package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) PlanCleanupAllocation(ctx context.Context, request *connectorv1.PlanCleanupAllocationRequest) (*connectorv1.AllocationCleanupPlanResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, planReadTimeout)
	defer cancel()
	ctx, releaseInventory, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := validateKind(request.GetKind()); err != nil {
		return nil, err
	}
	if request.GetAllocationId() == "" {
		return nil, hostv1.InvalidField("allocation_id", "allocation identity is required")
	}
	_, environment, err := server.remote.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if err == nil {
		unlockEnvironment, lockErr := server.workloads.LockEnvironment(ctx, environment.ID)
		if lockErr != nil {
			return nil, lockErr
		}
		defer unlockEnvironment()
		_, environment, err = server.remote.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	}
	if errors.Is(err, sql.ErrNoRows) && request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return &connectorv1.AllocationCleanupPlanResponse{Plan: &connectorv1.AllocationCleanupPlan{AllocationId: request.GetAllocationId(), Kind: request.GetKind()}}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	plan, err := server.Plan(ctx, environment.ID, request.GetKind(), true)
	if err != nil {
		return nil, err
	}
	return &connectorv1.AllocationCleanupPlanResponse{Plan: allocationCleanupPlan(request.GetAllocationId(), environment.AllocationGeneration, plan)}, nil
}

func allocationCleanupPlan(allocationID string, allocationGeneration uint64, plan *hostv1.CleanupPlan) *connectorv1.AllocationCleanupPlan {
	result := &connectorv1.AllocationCleanupPlan{AllocationId: allocationID, AllocationGeneration: allocationGeneration, Revision: plan.GetRevision(), Kind: plan.GetKind()}
	for _, impact := range plan.GetImpacts() {
		copy := &hostv1.CleanupImpact{Effect: impact.GetEffect(), Irreversible: impact.GetIrreversible(), Blocker: impact.GetBlocker()}
		if resource := impact.GetResource(); resource != nil {
			copy.Resource = &hostv1.ResourceReference{Kind: resource.GetKind(), ResourceId: resource.GetResourceId()}
			if resource.GetKind() == "volume" {
				result.VolumeIds = append(result.VolumeIds, resource.GetResourceId())
			}
		}
		result.Impacts = append(result.Impacts, copy)
	}
	sort.Strings(result.VolumeIds)
	return result
}

func (server *Owner) ExecuteCleanupAllocation(ctx context.Context, request *connectorv1.ExecuteCleanupAllocationRequest) (*connectorv1.AllocationResponse, error) {
	ctx, cancelAdmission := context.WithTimeout(ctx, planReadTimeout)
	defer cancelAdmission()
	ctx, releaseInventory, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, admissionError(err, request.GetAllocationId())
	}
	defer releaseInventory()
	if request.GetAllocationId() == "" {
		return nil, hostv1.InvalidField("allocation_id", "allocation identity is required")
	}
	_, environment, err := server.remote.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) && request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && request.GetPlanRevision() == 0 && len(request.GetConfirmedVolumeIds()) == 0 {
		return &connectorv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	if request.GetPlanRevision() == 0 {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_PLAN_STALE, "allocation was created after the absence plan", hostv1.WithResource("allocation", request.GetAllocationId()))
	}
	requestID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	requested := &hostv1.CleanupPlan{
		EnvironmentId: environment.ID,
		Kind:          request.GetKind(),
		Revision:      request.GetPlanRevision(),
	}
	operation, err := server.execute(ctx, cleanupActor{remoteOwner: true, confirmedVolumes: request.GetConfirmedVolumeIds(), beforeCleanup: func(executionCtx context.Context, plan *hostv1.CleanupPlan) error {
		_, current, err := server.remote.ReadAllocation(executionCtx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
		if err == nil && current.AllocationGeneration != environment.AllocationGeneration {
			err = status.Error(codes.Aborted, "allocation generation changed")
		}
		if err != nil {
			return err
		}
		_, err = server.remote.RetainEnvironmentLocked(executionCtx, current, planHasMissingNamespace(plan))
		return err
	}}, cleanupRequest{id: requestID, plan: requested},
	)
	if err != nil {
		if operation.ID != "" {
			return nil, opreceipt.CommittedFailure(err)
		}
		return nil, err
	}
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, opreceipt.CommittedFailure(status.Error(codes.FailedPrecondition, operation.ErrorMessage))
	}
	if request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		responseCtx, cancelResponse := context.WithTimeout(context.Background(), terminalTimeout)
		defer cancelResponse()
		current, err := server.remoteEnvironment(responseCtx, request.GetRegistrationId(), request.GetAllocationId())
		if err != nil {
			return nil, opreceipt.CommittedFailure(err)
		}
		return &connectorv1.AllocationResponse{Allocation: server.remote.Allocation(current)}, nil
	}
	return &connectorv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
}

func (server *Owner) remoteEnvironment(ctx context.Context, registrationID, allocationID string) (store.Environment, error) {
	registration, err := server.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil {
		return store.Environment{}, status.Error(codes.NotFound, "active remote registration does not exist")
	}
	environment, err := server.store.GetRemoteEnvironment(ctx, registration.ID, allocationID)
	if err != nil {
		return store.Environment{}, status.Error(codes.NotFound, "allocation does not exist")
	}
	return environment, nil
}
