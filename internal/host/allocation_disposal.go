//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (server *remoteComputeServer) DisposeAllocation(ctx context.Context, request *hostv1.DisposeAllocationRequest) (*hostv1.AllocationResponse, error) {
	if request.GetRequestId() == "" || len(request.GetRequestId()) > 128 || request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 || request.GetExpectedGeneration() == 0 {
		return nil, invalidField("request_id/allocation_id/revision/generation", "exact allocation deletion authority is required")
	}
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}

	root, err := server.controller.store.AllocationDisposal(ctx, request.GetRegistrationId(), request.GetAllocationId(), request.GetExpectedGeneration())
	if errors.Is(err, sql.ErrNoRows) {
		environment, loadErr := server.controller.store.GetRemoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
		if loadErr != nil {
			return nil, disposalReadError(loadErr, request.GetAllocationId())
		}
		unlock, lockErr := server.controller.lockEnvironmentContext(ctx, environment.ID)
		if lockErr != nil {
			return nil, lockErr
		}
		defer unlock()
		// No receipt exists for this allocation scope. Only this rejection permits
		// the controller to refresh a revision before durable acceptance.
		if environment.AllocationGeneration != request.GetExpectedGeneration() {
			return nil, resourceError(codes.Aborted, "ALLOCATION_DISPOSAL_CONFLICT", "allocation", request.GetAllocationId(), "allocation generation changed")
		}
		if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
			return nil, resourceErrorWithMetadata(codes.Aborted, "DISPOSAL_REVISION_NOT_ACCEPTED", "allocation", request.GetAllocationId(), "allocation revision changed before deletion acceptance", map[string]string{"allocation_id": environment.AllocationID, "generation": strconv.FormatUint(environment.AllocationGeneration, 10), "revision": strconv.FormatUint(environment.PolicyRevision, 10)})
		}
		id, idErr := randomID()
		if idErr != nil {
			return nil, idErr
		}
		root = store.LocalDisposal{Kind: "environment", TargetID: environment.ID, NamespaceID: environment.NamespaceID, RegistrationID: request.GetRegistrationId(), AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetExpectedGeneration(), Revision: request.GetExpectedPolicyRevision()}
		if _, err := server.controller.store.AcceptAllocationDisposal(ctx, store.Operation{ID: id, RequestID: request.GetRequestId()}, root); err != nil {
			return nil, resourceError(codes.Aborted, "ALLOCATION_DISPOSAL_CONFLICT", "allocation", request.GetAllocationId(), "allocation deletion scope changed")
		}
	} else if err != nil {
		return nil, disposalReadError(err, request.GetAllocationId())
	}
	if root.Revision != request.GetExpectedPolicyRevision() {
		return nil, resourceError(codes.Aborted, "ALLOCATION_DISPOSAL_CONFLICT", "allocation", request.GetAllocationId(), "allocation deletion revision changed")
	}
	if root.ParentID != "" {
		if !root.Completed {
			return nil, resourceError(codes.Unavailable, "ALLOCATION_DISPOSAL_PENDING", "allocation", request.GetAllocationId(), "the accepted local deletion must finish first")
		}
		return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), Generation: request.GetExpectedGeneration(), Revision: root.Revision, State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
	}
	operation, err := server.controller.store.OperationByRequestID(ctx, request.GetRequestId())
	if err != nil {
		return nil, disposalReadError(err, request.GetAllocationId())
	}
	if root.OperationID != "" && root.OperationID != operation.ID {
		return nil, resourceError(codes.Aborted, "ALLOCATION_DISPOSAL_CONFLICT", "allocation", request.GetAllocationId(), "allocation deletion belongs to another request")
	}
	server.controller.wakeLocalDisposal()
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, resourceError(codes.Unavailable, "ALLOCATION_DISPOSAL_PENDING", "allocation", request.GetAllocationId(), "allocation deletion continues when its runtime is available")
	}
	return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), Generation: request.GetExpectedGeneration(), Revision: request.GetExpectedPolicyRevision(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
}
