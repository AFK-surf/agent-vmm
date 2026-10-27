//go:build darwin || linux

package remote

import (
	"context"
	"database/sql"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) DisposeAllocation(ctx context.Context, request *connectorv1.DisposeAllocationRequest) (*connectorv1.AllocationResponse, error) {
	if request.GetRequestId() == "" || len(request.GetRequestId()) > 128 || request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 || request.GetExpectedGeneration() == 0 {
		return nil, hostv1.InvalidField("request_id/allocation_id/revision/generation", "exact allocation deletion authority is required")
	}
	ctx, releaseInventory, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	root, err := server.acceptAllocationDisposal(ctx, request)
	if err != nil {
		return nil, err
	}
	if root.Revision != request.GetExpectedPolicyRevision() {
		return nil, status.Error(codes.Unavailable, "the accepted allocation deletion has a different revision")
	}
	if root.ParentID != "" {
		if !root.Completed {
			return nil, status.Error(codes.Unavailable, "the accepted local deletion must finish first")
		}
		return &connectorv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), Generation: request.GetExpectedGeneration(), Revision: root.Revision, State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
	}
	operation, err := server.store.OperationByRequestID(ctx, request.GetRequestId())
	if err != nil {
		return nil, opreceipt.CommittedFailure(err)
	}
	if root.OperationID != "" && root.OperationID != operation.ID {
		return nil, status.Error(codes.Unavailable, "allocation deletion belongs to another accepted request")
	}
	server.cleanup.WakeLocalDisposal()
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, status.Error(codes.Unavailable, "allocation deletion continues when its runtime is available")
	}
	return &connectorv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), Generation: request.GetExpectedGeneration(), Revision: request.GetExpectedPolicyRevision(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
}

func (server *Owner) acceptAllocationDisposal(ctx context.Context, request *connectorv1.DisposeAllocationRequest) (store.LocalDisposal, error) {
	read := func() (store.LocalDisposal, store.Environment, error) {
		server.remoteAdmissionMu.Lock()
		defer server.remoteAdmissionMu.Unlock()
		if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
			return store.LocalDisposal{}, store.Environment{}, err
		}
		root, err := server.store.AllocationDisposal(ctx, request.GetRegistrationId(), request.GetAllocationId(), request.GetExpectedGeneration())
		if !errors.Is(err, sql.ErrNoRows) {
			return root, store.Environment{}, err
		}
		environment, err := server.store.GetRemoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
		return store.LocalDisposal{}, environment, err
	}
	root, environment, err := read()
	if err != nil {
		if value, ok := status.FromError(err); ok {
			return root, value.Err()
		}
		return root, cleanup.ReadError(err)
	}
	if root.TargetID != "" {
		return root, nil
	}
	unlock, err := server.workloads.LockEnvironment(ctx, environment.ID)
	if err != nil {
		return root, err
	}
	defer unlock()
	server.remoteAdmissionMu.Lock()
	defer server.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return root, err
	}
	root, err = server.store.AllocationDisposal(ctx, request.GetRegistrationId(), request.GetAllocationId(), request.GetExpectedGeneration())
	if !errors.Is(err, sql.ErrNoRows) {
		return root, err
	}
	environment, err = server.store.GetRemoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return root, cleanup.ReadError(err)
	}
	if environment.AllocationGeneration != request.GetExpectedGeneration() {
		return root, status.Error(codes.Aborted, "allocation generation changed before deletion acceptance")
	}
	if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
		return root, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED, "allocation revision changed before deletion acceptance", hostv1.WithResource("allocation", request.GetAllocationId()), hostv1.WithMetadata(map[string]string{"allocation_id": environment.AllocationID, "generation": strconv.FormatUint(environment.AllocationGeneration, 10), "revision": strconv.FormatUint(environment.PolicyRevision, 10)}))
	}
	id, err := opreceipt.NewID()
	if err != nil {
		return root, err
	}
	root = store.LocalDisposal{Kind: "environment", TargetID: environment.ID, NamespaceID: environment.NamespaceID, RegistrationID: request.GetRegistrationId(), AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetExpectedGeneration(), Revision: request.GetExpectedPolicyRevision()}
	operation, err := server.store.AcceptAllocationDisposal(ctx, store.Operation{ID: id, RequestID: request.GetRequestId()}, root)
	if err != nil {
		return root, opreceipt.CommittedFailure(err)
	}
	root.OperationID = operation.ID
	return root, nil
}
