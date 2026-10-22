//go:build darwin || linux

package host

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (controller *Controller) beginOperation(ctx context.Context, kind, environmentID, requestID string) (store.Operation, bool, error) {
	if requestID == "" {
		return store.Operation{}, false, invalidField("request_id", "request ID is required")
	}
	id, err := randomID()
	if err != nil {
		return store.Operation{}, false, err
	}
	operation, inserted, err := controller.store.PutOperation(ctx, store.Operation{
		ID: id, Kind: kind, EnvironmentID: environmentID, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: requestID,
	})
	if err != nil {
		return store.Operation{}, false, err
	}
	if operation.Kind != kind || operation.EnvironmentID != environmentID {
		return store.Operation{}, false, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", operation.ID, "request ID belongs to a different operation")
	}
	return operation, !inserted, nil
}

func (controller *Controller) completeOperation(ctx context.Context, operation *store.Operation, stage string) error {
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	operation.Stage = stage
	operation.UpdatedAt = time.Now()
	return controller.store.UpdateOperation(ctx, *operation)
}

func (controller *Controller) failOperation(ctx context.Context, operation *store.Operation, reason string, failure error) error {
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_FAILED)
	operation.Stage = "failed"
	operation.ErrorReason = reason
	operation.ErrorMessage = failure.Error()
	operation.UpdatedAt = time.Now()
	return controller.store.UpdateOperation(ctx, *operation)
}
