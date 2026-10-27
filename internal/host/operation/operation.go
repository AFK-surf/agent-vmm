//go:build darwin || linux

package operation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TerminalTimeout = 5 * time.Second

func Begin(ctx context.Context, database *store.Store, kind, environmentID, requestID string) (store.Operation, bool, error) {
	if requestID == "" {
		return store.Operation{}, false, hostv1.InvalidField("request_id", "request ID is required")
	}
	id, err := NewID()
	if err != nil {
		return store.Operation{}, false, err
	}
	operation, inserted, err := database.PutOperation(ctx, store.Operation{
		ID: id, Kind: kind, EnvironmentID: environmentID, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: requestID,
	})
	if err != nil {
		return store.Operation{}, false, err
	}
	if operation.Kind != kind || operation.EnvironmentID != environmentID {
		return store.Operation{}, false, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID belongs to a different operation", hostv1.WithResource("operation", operation.ID))
	}
	return operation, !inserted, nil
}

func Complete(ctx context.Context, database *store.Store, operation *store.Operation, stage string) error {
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	operation.Stage = stage
	operation.UpdatedAt = time.Now()
	return database.UpdateOperation(ctx, *operation)
}

func Fail(ctx context.Context, database *store.Store, operation *store.Operation, reason string, failure error) error {
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_FAILED)
	operation.Stage = "failed"
	operation.ErrorReason = reason
	operation.ErrorMessage = failure.Error()
	operation.UpdatedAt = time.Now()
	return database.UpdateOperation(ctx, *operation)
}

func InterruptOrphaned(database *store.Store, operation *store.Operation, reason string) error {
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED)
	operation.Stage = "interrupted"
	operation.ErrorReason = reason
	operation.ErrorMessage = "the accepted operation did not save a terminal result; inspect its resource and use a new request ID"
	operation.UpdatedAt = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), TerminalTimeout)
	defer cancel()
	return database.UpdateOperation(ctx, *operation)
}

func NewID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func CommittedFailure(err error) error {
	if err == nil {
		return nil
	}
	value := status.Convert(err).Proto()
	value.Code = int32(codes.Unavailable)
	return status.FromProto(value).Err()
}
