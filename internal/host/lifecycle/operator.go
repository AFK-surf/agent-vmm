//go:build darwin || linux

package lifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errVMMutationFence = errors.New("VM mutation fence conflict")

const (
	vmOperatorExecutionTimeout = 2 * time.Minute
	vmOperatorTerminalTimeout  = 5 * time.Second
)

func (controller *Owner) StartVM(ctx context.Context, requestID string, fence *hostv1.VMMutationFence) (*hostv1.StartVMResponse, error) {
	if requestID == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	return controller.executeOperatorVMOperation(ctx, Start, "vm-start", requestID, "running", "VM_START_FAILED", controller.vmFenceValidation(fence), func(executionCtx context.Context, operation store.Operation) error {
		startContext := vmm.WithStartObservation(executionCtx, operation.ID, requestID)
		return controller.StartOwned(startContext)
	})
}

func (controller *Owner) executeOperatorVMOperation(ctx context.Context, lifecycleOperation Operation, kind, requestID, successStage, failureReason string, validate func() error, run func(context.Context, store.Operation) error) (*hostv1.StartVMResponse, error) {
	var response *hostv1.StartVMResponse
	accepted := false
	err := controller.Run(ctx, lifecycleOperation, func() error {
		if validate == nil {
			return nil
		}
		if _, err := controller.store.OperationByRequestID(ctx, requestID); err == nil {
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := validate(); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
		return nil
	}, func(context.Context) error {
		accepted = true
		var err error
		response, err = controller.executeOperatorVMOperationOwned(ctx, kind, requestID, successStage, failureReason, run)
		return err
	})
	if !accepted && err != nil {
		return controller.reconcileOperatorVMAdmission(err, kind, requestID)
	}
	return response, err
}

func (controller *Owner) executeOperatorVMOperationOwned(ctx context.Context, kind, requestID, successStage, failureReason string, run func(context.Context, store.Operation) error) (*hostv1.StartVMResponse, error) {
	if err := ctx.Err(); err != nil {
		return controller.reconcileOperatorVMAdmission(err, kind, requestID)
	}
	acceptCtx, cancelAccept := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	operation, existing, err := opreceipt.Begin(acceptCtx, controller.store, kind, "system", requestID)
	cancelAccept()
	if err != nil {
		return nil, err
	}
	if existing {
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			if err := opreceipt.InterruptOrphaned(controller.store, &operation, "VM_TERMINAL_RESULT_UNKNOWN"); err != nil {
				return nil, operatorVMTerminalError(operation)
			}
		}
		return &hostv1.StartVMResponse{Operation: opreceipt.FromStore(operation)}, nil
	}
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), vmOperatorExecutionTimeout)
	defer cancelExecution()
	if err := run(executionCtx, operation); err != nil {
		if persistErr := controller.persistOperatorVMFailure(&operation, failureReason, err); persistErr != nil {
			return nil, operatorVMTerminalError(operation)
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := controller.persistOperatorVMSuccess(&operation, successStage); err != nil {
		return nil, operatorVMTerminalError(operation)
	}
	return &hostv1.StartVMResponse{Operation: opreceipt.FromStore(operation)}, nil
}

func (controller *Owner) reconcileOperatorVMAdmission(admissionErr error, kind, requestID string) (*hostv1.StartVMResponse, error) {
	readCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	operation, err := controller.store.OperationByRequestID(readCtx, requestID)
	if err == nil {
		if operation.Kind != kind || operation.EnvironmentID != "system" {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID belongs to a different operation", hostv1.WithResource("operation", operation.ID))
		}
		return &hostv1.StartVMResponse{Operation: opreceipt.FromStore(operation)}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		if errors.Is(admissionErr, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "VM mutation timed out before an operation was accepted")
		}
		if errors.Is(admissionErr, context.Canceled) {
			return nil, status.Error(codes.Canceled, "VM mutation was canceled before an operation was accepted")
		}
		return nil, admissionErr
	}
	return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_OUTCOME_UNKNOWN, "VM mutation admission failed and the request result could not be read", hostv1.WithResource("vm", "local"), hostv1.WithMetadata(map[string]string{"request_id": requestID}))
}

func (controller *Owner) persistOperatorVMFailure(operation *store.Operation, reason string, failure error) error {
	terminalCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	return opreceipt.Fail(terminalCtx, controller.store, operation, reason, failure)
}

func (controller *Owner) persistOperatorVMSuccess(operation *store.Operation, stage string) error {
	terminalCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	return opreceipt.Complete(terminalCtx, controller.store, operation, stage)
}

func operatorVMTerminalError(operation store.Operation) error {
	return hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_VM_TERMINAL_PERSIST_FAILED, "VM mutation was accepted but its terminal result could not be saved", hostv1.WithResource("operation", operation.ID), hostv1.WithMetadata(map[string]string{"operation_id": operation.ID, "request_id": operation.RequestID}))
}

func (controller *Owner) vmFenceValidation(fence *hostv1.VMMutationFence) func() error {
	if fence == nil {
		return nil
	}
	return func() error { return controller.validateVMMutationFenceLocked(fence) }
}

func (controller *Owner) validateVMMutationFenceLocked(expected *hostv1.VMMutationFence) error {
	if !expected.GetWholeVmImpactAcknowledged() {
		return fmt.Errorf("%w: whole-VM impact was not acknowledged", errVMMutationFence)
	}
	current := controller.manager.Snapshot()
	if current.GetState() != expected.GetState() || current.GetBootId() != expected.GetBootId() {
		return fmt.Errorf("%w: state or boot identity changed", errVMMutationFence)
	}
	return nil
}

func (controller *Owner) StopVM(ctx context.Context, request *hostv1.StopVMRequest) (*hostv1.StopVMResponse, error) {
	response, err := controller.executeOperatorVMOperation(ctx, Stop, "vm-stop", request.GetRequestId(), "stopped", "VM_STOP_FAILED", controller.vmFenceValidation(request.GetFence()), func(executionCtx context.Context, _ store.Operation) error {
		return controller.StopOwned(executionCtx, request.GetForce())
	})
	return &hostv1.StopVMResponse{Operation: response.GetOperation()}, err
}
