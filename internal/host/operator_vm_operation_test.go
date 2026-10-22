//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newOperatorVMTestController(t *testing.T) *Controller {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	controller := &Controller{store: database}
	controller.vmLifecycle = &VMLifecycleOwner{controller: controller, shutdownDone: make(chan struct{})}
	return controller
}

func TestAcceptedOperatorVMMutationOutlivesCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation vmLifecycleOperation
		kind      string
		stage     string
	}{
		{name: "start", operation: vmLifecycleStart, kind: "vm-start", stage: "running"},
		{name: "stop", operation: vmLifecycleStop, kind: "vm-stop", stage: "stopped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := newOperatorVMTestController(t)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			result := make(chan *hostv1.OperationResponse, 1)
			failure := make(chan error, 1)
			go func() {
				response, err := controller.executeOperatorVMOperation(requestCtx, test.operation, test.kind, "request-1", test.stage, "VM_MUTATION_FAILED", func(executionCtx context.Context, _ store.Operation) error {
					entered <- executionCtx
					<-release
					return executionCtx.Err()
				})
				result <- response
				failure <- err
			}()
			executionCtx := <-entered
			cancelRequest()
			if err := executionCtx.Err(); err != nil {
				t.Fatalf("accepted execution inherited caller cancellation: %v", err)
			}
			close(release)
			response := <-result
			if err := <-failure; err != nil || response.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED || response.GetOperation().GetStage() != test.stage {
				t.Fatalf("response=%+v err=%v", response, err)
			}
			replay, err := controller.executeOperatorVMOperation(context.Background(), test.operation, test.kind, "request-1", test.stage, "VM_MUTATION_FAILED", func(context.Context, store.Operation) error {
				t.Fatal("terminal request replay executed the mutation again")
				return nil
			})
			if err != nil || replay.GetOperation().GetId() != response.GetOperation().GetId() || replay.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
		})
	}
}

func TestOperatorVMMutationAdmissionTimeoutCreatesNoReceipt(t *testing.T) {
	for _, lock := range []string{"lifecycle", "inventory"} {
		t.Run(lock, func(t *testing.T) {
			controller := newOperatorVMTestController(t)
			if lock == "lifecycle" {
				controller.vmLifecycle.operationMu.Lock()
				defer controller.vmLifecycle.operationMu.Unlock()
			} else {
				controller.inventoryTransition.Lock()
				defer controller.inventoryTransition.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			response, err := controller.executeOperatorVMOperation(ctx, vmLifecycleStart, "vm-start", "request-timeout", "running", "VM_START_FAILED", func(context.Context, store.Operation) error {
				t.Fatal("mutation ran without complete lifecycle admission")
				return nil
			})
			if response != nil || status.Code(err) != codes.DeadlineExceeded || grpcErrorReason(err) != "VM_ADMISSION_TIMEOUT" {
				t.Fatalf("response=%+v err=%v", response, err)
			}
			readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
			defer cancelRead()
			if _, err := controller.store.OperationByRequestID(readCtx, "request-timeout"); err == nil {
				t.Fatal("pre-admission timeout created a durable receipt")
			}
		})
	}
}

func TestOperatorVMMutationReplaysOrphanAsInterrupted(t *testing.T) {
	controller := newOperatorVMTestController(t)
	orphan, _, err := controller.beginOperation(context.Background(), "vm-stop", "system", "request-orphan")
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.executeOperatorVMOperation(context.Background(), vmLifecycleStop, "vm-stop", "request-orphan", "stopped", "VM_STOP_FAILED", func(context.Context, store.Operation) error {
		t.Fatal("orphan replay executed the mutation again")
		return nil
	})
	operation := response.GetOperation()
	if err != nil || operation.GetId() != orphan.ID || operation.GetState() != hostv1.OperationState_OPERATION_STATE_INTERRUPTED || operation.GetError().GetReason() != "VM_TERMINAL_RESULT_UNKNOWN" {
		t.Fatalf("operation=%+v err=%v", operation, err)
	}
}
