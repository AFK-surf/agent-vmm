//go:build darwin || linux

package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAcceptedOperatorVMMutationOutlivesCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation Operation
		kind      string
		stage     string
	}{
		{name: "start", operation: Start, kind: "vm-start", stage: "running"},
		{name: "stop", operation: Stop, kind: "vm-stop", stage: "stopped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := lifecycleTestOwner(t)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			result := make(chan *hostv1.StartVMResponse, 1)
			failure := make(chan error, 1)
			go func() {
				response, err := controller.executeOperatorVMOperation(requestCtx, test.operation, test.kind, "request-1", test.stage, "VM_MUTATION_FAILED", nil, func(executionCtx context.Context, _ store.Operation) error {
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
			replay, err := controller.executeOperatorVMOperation(context.Background(), test.operation, test.kind, "request-1", test.stage, "VM_MUTATION_FAILED", nil, func(context.Context, store.Operation) error {
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
	controller := lifecycleTestOwner(t)
	if err := controller.operationMu.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer controller.operationMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	response, err := controller.executeOperatorVMOperation(ctx, Start, "vm-start", "request-timeout", "running", "VM_START_FAILED", nil, func(context.Context, store.Operation) error {
		t.Fatal("mutation ran without complete lifecycle admission")
		return nil
	})
	if response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	if _, err := controller.store.OperationByRequestID(readCtx, "request-timeout"); err == nil {
		t.Fatal("pre-admission timeout created a durable receipt")
	}
}

func TestOperatorVMMutationReplaysOrphanAsInterrupted(t *testing.T) {
	controller := lifecycleTestOwner(t)
	orphan, _, err := opreceipt.Begin(context.Background(), controller.store, "vm-stop", "system", "request-orphan")
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.executeOperatorVMOperation(context.Background(), Stop, "vm-stop", "request-orphan", "stopped", "VM_STOP_FAILED", nil, func(context.Context, store.Operation) error {
		t.Fatal("orphan replay executed the mutation again")
		return nil
	})
	operation := response.GetOperation()
	if err != nil || operation.GetId() != orphan.ID || operation.GetState() != hostv1.OperationState_OPERATION_STATE_INTERRUPTED || operation.GetError().GetReason() != "VM_TERMINAL_RESULT_UNKNOWN" {
		t.Fatalf("operation=%+v err=%v", operation, err)
	}
}

func TestVMMutationFenceAllowsMembershipAddedAfterWholeVMAcknowledgement(t *testing.T) {
	controller := lifecycleTestOwner(t)
	database := controller.store
	before := controller.Snapshot()
	_, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "environment-new", NamespaceID: "namespace-new", DisplayName: "New", OwnerKind: store.OwnerLocal})
	if err != nil {
		t.Fatal(err)
	}
	fence := &hostv1.VMMutationFence{State: before.GetState(), BootId: before.GetBootId(), WholeVmImpactAcknowledged: true}
	releaseInventory, err := controller.inventory.Transition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = controller.validateVMMutationFenceLocked(fence)
	releaseInventory()
	if err != nil {
		t.Fatalf("whole-VM acknowledgement rejected newly added membership: %v", err)
	}
}

func TestInventoryTransitionWaitsForMutationWithoutFencingWholeVMByWorkloadRevision(t *testing.T) {
	controller := lifecycleTestOwner(t)
	database := controller.store
	environment, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "created", NamespaceID: "created", DisplayName: "Created", PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1, OwnerKind: store.OwnerLocal})
	if err != nil {
		t.Fatal(err)
	}
	before := controller.Snapshot()
	fence := &hostv1.VMMutationFence{State: before.GetState(), BootId: before.GetBootId(), ObservedWorkloads: []*hostv1.ResourceReference{{Kind: "environment", EnvironmentId: environment.ID, Generation: environment.PolicyRevision}}, WholeVmImpactAcknowledged: true}

	_, releaseInventoryReader, err := controller.inventory.Enter(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		releaseInventory, err := controller.inventory.Transition(t.Context())
		if err != nil {
			result <- err
			return
		}
		defer releaseInventory()
		result <- controller.validateVMMutationFenceLocked(fence)
	}()
	select {
	case err := <-result:
		t.Fatalf("transition did not wait for membership mutation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := database.UpdatePolicy(context.Background(), environment.ID, environment.PolicyRevision, store.Environment{PIDs: 256, DiskBytes: 8 << 30, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	releaseInventoryReader()
	if err := <-result; err != nil {
		t.Fatalf("whole-VM acknowledgement rejected a workload revision change: %v", err)
	}
}

func TestInventoryTransitionBlocksNewMembershipMutationAfterValidationBegins(t *testing.T) {
	controller := lifecycleTestOwner(t)
	database := controller.store
	releaseInventory, err := controller.inventory.Transition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finished := make(chan struct{})
	var mutationErr error
	var mutationMu sync.Mutex
	go func() {
		close(started)
		_, releaseInventoryReader, err := controller.inventory.Enter(t.Context())
		if err != nil {
			mutationMu.Lock()
			mutationErr = err
			mutationMu.Unlock()
			close(finished)
			return
		}
		defer releaseInventoryReader()
		_, err = database.CreateEnvironment(context.Background(), store.Environment{ID: "created", NamespaceID: "created", OwnerKind: store.OwnerLocal})
		mutationMu.Lock()
		mutationErr = err
		mutationMu.Unlock()
		close(finished)
	}()
	<-started
	select {
	case <-finished:
		t.Fatal("membership mutation passed the active transition fence")
	case <-time.After(25 * time.Millisecond):
	}
	releaseInventory()
	<-finished
	mutationMu.Lock()
	defer mutationMu.Unlock()
	if mutationErr != nil {
		t.Fatal(mutationErr)
	}
}

func TestVMMutationFenceRequiresWholeVMImpactAcknowledgement(t *testing.T) {
	controller := lifecycleTestOwner(t)
	releaseInventory, err := controller.inventory.Transition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = controller.validateVMMutationFenceLocked(&hostv1.VMMutationFence{State: hostv1.VMState_VM_STATE_STOPPED})
	releaseInventory()
	if err == nil {
		t.Fatal("VM mutation accepted without whole-VM impact acknowledgement")
	}
	for _, fence := range []*hostv1.VMMutationFence{
		{State: hostv1.VMState_VM_STATE_RUNNING, WholeVmImpactAcknowledged: true},
		{State: hostv1.VMState_VM_STATE_STOPPED, BootId: "different-boot", WholeVmImpactAcknowledged: true},
	} {
		if err = controller.validateVMMutationFenceLocked(fence); err == nil {
			t.Fatalf("VM mutation accepted stale state or boot: %v", fence)
		}
	}
}
