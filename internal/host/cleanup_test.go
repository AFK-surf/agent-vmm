//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cancelWhenObservedContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func (value *cancelWhenObservedContext) Done() <-chan struct{} {
	value.once.Do(func() { close(value.done) })
	return value.done
}

func (value *cancelWhenObservedContext) Err() error {
	select {
	case <-value.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestValidateCleanupKindRejectsNonTerminalValues(t *testing.T) {
	for _, kind := range []hostv1.CleanupKind{hostv1.CleanupKind_CLEANUP_KIND_RESET, hostv1.CleanupKind_CLEANUP_KIND_DISCARD} {
		if err := validateCleanupKind(kind); err != nil {
			t.Fatalf("valid cleanup kind %v rejected: %v", kind, err)
		}
	}
	for _, kind := range []hostv1.CleanupKind{hostv1.CleanupKind_CLEANUP_KIND_UNSPECIFIED, hostv1.CleanupKind(99)} {
		if err := validateCleanupKind(kind); err == nil {
			t.Fatalf("invalid cleanup kind %v accepted", kind)
		}
	}
}

type cleanupNamespaceClient struct {
	vmmdv1.VMMDServiceClient
	err         error
	called      int
	namespaceID string
}

func (client *cleanupNamespaceClient) GetNamespace(_ context.Context, request *vmmdv1.GetNamespaceRequest, _ ...grpc.CallOption) (*vmmdv1.GetNamespaceResponse, error) {
	client.called++
	client.namespaceID = request.GetNamespaceId()
	return &vmmdv1.GetNamespaceResponse{}, client.err
}
func TestDiscardRequiresExactNamespaceAbsence(t *testing.T) {
	for _, test := range []struct {
		name                 string
		kind                 hostv1.CleanupKind
		inventory, namespace error
		missing              bool
		calls                int
		code                 codes.Code
	}{
		{"resource missing namespace present", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.NotFound, "volume missing"), nil, false, 1, codes.OK},
		{"namespace absent", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.NotFound, "missing"), status.Error(codes.NotFound, "namespace missing"), true, 1, codes.OK},
		{"namespace unknown", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.NotFound, "missing"), status.Error(codes.Unavailable, "unknown"), false, 1, codes.Unavailable},
		{"reset", hostv1.CleanupKind_CLEANUP_KIND_RESET, status.Error(codes.NotFound, "missing"), nil, false, 0, codes.OK},
		{"inventory unavailable", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.Unavailable, "unknown"), nil, false, 0, codes.OK},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &cleanupNamespaceClient{err: test.namespace}
			missing, err := confirmMissingNamespaceDiscard(context.Background(), client, "exact-namespace", test.kind, test.inventory)
			if missing != test.missing || status.Code(err) != test.code || client.called != test.calls {
				t.Fatalf("missing=%v err=%v calls=%d", missing, err, client.called)
			}
			if client.called != 0 && client.namespaceID != "exact-namespace" {
				t.Fatal("wrong namespace queried")
			}
		})
	}
}

func TestMissingNamespaceDiscardPlanDisclosesAndBindsGrantMutations(t *testing.T) {
	environment := store.Environment{ID: "retained", NamespaceID: "namespace-retained", PolicyRevision: 7}
	grants := []store.Grant{
		{ID: "active", Kind: "network-link", SourceNamespaceID: environment.NamespaceID, TargetNamespaceID: "survivor", Payload: "active"},
		{ID: "tombstone", Kind: "network-link", SourceNamespaceID: "survivor", TargetNamespaceID: environment.NamespaceID, Payload: "revoked", Revoked: true},
	}
	first := missingNamespaceCleanupPlan(environment, grants)
	second := missingNamespaceCleanupPlan(environment, []store.Grant{grants[1], grants[0]})
	if first.GetRevision() == 0 || first.GetRevision() != second.GetRevision() {
		t.Fatalf("missing namespace revision is not stable: %d %d", first.GetRevision(), second.GetRevision())
	}
	if first.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_DISCARD || len(first.GetImpacts()) != 3 {
		t.Fatalf("missing namespace plan = %+v", first)
	}
	for index, grantID := range []string{"active", "tombstone"} {
		if impact := first.GetImpacts()[index]; impact.GetResource().GetKind() != "grant" || impact.GetResource().GetResourceId() != grantID {
			t.Fatalf("grant impact %d = %+v", index, impact)
		}
	}
	if impact := first.GetImpacts()[2]; impact.GetResource().GetKind() != "environment" || impact.GetResource().GetGenerationId() != "namespace-missing" {
		t.Fatalf("environment impact = %+v", impact)
	}
	grants[1].Payload = "changed"
	if first.GetRevision() == missingNamespaceCleanupPlan(environment, grants).GetRevision() {
		t.Fatal("tombstone change did not invalidate missing-namespace plan")
	}
}

func TestCleanupRevisionTracksResourceSetNotLifecycleState(t *testing.T) {
	grants := []store.Grant{{ID: "grant-a", Payload: "scope", SourceNamespaceID: "namespace-a"}}
	containers := []*vmmdv1.Container{
		{Id: "container-b", GenerationId: "generation-b", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING},
		{Id: "container-a", GenerationId: "generation-a", State: vmmdv1.ContainerState_CONTAINER_STATE_CREATED},
	}
	volumes := []*vmmdv1.Volume{{Id: "volume-a", GenerationId: "volume-generation-a", MountedBy: []string{"container-b"}}}
	first := cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, containers, volumes, nil)

	containers[0].State = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	volumes[0].MountedBy = nil
	second := cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, []*vmmdv1.Container{containers[1], containers[0]}, volumes, nil)
	if first != second {
		t.Fatalf("lifecycle-only change invalidated cleanup plan: %d != %d", first, second)
	}

	withAdditionalContainer := append(containers, &vmmdv1.Container{Id: "container-c"})
	if first == cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, withAdditionalContainer, volumes, nil) {
		t.Fatal("resource-set change did not invalidate cleanup plan")
	}
	containers[0].GenerationId = "generation-b-recreated"
	if first == cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, containers, volumes, nil) {
		t.Fatal("same-ID container recreation did not invalidate cleanup plan")
	}
	containers[0].GenerationId = "generation-b"
	volumes[0].GenerationId = "volume-generation-recreated"
	if first == cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, containers, volumes, nil) {
		t.Fatal("same-ID volume recreation did not invalidate cleanup plan")
	}
}

func TestCleanupChildRequestIDIsStableAndBounded(t *testing.T) {
	value := "operation-delete-workspace-remote-pi-019ffe950d107fa2995194027d5a4f9e"
	first := derivedRequestID(value)
	if len(first) > 63 || first != derivedRequestID(value) || first == derivedRequestID(value+"-different") {
		t.Fatalf("invalid derived request ID %q", first)
	}
}

func TestCleanupInventoryLimitIsActionable(t *testing.T) {
	err := cleanupInventoryLimitError("environment-a")
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "100-object owner work bound") {
		t.Fatalf("limit error=%v", err)
	}
}

func TestFailedCleanupRequestReplaysPriorResultWithoutReplanningOrDeletingNewData(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	prior, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_DISCARD", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_FAILED), Stage: "deleting-containers", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: &Controller{store: database}}
	mutated := false
	replayed, err := server.executeCleanupAdmissionLockedWithFence(context.Background(), "request-cleanup", &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, Revision: 999,
		Impacts: []*hostv1.CleanupImpact{{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "new-volume", GenerationId: "new-generation"}}},
	}, []string{"new-volume"}, "", false, false, func(context.Context, *hostv1.CleanupPlan) error {
		mutated = true
		return nil
	})
	if err != nil || replayed.ID != prior.ID || replayed.State != prior.State || mutated {
		t.Fatalf("replayed=%+v err=%v mutated=%t", replayed, err, mutated)
	}
}

func TestRunningCleanupReplayBecomesDurableInterrupted(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	prior, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: &Controller{store: database}}
	replayed, err := server.executeCleanupAdmissionLockedWithFence(context.Background(), prior.RequestID, &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 999,
	}, nil, "", false, false, nil)
	if err != nil || replayed.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || replayed.ErrorReason != "CLEANUP_TERMINAL_RESULT_UNKNOWN" {
		t.Fatalf("orphan replay=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || stored.ErrorReason != replayed.ErrorReason {
		t.Fatalf("durable orphan repair=%+v err=%v", stored, err)
	}
}

func TestExpiredInitialRequestLookupReconcilesPriorRunningReceipt(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	prior, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "deleting-containers", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &cancelWhenObservedContext{Context: context.Background(), done: make(chan struct{})}
	replayed, err := (&operatorServer{controller: &Controller{store: database}}).executeCleanupAdmissionLockedWithFence(ctx, prior.RequestID, &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, nil, "", false, false, nil)
	if err != nil || replayed.ID != prior.ID || replayed.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || replayed.Stage != prior.Stage {
		t.Fatalf("reconciled request=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || stored.Stage != prior.Stage {
		t.Fatalf("reconciled request was mutated: stored=%+v err=%v", stored, err)
	}
}

func TestAgentCleanupRetryReplaysReceiptBeforePlanRead(t *testing.T) {
	for _, test := range []struct {
		name string
		kind hostv1.CleanupKind
		call func(*agentServer, context.Context, *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error)
	}{
		{name: "reset", kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, call: (*agentServer).RequestReset},
		{name: "discard", kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, call: (*agentServer).RequestDiscard},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			kind := "environment-" + test.kind.String()
			prior, _, err := database.PutOperation(context.Background(), store.Operation{
				ID: "operation-cleanup", Kind: kind, EnvironmentID: "environment-a",
				State: int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED), Stage: "complete", RequestID: "request-cleanup",
			})
			if err != nil {
				t.Fatal(err)
			}
			server := &agentServer{controller: &Controller{store: database, sessions: make(map[string]*sessionServer)}}
			result, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Agent, SessionID: "session-a", EnvironmentID: prior.EnvironmentID, NamespaceID: "namespace-a"})(
				context.Background(),
				&hostv1.EnvironmentOperationRequest{RequestId: prior.RequestID, PlanRevision: 999},
				nil,
				func(ctx context.Context, request any) (any, error) {
					return test.call(server, ctx, request.(*hostv1.EnvironmentOperationRequest))
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			response := result.(*hostv1.OperationResponse)
			if response.GetOperation().GetId() != prior.ID || response.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
				t.Fatalf("agent cleanup replay=%+v", response.GetOperation())
			}
			time.Sleep(20 * time.Millisecond)
		})
	}
}

func TestAcceptedCleanupPersistsFailureWithOwnerContext(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	operation, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: &Controller{store: database}}
	result, resultErr := server.executeAcceptedCleanup(nil, operation, &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, "", func(context.Context, *hostv1.CleanupPlan) error {
		stored, loadErr := database.OperationByRequestID(context.Background(), operation.RequestID)
		if loadErr != nil || stored.ID != operation.ID || stored.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			t.Fatalf("cleanup side effect ran before durable acceptance: stored=%+v err=%v", stored, loadErr)
		}
		return context.DeadlineExceeded
	})
	if resultErr != nil || result.State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) {
		t.Fatalf("accepted cleanup result=%+v err=%v", result, resultErr)
	}
	stored, err := database.OperationByRequestID(context.Background(), operation.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) || stored.ErrorReason != "CLEANUP_FAILED" {
		t.Fatalf("durable cleanup result=%+v err=%v", stored, err)
	}
}

func TestAcceptedCleanupReturnsTerminalPersistenceFailure(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	operation, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "started", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: &Controller{store: database}}
	_, resultErr := server.executeAcceptedCleanup(nil, operation, &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, "", func(context.Context, *hostv1.CleanupPlan) error {
		return errors.New("execution failed")
	})
	if resultErr == nil || !strings.Contains(resultErr.Error(), "execution failed") || !strings.Contains(resultErr.Error(), "persist cleanup failure") {
		t.Fatalf("terminal persistence err=%v", resultErr)
	}
	completion := operation
	if completionErr := server.completeCleanupOperation(&completion); completionErr == nil || !strings.Contains(completionErr.Error(), "persist cleanup completion") {
		t.Fatalf("completion persistence err=%v", completionErr)
	}
}

func TestCleanupInventoryFenceBlocksImageAndNetworkLinkMutation(t *testing.T) {
	controller := &Controller{}
	controller.inventoryTransition.Lock()
	attempted := make(chan string, 2)
	started := make(chan string, 2)
	failed := make(chan error, 2)
	for _, mutation := range []string{"image-import", "network-link-apply"} {
		mutation := mutation
		go func() {
			attempted <- mutation
			release, err := controller.lockInventoryMutation(context.Background())
			if err != nil {
				failed <- err
				return
			}
			defer release()
			started <- mutation
		}()
	}
	<-attempted
	<-attempted
	select {
	case mutation := <-started:
		t.Fatalf("%s crossed the cleanup final-replan fence", mutation)
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(50 * time.Millisecond):
	}
	controller.inventoryTransition.Unlock()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case mutation := <-started:
			seen[mutation] = true
		case err := <-failed:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatalf("mutations did not continue after cleanup completed: %v", seen)
		}
	}
}

func TestExpiredInventoryMutationDoesNotBeginAfterCleanupUnlocks(t *testing.T) {
	controller := &Controller{}
	controller.inventoryTransition.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	mutated := make(chan struct{}, 1)
	go func() {
		release, err := controller.lockInventoryMutation(ctx)
		if err != nil {
			result <- err
			return
		}
		defer release()
		mutated <- struct{}{}
		result <- nil
	}()
	cancel()
	controller.inventoryTransition.Unlock()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expired mutation err=%v", err)
	}
	select {
	case <-mutated:
		t.Fatal("expired mutation began after cleanup released its fence")
	default:
	}
}

func TestCleanupAdmissionDeadlineBoundsInventoryAndRemoteWaits(t *testing.T) {
	requested := &hostv1.CleanupPlan{
		EnvironmentId: "environment-a",
		Kind:          hostv1.CleanupKind_CLEANUP_KIND_RESET,
		Revision:      1,
	}
	for _, test := range []struct {
		name  string
		block func(*Controller) func()
	}{
		{
			name: "inventory mutation",
			block: func(controller *Controller) func() {
				controller.inventoryTransition.RLock()
				return controller.inventoryTransition.RUnlock
			},
		},
		{
			name: "remote admission",
			block: func(controller *Controller) func() {
				controller.remoteAdmissionMu.Lock()
				return controller.remoteAdmissionMu.Unlock
			},
		},
		{
			name: "environment admission",
			block: func(controller *Controller) func() {
				return controller.lockEnvironment("environment-a")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			controller := &Controller{store: database}
			unblock := test.block(controller)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := (&operatorServer{controller: controller}).executeCleanup(ctx, "request-a", requested, nil, "")
				result <- err
			}()
			select {
			case err := <-result:
				if status.Code(err) != codes.DeadlineExceeded || grpcErrorReason(err) != "CLEANUP_ADMISSION_TIMEOUT" {
					t.Fatalf("cleanup admission err=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cleanup admission did not honor its deadline")
			}
			if !controller.inventoryTransition.TryRLock() {
				t.Fatal("timed-out cleanup left a writer queued or held")
			}
			controller.inventoryTransition.RUnlock()
			unblock()
		})
	}
}

func TestCleanupAdmissionTimeoutReplaysRunningRequestWithoutOrphanRepair(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	prior, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "deleting-containers", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	controller.inventoryTransition.RLock()
	defer controller.inventoryTransition.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	replayed, err := (&operatorServer{controller: controller}).executeCleanup(ctx, prior.RequestID, &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, nil, "")
	if err != nil || replayed.ID != prior.ID || replayed.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || replayed.Stage != prior.Stage {
		t.Fatalf("live request replay=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || stored.Stage != prior.Stage {
		t.Fatalf("live request was repaired as orphan: stored=%+v err=%v", stored, err)
	}
}

func TestRemoteCleanupAdmissionDeadlineUsesInventoryFence(t *testing.T) {
	controller := &Controller{}
	controller.inventoryTransition.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := (&remoteComputeServer{controller: controller}).ExecuteCleanupAllocation(ctx, &hostv1.ExecuteCleanupAllocationRequest{AllocationId: "allocation-a"})
		result <- err
	}()
	select {
	case err := <-result:
		if status.Code(err) != codes.DeadlineExceeded || grpcErrorReason(err) != "CLEANUP_ADMISSION_TIMEOUT" {
			t.Fatalf("remote cleanup admission err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote cleanup admission did not honor its deadline")
	}
	if !controller.inventoryTransition.TryRLock() {
		t.Fatal("remote cleanup left a writer queued after timeout")
	}
	controller.inventoryTransition.RUnlock()
	controller.inventoryTransition.RUnlock()
}

func TestRemoteCleanupPlanDeadlineBoundsRemoteAdmission(t *testing.T) {
	controller := &Controller{}
	controller.remoteAdmissionMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := (&remoteComputeServer{controller: controller}).PlanCleanupAllocation(ctx, &hostv1.PlanCleanupAllocationRequest{})
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("remote cleanup plan err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote cleanup plan did not honor its deadline")
	}
	controller.remoteAdmissionMu.Unlock()
}

func grpcErrorReason(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func TestCleanupFenceStopsActualImageAndLinkHandlersBeforeOwnerMutation(t *testing.T) {
	controller := &Controller{}
	controller.inventoryTransition.Lock()
	agent := &agentServer{controller: controller}
	operator := &operatorServer{controller: controller}
	contexts := make([]context.CancelFunc, 0, 2)
	entered := make(chan string, 2)
	results := make(chan error, 2)

	agentContext, cancelAgent := context.WithCancel(context.Background())
	contexts = append(contexts, cancelAgent)
	go func() {
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "environment", NamespaceID: "namespace"})(
			agentContext,
			&hostv1.DeleteImageRequest{RequestId: "delete-image", Reference: "example.com/image", ExpectedGenerationId: "generation", ExpectedDigest: "sha256:digest"},
			nil,
			func(ctx context.Context, request any) (any, error) {
				entered <- "image"
				return agent.DeleteImage(ctx, request.(*hostv1.DeleteImageRequest))
			},
		)
		results <- err
	}()

	operatorContext, cancelOperator := context.WithCancel(context.Background())
	contexts = append(contexts, cancelOperator)
	go func() {
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(
			operatorContext,
			&hostv1.ApplyNetworkLinkRequest{RequestId: "apply-link", Link: &hostv1.NetworkLink{Id: "link"}},
			nil,
			func(ctx context.Context, request any) (any, error) {
				entered <- "link"
				return operator.ApplyNetworkLink(ctx, request.(*hostv1.ApplyNetworkLinkRequest))
			},
		)
		results <- err
	}()

	<-entered
	<-entered
	select {
	case err := <-results:
		t.Fatalf("mutation crossed cleanup fence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	for _, cancel := range contexts {
		cancel()
	}
	controller.inventoryTransition.Unlock()
	for range contexts {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("fenced handler err=%v", err)
		}
	}
}

func TestAllocationCleanupPlanPreservesKindAndImpacts(t *testing.T) {
	plan := allocationCleanupPlan("allocation", 13, &hostv1.CleanupPlan{Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 9, Impacts: []*hostv1.CleanupImpact{
		{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "state"}, Effect: "retained"},
		{Resource: &hostv1.ResourceReference{Kind: "container", ResourceId: "worker"}, Effect: "deleted", Irreversible: true},
	}})
	if plan.GetAllocationId() != "allocation" || plan.GetAllocationGeneration() != 13 || plan.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_RESET || plan.GetRevision() != 9 || len(plan.GetImpacts()) != 2 || len(plan.GetVolumeIds()) != 1 || plan.GetVolumeIds()[0] != "state" {
		t.Fatalf("allocation cleanup projection = %+v", plan)
	}
}

func TestCleanupRevisionRejectsReplacementNamespaceWithEmptyInventory(t *testing.T) {
	original := store.Environment{ID: "original-environment", NamespaceID: "original-namespace", RegistrationID: "controller", AllocationID: "allocation", AllocationGeneration: 1, PolicyRevision: 1}
	replacement := store.Environment{ID: "replacement-environment", NamespaceID: "replacement-namespace", RegistrationID: original.RegistrationID, AllocationID: original.AllocationID, AllocationGeneration: 2, PolicyRevision: original.PolicyRevision}
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			planFor := func(environment store.Environment) *hostv1.CleanupPlan {
				if missing {
					return missingNamespaceCleanupPlan(environment, nil)
				}
				return &hostv1.CleanupPlan{EnvironmentId: environment.ID, Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, Revision: cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, environment.PolicyRevision, environment.NamespaceID, nil, nil, nil, nil)}
			}
			originalPlan := planFor(original)
			currentPlan := planFor(replacement)
			if originalPlan.GetRevision() == 0 || currentPlan.GetRevision() == 0 {
				t.Fatal("namespace plans must have a nonzero revision")
			}
			if currentPlan.GetRevision() == originalPlan.GetRevision() {
				t.Fatal("old plan revision admits cleanup of the replacement generation")
			}
			if planFor(replacement).GetRevision() != currentPlan.GetRevision() {
				t.Fatal("replacement plan is not stable")
			}
		})
	}
}
