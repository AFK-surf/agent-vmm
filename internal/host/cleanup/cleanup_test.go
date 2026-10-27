//go:build darwin || linux

package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
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
		if err := validateKind(kind); err != nil {
			t.Fatalf("valid cleanup kind %v rejected: %v", kind, err)
		}
	}
	for _, kind := range []hostv1.CleanupKind{hostv1.CleanupKind_CLEANUP_KIND_UNSPECIFIED, hostv1.CleanupKind(99)} {
		if err := validateKind(kind); err == nil {
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
			missing, err := ConfirmMissingNamespaceDiscard(context.Background(), client, "exact-namespace", test.kind, test.inventory)
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
	first := opreceipt.DerivedRequestID(value)
	if len(first) > 63 || first != opreceipt.DerivedRequestID(value) || first == opreceipt.DerivedRequestID(value+"-different") {
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
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
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
	server := newCleanupTestOwner(database)
	mutated := false
	replayed, err := server.execute(context.Background(), cleanupActor{confirmedVolumes: []string{"new-volume"}, beforeCleanup: func(context.Context, *hostv1.CleanupPlan) error {
		mutated = true
		return nil
	}}, cleanupRequest{id: "request-cleanup", plan: &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, Revision: 999,
		Impacts: []*hostv1.CleanupImpact{{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "new-volume", GenerationId: "new-generation"}}},
	}},
	)
	if err != nil || replayed.ID != prior.ID || replayed.State != prior.State || mutated {
		t.Fatalf("replayed=%+v err=%v mutated=%t", replayed, err, mutated)
	}
}

func TestRunningCleanupReplayBecomesDurableInterrupted(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
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
	server := newCleanupTestOwner(database)
	replayed, err := server.execute(context.Background(), cleanupActor{confirmedVolumes: nil, beforeCleanup: nil}, cleanupRequest{id: prior.RequestID, plan: &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 999,
	}})
	if err != nil || replayed.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || replayed.ErrorReason != "CLEANUP_TERMINAL_RESULT_UNKNOWN" {
		t.Fatalf("orphan replay=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || stored.ErrorReason != replayed.ErrorReason {
		t.Fatalf("durable orphan repair=%+v err=%v", stored, err)
	}
}

func TestExpiredInitialRequestLookupReconcilesPriorRunningReceipt(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
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
	replayed, err := newCleanupTestOwner(database).execute(ctx, cleanupActor{confirmedVolumes: nil, beforeCleanup: nil}, cleanupRequest{id: prior.RequestID, plan: &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}})
	if err != nil || replayed.ID != prior.ID || replayed.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || replayed.Stage != prior.Stage {
		t.Fatalf("reconciled request=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || stored.Stage != prior.Stage {
		t.Fatalf("reconciled request was mutated: stored=%+v err=%v", stored, err)
	}
}

func TestAcceptedCleanupPersistsFailureWithOwnerContext(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
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
	server := newCleanupTestOwner(database)
	result, resultErr := server.executeAcceptedCleanup(nil, operation, &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, func(context.Context, *hostv1.CleanupPlan) error {
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
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
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
	server := newCleanupTestOwner(database)
	_, resultErr := server.executeAcceptedCleanup(nil, operation, &hostv1.CleanupPlan{
		EnvironmentId: "environment-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}, func(context.Context, *hostv1.CleanupPlan) error {
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

func newCleanupTestOwner(database *store.Store) *Owner {
	return New(Config{Store: database, Inventory: &hostlifecycle.InventoryFence{}, Workloads: workload.New(workload.Config{Store: database})})
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

type cleanupPlanRemote struct {
	Remote
	reads           int
	secondReadError error
}

func (remote *cleanupPlanRemote) ReadAllocation(context.Context, string, string, string) (store.RemoteRegistration, store.Environment, error) {
	remote.reads++
	if remote.reads > 1 {
		return store.RemoteRegistration{}, store.Environment{}, remote.secondReadError
	}
	return store.RemoteRegistration{}, store.Environment{ID: "environment"}, nil
}

type cleanupPlanWorkloads struct {
	Workloads
	entered chan struct{}
	release chan struct{}
}

func (workloads *cleanupPlanWorkloads) LockEnvironment(ctx context.Context, _ string) (func(), error) {
	close(workloads.entered)
	select {
	case <-workloads.release:
		return func() {}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPlanCleanupAllocationRechecksMissingStateAndAuthorityAfterLock(t *testing.T) {
	for _, test := range []struct {
		name string
		kind hostv1.CleanupKind
		err  error
		code codes.Code
	}{
		{"discard completed", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, sql.ErrNoRows, codes.OK},
		{"reset target removed", hostv1.CleanupKind_CLEANUP_KIND_RESET, sql.ErrNoRows, codes.NotFound},
		{"connection replaced", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.Aborted, "connection replaced"), codes.Aborted},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote := &cleanupPlanRemote{}
			workloads := &cleanupPlanWorkloads{entered: make(chan struct{}), release: make(chan struct{})}
			owner := &Owner{inventory: &hostlifecycle.InventoryFence{}, remote: remote, workloads: workloads}
			done := make(chan struct{})
			var response *connectorv1.AllocationCleanupPlanResponse
			var err error
			go func() {
				defer close(done)
				response, err = owner.PlanCleanupAllocation(context.Background(), &connectorv1.PlanCleanupAllocationRequest{RegistrationId: "registration", ConnectionInstance: "connection", AllocationId: "allocation", Kind: test.kind})
			}()
			<-workloads.entered
			remote.secondReadError = test.err
			close(workloads.release)
			<-done
			if remote.reads != 2 || status.Code(err) != test.code {
				t.Fatalf("reads=%d error=%v", remote.reads, err)
			}
			if test.code == codes.OK && (response.GetPlan().GetAllocationId() != "allocation" || response.GetPlan().GetKind() != test.kind || response.GetPlan().GetRevision() != 0) {
				t.Fatalf("completed discard plan=%v", response)
			}
		})
	}
}
