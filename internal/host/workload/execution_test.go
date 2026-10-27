//go:build darwin || linux

package workload

import (
	"context"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecutionSetAllowsIndependentActivitiesAndExactRelease(t *testing.T) {
	_, run, client := executionTestFixture(t)
	ctx := context.Background()
	first := mainExecution("main:a")
	second := mainExecution("main:b")
	for _, request := range []*hostv1.AcquireExecutionRequest{first, second} {
		acquired, err := client.AcquireExecution(ctx, request)
		if err != nil || !acquired.GetAcquired() {
			t.Fatalf("acquire %s = %+v, %v", request.GetExecutionId(), acquired, err)
		}
	}
	retried, err := client.AcquireExecution(ctx, first)
	if err != nil || retried.GetAcquired() {
		t.Fatalf("idempotent acquire = %+v, %v", retried, err)
	}
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(first)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(first)); err != nil {
		t.Fatalf("duplicate release changed another activity: %v", err)
	}
	if run.executions[second.GetExecutionId()] == nil || len(run.executions) != 1 {
		t.Fatalf("exact release changed another activity: %+v", run.executions)
	}
	listed, err := client.ListExecutions(ctx, listExecutions())
	if err != nil || listed.GetStatus() != hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_PRESENT || len(listed.GetExecutions()) != 1 || listed.GetExecutions()[0].GetExecutionId() != second.GetExecutionId() {
		t.Fatalf("list after release = %+v, %v", listed, err)
	}
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(second)); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListExecutions(ctx, listExecutions())
	if err != nil || listed.GetStatus() != hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY {
		t.Fatalf("empty list = %+v, %v", listed, err)
	}
}

func TestExpiredOperationBecomesUnknownAndStillProtectsWorkload(t *testing.T) {
	_, run, client := executionTestFixture(t)
	request := &hostv1.AcquireExecutionRequest{
		ExecutionId: "auth:probe", ContainerId: "main", ExpectedInstanceId: "instance-a",
		Kind: hostv1.ExecutionKind_EXECUTION_KIND_AUTH_OPERATION, DeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
	}
	if _, err := client.AcquireExecution(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	// Old behavior deleted the final expired holder and scheduled an instance stop.
	// The operation owner now must settle unknown work, so expiry retains the slot.
	run.executions[request.GetExecutionId()].deadline = time.Now().Add(-time.Second)
	listed, err := client.ListExecutions(context.Background(), listExecutions())
	if err != nil || listed.GetExecutions()[0].GetState() != hostv1.ExecutionState_EXECUTION_STATE_UNKNOWN {
		t.Fatalf("expired operation list = %+v, %v", listed, err)
	}
	if run.stopping || run.stopRequestID != "" {
		t.Fatalf("deadline expiry attempted to stop the instance: %+v", run)
	}
	request.DeadlineUnixNano = time.Now().Add(-time.Second).UnixNano()
	if retried, err := client.AcquireExecution(context.Background(), request); err != nil || retried.GetAcquired() {
		t.Fatalf("expired exact acquire retry = %+v, %v", retried, err)
	}
	if _, err := client.QuiesceContainer(context.Background(), &hostv1.QuiesceContainerRequest{RequestId: "quiet", ContainerId: "main", ExpectedInstanceId: "instance-a"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unknown operation no longer protected quiet: %v", err)
	}
}

func TestExecutionCapacityAndLegacyHolderValidation(t *testing.T) {
	_, run, client := executionTestFixture(t)
	ctx := context.Background()
	for index := range maxWorkloadExecutions {
		request := mainExecution(fmt.Sprintf("main:%02d", index))
		if _, err := client.AcquireExecution(ctx, request); err != nil {
			t.Fatalf("acquire %d: %v", index, err)
		}
	}
	if _, err := client.AcquireExecution(ctx, mainExecution("main:overflow")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("capacity overflow = %v", err)
	}
	if retried, err := client.AcquireExecution(ctx, mainExecution("main:00")); err != nil || retried.GetAcquired() {
		t.Fatalf("capacity blocked idempotent retry = %+v, %v", retried, err)
	}
	legacy := mainExecution("main:legacy")
	legacy.HolderId = "old-holder"
	if _, err := client.AcquireExecution(ctx, legacy); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("legacy holder accepted: %v", err)
	}
	if len(run.executions) != maxWorkloadExecutions {
		t.Fatalf("capacity changed after rejected requests: %d", len(run.executions))
	}
}

func TestExecutionFencesQuiesceInstanceAndAuthority(t *testing.T) {
	controller, run, client := executionTestFixture(t)
	ctx := context.Background()
	first := mainExecution("main:first")
	if _, err := client.AcquireExecution(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := client.QuiesceContainer(ctx, &hostv1.QuiesceContainerRequest{RequestId: "quiet", ContainerId: "main", ExpectedInstanceId: "instance-a"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce admitted active execution: %v", err)
	}
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(first)); err != nil {
		t.Fatal(err)
	}
	run.instanceID = "instance-b"
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(first)); status.Code(err) != codes.Aborted {
		t.Fatalf("old instance release = %v", err)
	}
	second := mainExecution("main:second")
	second.ExpectedInstanceId = "instance-b"
	if _, err := client.AcquireExecution(ctx, second); err != nil {
		t.Fatal(err)
	}
	intruder := executionClient(t, controller, auth.Identity{
		Role: auth.Agent, SessionID: "host-control-intruder", EnvironmentID: "env-a", NamespaceID: "a",
		AllocationID: "allocation-b", ExecutionOwnerID: "runtime-instance-a:1",
	})
	if _, err := intruder.ReleaseExecution(ctx, releaseExecutionRequest(second)); status.Code(err) != codes.Aborted {
		t.Fatalf("different allocation authority released activity: %v", err)
	}
}

func TestQuiescedReclaimAcceptsBoundedGuestClockSkew(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	now := time.Now()
	controller.mu.Lock()
	run.quiescing = true
	run.quiesceRequestID = "pressure-reclaim"
	run.stopDeadline = now.Add(workloadStopBudget)
	run.usage = &vmmdv1.Usage{SampledAtUnixNano: now.Add(time.Second).UnixNano()}
	controller.workloadMemoryPressure = true
	controller.workloadPressureAt = now
	controller.workloadReclaim = &workloadReclaimCandidate{run: run, value: &connectorv1.WorkloadReclaimCandidate{
		ContainerInstanceId: run.instanceID,
		ExpiresUnixMillis:   now.Add(workloadStopBudget).UnixMilli(),
	}}
	controller.mu.Unlock()

	identity := auth.Identity{EnvironmentID: "env-a"}
	request := &hostv1.StopContainerRequest{
		ContainerId:        "main",
		ExpectedInstanceId: "instance-a",
		QuiesceRequestId:   "pressure-reclaim",
	}
	if err := controller.ValidateQuiescedStop(identity, request); err != nil {
		t.Fatalf("bounded Guest clock skew rejected quiesced reclaim: %v", err)
	}
}

func TestReplacementControlSessionCanSettleSameOwnerUnderSameAuthority(t *testing.T) {
	controller, run, firstClient := executionTestFixture(t)
	request := mainExecution("main:replacement")
	if _, err := firstClient.AcquireExecution(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	intruder := executionClient(t, controller, auth.Identity{
		Role: auth.Agent, SessionID: "host-control-intruder", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-b:1",
	})
	if _, err := intruder.AcquireExecution(context.Background(), request); status.Code(err) != codes.Aborted {
		t.Fatalf("different authenticated owner reacquired activity: %v", err)
	}
	if _, err := intruder.ReleaseExecution(context.Background(), releaseExecutionRequest(request)); status.Code(err) != codes.Aborted {
		t.Fatalf("different authenticated owner settled activity: %v", err)
	}
	replacement := executionClient(t, controller, auth.Identity{
		Role: auth.Agent, SessionID: "host-control-b", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1",
	})
	listed, err := replacement.ListExecutions(context.Background(), listExecutions())
	if err != nil || listed.GetAllocationAuthority() != "allocation-a" || listed.GetExecutions()[0].GetOwner() != "runtime-instance-a:1" {
		t.Fatalf("replacement list = %+v, %v", listed, err)
	}
	if _, err := replacement.ReleaseExecution(context.Background(), releaseExecutionRequest(request)); err != nil {
		t.Fatalf("same owner could not settle after control replacement: %v", err)
	}
	if len(run.executions) != 0 {
		t.Fatalf("replacement release left activity: %+v", run.executions)
	}
}

func TestHostStreamOwnsAcceptedChildUntilTerminalEvidence(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	identity := auth.Identity{
		Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a",
		AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1",
	}
	token, err := controller.AcquireStreamExecution(context.Background(),
		identity,
		"main",
		"instance-a",
		"process:child-a",
		hostv1.ExecutionKind_EXECUTION_KIND_PROCESS,
		time.Now().Add(time.Minute).UnixNano(),
	)
	if err != nil || run.executions["process:child-a"] == nil {
		t.Fatalf("stream acquire = %+v, %v; executions=%+v", token, err, run.executions)
	}
	if _, err := controller.AcquireStreamExecution(context.Background(),
		identity,
		"main",
		"instance-a",
		"process:child-a",
		hostv1.ExecutionKind_EXECUTION_KIND_PROCESS,
		time.Now().Add(time.Minute).UnixNano(),
	); status.Code(err) != codes.Aborted {
		t.Fatalf("accepted child was replayed: %v", err)
	}

	if run.executions["process:child-a"] == nil {
		t.Fatal("accepted child disappeared before terminal evidence")
	}
	controller.ReleaseStreamExecution(token)
	if len(run.executions) != 0 {
		t.Fatalf("terminal guest evidence did not settle exact child: %+v", run.executions)
	}
}

func TestListExecutionsDistinguishesTargetStates(t *testing.T) {
	controller, run, client := executionTestFixture(t)
	ctx := context.Background()
	controller.workloadRunsRecovered = false
	if _, err := client.ListExecutions(ctx, listExecutions()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("incomplete recovery list = %v", err)
	}
	controller.workloadRunsRecovered = true
	assertListStatus(t, client, ctx, &hostv1.ListExecutionsRequest{ContainerId: "missing", ExpectedInstanceId: "instance-a"}, hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND)
	assertListStatus(t, client, ctx, &hostv1.ListExecutionsRequest{ContainerId: "main", ExpectedInstanceId: "old-instance"}, hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STALE)
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY)
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY)
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", InstanceId: "instance-b", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY)
	if _, err := client.AcquireExecution(ctx, mainExecution("main:pending-stop")); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)
}

func TestStoppedEvidenceIsBoundedFencedAndClearedWithEnvironment(t *testing.T) {
	controller, run, client := executionTestFixture(t)
	ctx := context.Background()
	if _, err := client.AcquireExecution(ctx, mainExecution("main:pending-stop")); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)

	replacement, err := controller.admitWorkloadStart("env-a", "a", "main", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	replacement.inFlight = false
	controller.observeWorkloadRunLocked(replacement, &vmmdv1.Container{Id: "main", InstanceId: "instance-b", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STALE)
	if _, err := client.ReleaseExecution(ctx, releaseExecutionRequest(mainExecution("main:pending-stop"))); status.Code(err) != codes.Aborted {
		t.Fatalf("old stopped execution released replacement: %v", err)
	}
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(replacement, &vmmdv1.Container{Id: "main", InstanceId: "instance-b", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	controller.mu.Unlock()
	assertListStatus(t, client, ctx, &hostv1.ListExecutionsRequest{ContainerId: "main", ExpectedInstanceId: "instance-b"}, hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND)

	controller.mu.Lock()
	controller.stoppedWorkloadInstances[workloadRunKey("env-a", "old")] = "old-instance"
	for index := len(controller.stoppedWorkloadInstances); index < maxStoppedWorkloadInstances; index++ {
		controller.stoppedWorkloadInstances[workloadRunKey(fmt.Sprintf("env-%d", index), "main")] = fmt.Sprintf("instance-%d", index)
	}
	overflow := &Run{environmentID: "env-overflow", containerID: "main", instanceID: "instance-overflow", executions: map[string]*workloadExecution{"pending": {id: "pending"}}}
	controller.workloadRuns[workloadRunKey(overflow.environmentID, overflow.containerID)] = overflow
	controller.observeWorkloadRunLocked(overflow, &vmmdv1.Container{Id: "main", InstanceId: overflow.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	if len(controller.stoppedWorkloadInstances) != maxStoppedWorkloadInstances {
		controller.mu.Unlock()
		t.Fatalf("stopped evidence exceeded bound: %d", len(controller.stoppedWorkloadInstances))
	}
	controller.mu.Unlock()
	controller.ForgetContainers("env-a")
	controller.mu.Lock()
	defer controller.mu.Unlock()
	for key := range controller.stoppedWorkloadInstances {
		if len(key) >= len("env-a\x00") && key[:len("env-a\x00")] == "env-a\x00" {
			t.Fatalf("environment cleanup retained stopped evidence %q", key)
		}
	}
}

func TestHostRestartRetainsRunningInstanceOfOwningAllocation(t *testing.T) {
	for _, owner := range []string{"ready", "draining", "retained", "revoking", "disposing"} {
		t.Run(owner, func(t *testing.T) {
			database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			registration, err := database.CreateRemoteRegistration(ctx, observationTestRegistration("registration"))
			if err != nil {
				t.Fatal(err)
			}
			fixture := observationTestEnvironment("env-a", registration.ID)
			if owner == "disposing" {
				fixture.OwnerKind, fixture.RegistrationID, fixture.AllocationID = store.OwnerLocal, "", ""
			}
			environment, err := database.CreateEnvironment(ctx, fixture)
			if err != nil {
				t.Fatal(err)
			}
			switch owner {
			case "draining", "retained":
				state := map[string]hostv1.AllocationState{"draining": hostv1.AllocationState_ALLOCATION_STATE_DRAINING, "retained": hostv1.AllocationState_ALLOCATION_STATE_RETAINED}[owner]
				if _, err := database.SetAllocationState(ctx, environment.ID, environment.PolicyRevision, int32(state)); err != nil {
					t.Fatal(err)
				}
			case "revoking":
				if _, err := database.BeginRemoteRevocation(ctx, registration.ID, registration.Revision); err != nil {
					t.Fatal(err)
				}
			case "disposing":
				if _, err := database.AcceptLocalDisposal(ctx, store.Operation{ID: "dispose", RequestID: "dispose"}, store.LocalDisposal{Kind: "environment", TargetID: environment.ID, NamespaceID: environment.NamespaceID, Revision: environment.PolicyRevision}); err != nil {
					t.Fatal(err)
				}
			}
			guest := &workloadGuest{
				states: map[string]*vmmdv1.Container{environment.NamespaceID: {Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}},
				pool:   &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{}, WorkloadMemoryLimitBytes: 1},
			}
			controller := workloadTestController(t)
			controller.store = database
			controller.workloadRunsRecovered = false
			if err := controller.RecoverWorkloadRunsFromGuest(ctx, workloadGuestClient(t, guest)); err != nil {
				t.Fatal(err)
			}
			guest.mu.Lock()
			current, stops := guest.states[environment.NamespaceID], guest.stopHits
			guest.mu.Unlock()
			if stops != 0 || current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING || current.GetInstanceId() != "instance-a" {
				t.Fatalf("recovery stopped an owned workload: stops=%d container=%+v", stops, current)
			}
			run := controller.workloadRuns[workloadRunKey(environment.ID, "main")]
			if owner != "ready" && owner != "draining" {
				// Retirement, revocation, and disposal stop the workload through their own owner.
				if run != nil {
					t.Fatalf("recovery admitted a run that its owner is stopping: %+v", run)
				}
				return
			}
			if run == nil || run.stopping || run.instanceID != "instance-a" || run.registrationID != registration.ID {
				t.Fatalf("recovered run = %+v", run)
			}
			client := executionClient(t, controller, auth.Identity{Role: auth.Agent, SessionID: "control", EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID, AllocationGeneration: 1})
			if _, err := client.AcquireExecution(ctx, mainExecution("main:after-restart")); err != nil {
				t.Fatalf("restart recovery rejected work on the retained instance: %v", err)
			}
		})
	}
}

func assertListStatus(t *testing.T, client hostv1.AgentRuntimeServiceClient, ctx context.Context, request *hostv1.ListExecutionsRequest, want hostv1.ExecutionListStatus) {
	t.Helper()
	response, err := client.ListExecutions(ctx, request)
	if err != nil || response.GetStatus() != want {
		t.Fatalf("list status = %+v, %v; want %s", response, err, want)
	}
}

func mainExecution(id string) *hostv1.AcquireExecutionRequest {
	return &hostv1.AcquireExecutionRequest{ExecutionId: id, ContainerId: "main", ExpectedInstanceId: "instance-a", Kind: hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION}
}

func listExecutions() *hostv1.ListExecutionsRequest {
	return &hostv1.ListExecutionsRequest{ContainerId: "main", ExpectedInstanceId: "instance-a"}
}

func executionTestFixture(t *testing.T) (*Owner, *Run, hostv1.AgentRuntimeServiceClient) {
	t.Helper()
	controller := workloadTestController(t)
	run, err := controller.admitWorkloadStart("env-a", "a", "main", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	run.inFlight = false
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}, time.Now())
	controller.mu.Unlock()
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	return controller, run, executionClient(t, controller, identity)
}

func TestHostRestartRestoresOnlyProvenStoppedStartIdentity(t *testing.T) {
	for _, evidence := range []string{"proven", "created", "missing-start", "replacement", "nonempty-instance"} {
		t.Run(evidence, func(t *testing.T) {
			database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			registration := observationTestRegistration("registration")
			if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
				t.Fatal(err)
			}
			environment := observationTestEnvironment("env-a", registration.ID)
			if _, err := database.CreateEnvironment(ctx, environment); err != nil {
				t.Fatal(err)
			}
			container := &vmmdv1.Container{Id: "main", GenerationId: "generation-a", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED, StartRequestInstanceId: "instance-a", StartRequestSha256: strings.Repeat("a", 64), StartedAtUnixNano: 1}
			switch evidence {
			case "created":
				container.State = vmmdv1.ContainerState_CONTAINER_STATE_CREATED
			case "missing-start":
				container.StartRequestSha256 = ""
			case "replacement":
				container.GenerationId = "generation-b"
				container.StartRequestInstanceId = "instance-b"
			case "nonempty-instance":
				container.InstanceId = "instance-b"
			}
			guest := &workloadGuest{states: map[string]*vmmdv1.Container{environment.NamespaceID: container}, pool: &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{}, WorkloadMemoryLimitBytes: 1}}
			controller := workloadTestController(t)
			controller.store = database
			controller.workloadRunsRecovered = false
			if err := controller.RecoverWorkloadRunsFromGuest(ctx, workloadGuestClient(t, guest)); err != nil {
				t.Fatal(err)
			}
			client := executionClient(t, controller, auth.Identity{Role: auth.Agent, SessionID: "control", EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID, AllocationGeneration: 1})
			want := hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND
			if evidence == "proven" {
				want = hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED
			}
			assertListStatus(t, client, ctx, listExecutions(), want)
			wrong := listExecutions()
			wrong.ExpectedInstanceId = "unproven-instance"
			assertListStatus(t, client, ctx, wrong, hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND)
			if _, err := client.AcquireExecution(ctx, mainExecution("main:after-restart")); status.Code(err) != codes.Aborted {
				t.Fatalf("stopped instance admitted execution: %v", err)
			}
		})
	}
}

func releaseExecutionRequest(request *hostv1.AcquireExecutionRequest) *hostv1.ReleaseExecutionRequest {
	return &hostv1.ReleaseExecutionRequest{ContainerId: request.GetContainerId(), ExpectedInstanceId: request.GetExpectedInstanceId(), ExecutionId: request.GetExecutionId(), Kind: request.GetKind()}
}
