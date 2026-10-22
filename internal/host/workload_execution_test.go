//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestExecutionSetAllowsIndependentActivitiesAndExactRelease(t *testing.T) {
	_, run, client := executionTestFixture(t)
	ctx := context.Background()
	first := mainExecution("main:a")
	second := mainExecution("main:b")
	for _, request := range []*hostv1.ExecutionRequest{first, second} {
		acquired, err := client.AcquireExecution(ctx, request)
		if err != nil || !acquired.GetAcquired() {
			t.Fatalf("acquire %s = %+v, %v", request.GetExecutionId(), acquired, err)
		}
	}
	retried, err := client.AcquireExecution(ctx, first)
	if err != nil || retried.GetAcquired() {
		t.Fatalf("idempotent acquire = %+v, %v", retried, err)
	}
	if _, err := client.ReleaseExecution(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReleaseExecution(ctx, first); err != nil {
		t.Fatalf("duplicate release changed another activity: %v", err)
	}
	if run.executions[second.GetExecutionId()] == nil || len(run.executions) != 1 {
		t.Fatalf("exact release changed another activity: %+v", run.executions)
	}
	listed, err := client.ListExecutions(ctx, listExecutions())
	if err != nil || listed.GetStatus() != hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_PRESENT || len(listed.GetExecutions()) != 1 || listed.GetExecutions()[0].GetExecutionId() != second.GetExecutionId() {
		t.Fatalf("list after release = %+v, %v", listed, err)
	}
	if _, err := client.ReleaseExecution(ctx, second); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListExecutions(ctx, listExecutions())
	if err != nil || listed.GetStatus() != hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY {
		t.Fatalf("empty list = %+v, %v", listed, err)
	}
}

func TestExpiredOperationBecomesUnknownAndStillProtectsWorkload(t *testing.T) {
	_, run, client := executionTestFixture(t)
	request := &hostv1.ExecutionRequest{
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
	if _, err := client.ReleaseExecution(ctx, first); err != nil {
		t.Fatal(err)
	}
	run.instanceID = "instance-b"
	if _, err := client.ReleaseExecution(ctx, first); status.Code(err) != codes.Aborted {
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
	if _, err := intruder.ReleaseExecution(ctx, second); status.Code(err) != codes.Aborted {
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
	controller.workloadReclaim = &workloadReclaimCandidate{run: run, value: &hostv1.WorkloadReclaimCandidate{
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
	if err := controller.validateQuiescedStop(identity, request); err != nil {
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
	if _, err := intruder.ReleaseExecution(context.Background(), request); status.Code(err) != codes.Aborted {
		t.Fatalf("different authenticated owner settled activity: %v", err)
	}
	replacement := executionClient(t, controller, auth.Identity{
		Role: auth.Agent, SessionID: "host-control-b", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1",
	})
	listed, err := replacement.ListExecutions(context.Background(), listExecutions())
	if err != nil || listed.GetAllocationAuthority() != "allocation-a" || listed.GetExecutions()[0].GetOwner() != "runtime-instance-a:1" {
		t.Fatalf("replacement list = %+v, %v", listed, err)
	}
	if _, err := replacement.ReleaseExecution(context.Background(), request); err != nil {
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
	token, err := controller.acquireStreamExecution(
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
	if _, err := controller.acquireStreamExecution(
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
	controller.releaseStreamExecution(token)
	if len(run.executions) != 0 {
		t.Fatalf("terminal guest evidence did not settle exact child: %+v", run.executions)
	}
}

type detachedExecGuest struct {
	testGuestClientStream
	accepted   chan struct{}
	released   chan struct{}
	closed     chan struct{}
	terminal   bool
	acceptOnce sync.Once
	closeOnce  sync.Once
}

func (guest *detachedExecGuest) Send(*vmmdv1.ExecRequest) error {
	guest.acceptOnce.Do(func() { close(guest.accepted) })
	return nil
}
func (guest *detachedExecGuest) Recv() (*vmmdv1.ExecResponse, error) {
	<-guest.released
	if guest.terminal {
		return &vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_ExitCode{ExitCode: 0}}, nil
	}
	return nil, io.EOF
}
func (guest *detachedExecGuest) Close() error {
	guest.closeOnce.Do(func() { close(guest.closed) })
	return nil
}

func TestExecDisconnectReleasesSessionAndRetainsTerminalObservation(t *testing.T) {
	for _, mode := range []string{"terminal", "unknown", "exact-stop", "cancel-race"} {
		t.Run(mode, func(t *testing.T) {
			controller, run, _ := executionTestFixture(t)
			database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			controller.store = database
			if _, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "env-a", NamespaceID: "a", OwnerKind: store.OwnerLocal, CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
				t.Fatal(err)
			}
			if err := database.CreateSession(context.Background(), store.Session{ID: "old-session", EnvironmentID: "env-a", NamespaceID: "a", SocketPath: "/unused", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
				t.Fatal(err)
			}
			guest := &detachedExecGuest{accepted: make(chan struct{}), released: make(chan struct{}), closed: make(chan struct{}), terminal: mode != "unknown"}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(guest.released) }) })
			listener := bufconn.Listen(1 << 20)
			identity := auth.Identity{Role: auth.Agent, SessionID: "old-session", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
			server := grpc.NewServer(grpc.WaitForHandlers(true), grpc.StreamInterceptor(auth.StreamInterceptor(identity)))
			hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller, openGuestExec: func(ctx context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error) {
				guest.ctx = ctx
				return guest, guest, nil
			}})
			controller.mu.Lock()
			controller.sessions = map[string]*sessionServer{identity.SessionID: {server: server, listener: listener, stopDone: make(chan struct{})}}
			controller.mu.Unlock()
			go server.Serve(listener)
			defer server.Stop()
			connection, err := grpc.NewClient("passthrough:///exec-revoke", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := hostv1.NewAgentRuntimeServiceClient(connection).Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: "pending-callback", ContainerId: "main", ExpectedInstanceId: "instance-a", ExecutionId: "exec:callback", ExecutionKind: hostv1.ExecutionKind_EXECUTION_KIND_EXEC, ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano()}}}); err != nil {
				t.Fatal(err)
			}
			<-guest.accepted
			if mode == "cancel-race" {
				go cancel()
			} else {
				cancel()
			}
			stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
			defer stopCancel()
			if err := controller.revokeSession(stopCtx, identity.SessionID); err != nil {
				t.Fatalf("unresolved callback blocked session revoke: %v", err)
			}
			if guest.Context().Err() != nil {
				t.Fatal("session revoke cancelled accepted Guest execution")
			}
			replacement := identity
			replacement.SessionID = "new-session"
			client := executionClient(t, controller, replacement)
			listed, err := client.ListExecutions(context.Background(), listExecutions())
			if err != nil || len(listed.GetExecutions()) != 1 {
				t.Fatalf("replacement lost accepted execution: %v %v", listed, err)
			}
			if mode == "exact-stop" {
				stopped := &workloadGuest{omitStoppedInstance: true, states: map[string]*vmmdv1.Container{"a": {Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}}
				guestClient := workloadGuestClient(t, stopped)
				unlock := controller.lockEnvironment("env-a")
				request := &hostv1.StopContainerRequest{RequestId: "replacement-stop", ContainerId: "main", ExpectedInstanceId: "instance-a"}
				if err := controller.validateExplicitStop(replacement, request); err != nil {
					t.Fatal(err)
				}
				_, err := controller.stopWorkloadContainerLocked(context.Background(), guestClient, "env-a", &vmmdv1.StopContainerRequest{RequestId: request.RequestId, ActorNamespaceId: "a", ContainerId: "main"})
				unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			release.Do(func() { close(guest.released) })
			select {
			case <-guest.closed:
			case <-time.After(time.Second):
				t.Fatal("Host terminal observer did not finish")
			}
			controller.mu.Lock()
			retained := run.executions["exec:callback"] != nil
			controller.mu.Unlock()
			if mode == "exact-stop" {
				assertListStatus(t, client, context.Background(), listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)
			} else if retained != (mode == "unknown") {
				t.Fatalf("terminal=%s retained=%v", mode, retained)
			}
		})
	}
}

func TestExecAmbiguousFirstGuestSendKeepsExecutionRight(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	guest := &ambiguousExecGuestStream{testGuestClientStream: testGuestClientStream{ctx: context.Background()}}
	client := executionClientWithServer(t, controller, &agentServer{
		controller: controller,
		openGuestExec: func(context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error) {
			return guest, io.NopCloser(nilReader{}), nil
		},
	})
	stream, err := client.Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	request := &hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{
		RequestId: "exec-request", ContainerId: "main", ExpectedInstanceId: "instance-a",
		ExecutionId: "exec:ambiguous", ExecutionKind: hostv1.ExecutionKind_EXECUTION_KIND_EXEC,
		ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
	}}}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous Guest send = %v", err)
	}
	if !guest.accepted || run.executions["exec:ambiguous"] == nil {
		t.Fatalf("accepted exec lost its right: accepted=%v executions=%+v", guest.accepted, run.executions)
	}
	if _, err := client.QuiesceContainer(context.Background(), &hostv1.QuiesceContainerRequest{RequestId: "quiet", ContainerId: "main", ExpectedInstanceId: "instance-a"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce crossed ambiguous exec: %v", err)
	}
}

func TestBuildAmbiguousFirstGuestSendKeepsExecutionRight(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	guest := &ambiguousBuildGuestStream{testGuestClientStream: testGuestClientStream{ctx: context.Background()}}
	client := executionClientWithServer(t, controller, &agentServer{
		controller:    controller,
		buildLifetime: context.Background(),
		openGuestBuild: func(context.Context) (vmmdv1.VMMDService_BuildImageClient, io.Closer, error) {
			return guest, io.NopCloser(nilReader{}), nil
		},
	})
	stream, err := client.BuildImage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	request := &hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: &hostv1.BuildImageHeader{
		RequestId: "build-request", ContainerId: "main", ExpectedInstanceId: "instance-a",
		ExecutionId: "build:ambiguous", ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(),
	}}}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous Guest send = %v", err)
	}
	if !guest.accepted || run.executions["build:ambiguous"] == nil {
		t.Fatalf("accepted build lost its right: accepted=%v executions=%+v", guest.accepted, run.executions)
	}
	if _, err := client.QuiesceContainer(context.Background(), &hostv1.QuiesceContainerRequest{RequestId: "quiet", ContainerId: "main", ExpectedInstanceId: "instance-a"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce crossed ambiguous build: %v", err)
	}
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }

type testGuestClientStream struct {
	ctx context.Context
}

func (s *testGuestClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *testGuestClientStream) Trailer() metadata.MD         { return nil }
func (s *testGuestClientStream) CloseSend() error             { return nil }
func (s *testGuestClientStream) Context() context.Context     { return s.ctx }
func (s *testGuestClientStream) SendMsg(any) error            { return nil }
func (s *testGuestClientStream) RecvMsg(any) error            { return io.EOF }

type ambiguousExecGuestStream struct {
	testGuestClientStream
	accepted bool
}

func (s *ambiguousExecGuestStream) Send(*vmmdv1.ExecRequest) error {
	s.accepted = true
	return status.Error(codes.Unavailable, "response path lost after accept")
}

func (*ambiguousExecGuestStream) Recv() (*vmmdv1.ExecResponse, error) { return nil, io.EOF }

type ambiguousBuildGuestStream struct {
	testGuestClientStream
	accepted bool
}

func (s *ambiguousBuildGuestStream) Send(*vmmdv1.BuildImageRequest) error {
	s.accepted = true
	return status.Error(codes.Unavailable, "response path lost after accept")
}

func (*ambiguousBuildGuestStream) Recv() (*vmmdv1.BuildImageResponse, error) {
	return nil, io.EOF
}

func TestListExecutionsDistinguishesTargetStates(t *testing.T) {
	controller, run, client := executionTestFixture(t)
	ctx := context.Background()
	controller.workloadRunsRecovered = false
	if _, err := client.ListExecutions(ctx, listExecutions()); status.Code(err) != codes.Unavailable {
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
	if _, err := client.ReleaseExecution(ctx, mainExecution("main:pending-stop")); status.Code(err) != codes.Aborted {
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
	overflow := &workloadRun{environmentID: "env-overflow", containerID: "main", instanceID: "instance-overflow", executions: map[string]*workloadExecution{"pending": {id: "pending"}}}
	controller.workloadRuns[workloadRunKey(overflow.environmentID, overflow.containerID)] = overflow
	controller.observeWorkloadRunLocked(overflow, &vmmdv1.Container{Id: "main", InstanceId: overflow.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}, time.Now())
	if len(controller.stoppedWorkloadInstances) != maxStoppedWorkloadInstances {
		controller.mu.Unlock()
		t.Fatalf("stopped evidence exceeded bound: %d", len(controller.stoppedWorkloadInstances))
	}
	controller.mu.Unlock()
	controller.forgetContainers("env-a")
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
			if err := controller.recoverWorkloadRunsFromGuest(ctx, workloadGuestClient(t, guest)); err != nil {
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

func mainExecution(id string) *hostv1.ExecutionRequest {
	return &hostv1.ExecutionRequest{ExecutionId: id, ContainerId: "main", ExpectedInstanceId: "instance-a", Kind: hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION}
}

func listExecutions() *hostv1.ListExecutionsRequest {
	return &hostv1.ListExecutionsRequest{ContainerId: "main", ExpectedInstanceId: "instance-a"}
}

func executionTestFixture(t *testing.T) (*Controller, *workloadRun, hostv1.AgentRuntimeServiceClient) {
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

func executionClient(t *testing.T, controller *Controller, identity auth.Identity) hostv1.AgentRuntimeServiceClient {
	return executionClientWithIdentityAndServer(t, identity, &agentServer{controller: controller})
}

func executionClientWithServer(t *testing.T, controller *Controller, serverImpl *agentServer) hostv1.AgentRuntimeServiceClient {
	t.Helper()
	identity := auth.Identity{
		Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a",
		AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1",
	}
	if serverImpl.controller == nil {
		serverImpl.controller = controller
	}
	return executionClientWithIdentityAndServer(t, identity, serverImpl)
}

func executionClientWithIdentityAndServer(t *testing.T, identity auth.Identity, serverImpl *agentServer) hostv1.AgentRuntimeServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryInterceptor(identity)),
		grpc.StreamInterceptor(auth.StreamInterceptor(identity)),
	)
	hostv1.RegisterAgentRuntimeServiceServer(server, serverImpl)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///host", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection)
}

func TestExplicitStopFencesInstanceAndSettlesActiveExecution(t *testing.T) {
	controller, run, client := executionTestFixture(t)
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	if _, err := client.AcquireExecution(context.Background(), mainExecution("main:active")); err != nil {
		t.Fatal(err)
	}
	guest := &workloadGuest{omitStoppedInstance: true, states: map[string]*vmmdv1.Container{"a": {Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}}
	guestClient := workloadGuestClient(t, guest)
	request := &hostv1.StopContainerRequest{RequestId: "stop-exact", ContainerId: "main", ExpectedInstanceId: "old-instance"}
	unlock := controller.lockEnvironment("env-a")
	if err := controller.validateExplicitStop(identity, request); status.Code(err) != codes.Aborted {
		t.Fatalf("stale stop = %v", err)
	}
	if guest.stopHits != 0 || run.stopping || len(run.executions) != 1 {
		t.Fatal("stale instance stop changed current execution")
	}
	request.ExpectedInstanceId = "instance-a"
	request.QuiesceRequestId = "unowned-quiesce"
	if err := controller.validateExplicitStop(identity, request); status.Code(err) != codes.Aborted {
		t.Fatalf("unowned quiet stop = %v", err)
	}
	request.QuiesceRequestId = ""
	if err := controller.validateExplicitStop(identity, request); err != nil {
		t.Fatal(err)
	}
	response, err := controller.stopWorkloadContainerLocked(context.Background(), guestClient, "env-a", &vmmdv1.StopContainerRequest{RequestId: request.RequestId, ActorNamespaceId: "a", ContainerId: "main"})
	unlock()
	if err != nil || response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED || response.GetContainer().GetInstanceId() != "" || guest.stopHits != 1 {
		t.Fatalf("exact active stop = %+v, %v", response, err)
	}
	assertListStatus(t, client, context.Background(), listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)
}

func TestExecTerminalExitPreservesOutputDespiteClosedInput(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal=%v", terminal), func(t *testing.T) {
			controller, run, _ := executionTestFixture(t)
			guest := &closedInputExecGuest{testGuestClientStream: testGuestClientStream{ctx: context.Background()}, closed: make(chan struct{}), terminal: terminal}
			client := executionClientWithServer(t, controller, &agentServer{controller: controller, openGuestExec: func(context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error) {
				return guest, io.NopCloser(nilReader{}), nil
			}})
			stream, err := client.Exec(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: "fast-exec", ContainerId: "main", ExpectedInstanceId: "instance-a", ExecutionId: "exec:fast", ExecutionKind: hostv1.ExecutionKind_EXECUTION_KIND_EXEC, ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano()}}}); err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_CloseStdin{CloseStdin: true}}); err != nil {
				t.Fatal(err)
			}
			stdout, err := stream.Recv()
			if err != nil || string(stdout.GetStdout()) != "out" {
				t.Fatalf("stdout=%v,%v", stdout, err)
			}
			stderr, err := stream.Recv()
			if err != nil || string(stderr.GetStderr()) != "err" {
				t.Fatalf("stderr=%v,%v", stderr, err)
			}
			if terminal {
				exit, err := stream.Recv()
				if err != nil || exit.GetExitCode() != 0 {
					t.Fatalf("exit=%v,%v", exit, err)
				}
				if _, err := stream.Recv(); err != io.EOF {
					t.Fatalf("terminal exit overwritten by input error: %v", err)
				}
				if len(run.executions) != 0 {
					t.Fatalf("terminal exec still holds right: %+v", run.executions)
				}
			} else {
				if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
					t.Fatalf("missing exit became success: %v", err)
				}
				if run.executions["exec:fast"] == nil {
					t.Fatal("unknown exec lost its right")
				}
			}
		})
	}
}

type closedInputExecGuest struct {
	testGuestClientStream
	closed    chan struct{}
	terminal  bool
	responses int
}

func (s *closedInputExecGuest) Send(request *vmmdv1.ExecRequest) error {
	if request.GetCloseStdin() {
		close(s.closed)
		return status.Error(codes.Unavailable, "stdin closed after process completion")
	}
	return nil
}
func (s *closedInputExecGuest) Recv() (*vmmdv1.ExecResponse, error) {
	s.responses++
	switch s.responses {
	case 1:
		return &vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_Stdout{Stdout: []byte("out")}}, nil
	case 2:
		return &vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_Stderr{Stderr: []byte("err")}}, nil
	case 3:
		<-s.closed
		time.Sleep(10 * time.Millisecond)
		if s.terminal {
			return &vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_ExitCode{ExitCode: 0}}, nil
		}
	}
	return nil, io.EOF
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
			if err := controller.recoverWorkloadRunsFromGuest(ctx, workloadGuestClient(t, guest)); err != nil {
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

type revocationBuildGuestStream struct {
	testGuestClientStream
	accepted chan struct{}
}

func (stream *revocationBuildGuestStream) Send(*vmmdv1.BuildImageRequest) error {
	close(stream.accepted)
	return nil
}
func (stream *revocationBuildGuestStream) Recv() (*vmmdv1.BuildImageResponse, error) {
	<-stream.ctx.Done()
	return nil, stream.ctx.Err()
}

func TestBuildDisconnectPreservesWorkAndSessionRevokeBoundsStop(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller.store = database
	if _, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "env-a", NamespaceID: "a", OwnerKind: store.OwnerLocal, CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(context.Background(), store.Session{ID: "build-session", EnvironmentID: "env-a", NamespaceID: "a", SocketPath: "/unused", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	lifetime, cancelBuilds := context.WithCancel(context.Background())
	defer cancelBuilds()
	accepted := make(chan struct{})
	guestContext := make(chan context.Context, 1)
	listener := bufconn.Listen(1 << 20)
	identity := auth.Identity{Role: auth.Agent, SessionID: "build-session", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	statsHandler := &sessionStats{controller: controller, sessionID: identity.SessionID, connections: new(atomic.Uint32)}
	server := grpc.NewServer(grpc.WaitForHandlers(true), grpc.StreamInterceptor(auth.StreamInterceptor(identity)), grpc.StatsHandler(statsHandler))
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller, buildLifetime: lifetime, openGuestBuild: func(ctx context.Context) (vmmdv1.VMMDService_BuildImageClient, io.Closer, error) {
		guestContext <- ctx
		return &revocationBuildGuestStream{testGuestClientStream: testGuestClientStream{ctx: ctx}, accepted: accepted}, io.NopCloser(nilReader{}), nil
	}})
	controller.mu.Lock()
	controller.sessions = make(map[string]*sessionServer)
	controller.sessions[identity.SessionID] = &sessionServer{server: server, listener: listener, cancelBuilds: cancelBuilds, stopDone: make(chan struct{})}
	controller.mu.Unlock()
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///build-revoke", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	caller, cancelCaller := context.WithCancel(context.Background())
	stream, err := hostv1.NewAgentRuntimeServiceClient(connection).BuildImage(caller)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: &hostv1.BuildImageHeader{RequestId: "revoke-build", ContainerId: "main", ExpectedInstanceId: "instance-a", ExecutionId: "build:revoke", ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano()}}}); err != nil {
		t.Fatal(err)
	}
	<-accepted
	ctx := <-guestContext
	cancelCaller()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("caller disconnect=%v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("RPC cancellation canceled accepted build")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	waitUntil := time.Now().Add(time.Second)
	for {
		stored, err := database.GetSession(context.Background(), identity.SessionID)
		if err == nil && stored.ClosedAt != nil {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatal("connection close was not observed")
		}
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("connection close canceled accepted build")
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	if err := controller.stopSession(stopCtx, identity.SessionID); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("explicit session stop did not cancel build")
	}
	controller.mu.Lock()
	retained := run.executions["build:revoke"] != nil
	controller.mu.Unlock()
	if !retained {
		t.Fatal("cancellation was treated as terminal execution evidence")
	}
}
