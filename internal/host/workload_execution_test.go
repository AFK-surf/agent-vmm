//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

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
			controller, _ := executionTestFixture(t)
			guest := &detachedExecGuest{accepted: make(chan struct{}), released: make(chan struct{}), closed: make(chan struct{}), terminal: mode != "unknown"}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(guest.released) }) })
			identity := auth.Identity{Role: auth.Agent, EnvironmentID: "env-a", NamespaceID: "env-a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
			config := testSessionConfig(t, controller)
			config.Runtime = sessionRuntimeFunc(func(lifetime context.Context) hostv1.AgentRuntimeServiceServer {
				return &agentServer{controller: controller, buildLifetime: lifetime, openGuestExec: func(ctx context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error) {
					guest.ctx = ctx
					return guest, guest, nil
				}}
			})
			controller.sessions = session.New(config)
			t.Cleanup(controller.sessions.Close)
			record := createTestSession(t, controller, "env-a", "", 0, identity)
			identity.SessionID = record.ID
			_, connection := dialTestSession(t, record)
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
			if err := controller.sessions.Revoke(stopCtx, identity.SessionID); err != nil {
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
				stopped := &workloadGuest{omitStoppedInstance: true, states: map[string]*vmmdv1.Container{"env-a": {Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}}
				guestClient := workloadGuestClient(t, stopped)
				unlock, lockErr := controller.workloads.LockEnvironment(context.Background(), "env-a")
				if lockErr != nil {
					t.Fatal(lockErr)
				}

				request := &hostv1.StopContainerRequest{RequestId: "replacement-stop", ContainerId: "main", ExpectedInstanceId: "instance-a"}
				if err := controller.workloads.ValidateExplicitStop(replacement, request); err != nil {
					t.Fatal(err)
				}
				_, err := controller.workloads.StopWorkloadContainerLocked(context.Background(), guestClient, "env-a", &vmmdv1.StopContainerRequest{RequestId: request.RequestId, ActorNamespaceId: "env-a", ContainerId: "main"})
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
			retained := testExecutionPresent(t, controller, "exec:callback")
			if mode == "exact-stop" {
				assertListStatus(t, client, context.Background(), listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)
			} else if retained != (mode == "unknown") {
				t.Fatalf("terminal=%s retained=%v", mode, retained)
			}
		})
	}
}

func TestExecAmbiguousFirstGuestSendKeepsExecutionRight(t *testing.T) {
	controller, _ := executionTestFixture(t)
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
	if !guest.accepted || !testExecutionPresent(t, controller, "exec:ambiguous") {
		t.Fatalf("accepted exec lost its right: accepted=%v executions=%+v", guest.accepted, testExecutionSnapshot(t, controller))
	}
	if _, err := client.QuiesceContainer(context.Background(), &hostv1.QuiesceContainerRequest{RequestId: "quiet", ContainerId: "main", ExpectedInstanceId: "instance-a"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce crossed ambiguous exec: %v", err)
	}
}

func TestBuildAmbiguousFirstGuestSendKeepsExecutionRight(t *testing.T) {
	controller, _ := executionTestFixture(t)
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
	if !guest.accepted || !testExecutionPresent(t, controller, "build:ambiguous") {
		t.Fatalf("accepted build lost its right: accepted=%v executions=%+v", guest.accepted, testExecutionSnapshot(t, controller))
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
	controller, client := executionTestFixture(t)
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	if _, err := client.AcquireExecution(context.Background(), mainExecution("main:active")); err != nil {
		t.Fatal(err)
	}
	guest := &workloadGuest{omitStoppedInstance: true, states: map[string]*vmmdv1.Container{"a": {Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}}
	guestClient := workloadGuestClient(t, guest)
	request := &hostv1.StopContainerRequest{RequestId: "stop-exact", ContainerId: "main", ExpectedInstanceId: "old-instance"}
	unlock, lockErr := controller.workloads.LockEnvironment(context.Background(), "env-a")
	if lockErr != nil {
		t.Fatal(lockErr)
	}

	if err := controller.workloads.ValidateExplicitStop(identity, request); status.Code(err) != codes.Aborted {
		t.Fatalf("stale stop = %v", err)
	}
	if guest.stopHits != 0 || testWorkloadInstance(controller, "env-a", "main").Stopping || len(testExecutionSnapshot(t, controller)) != 1 {
		t.Fatal("stale instance stop changed current execution")
	}
	request.ExpectedInstanceId = "instance-a"
	request.QuiesceRequestId = "unowned-quiesce"
	if err := controller.workloads.ValidateExplicitStop(identity, request); status.Code(err) != codes.Aborted {
		t.Fatalf("unowned quiet stop = %v", err)
	}
	request.QuiesceRequestId = ""
	if err := controller.workloads.ValidateExplicitStop(identity, request); err != nil {
		t.Fatal(err)
	}
	response, err := controller.workloads.StopWorkloadContainerLocked(context.Background(), guestClient, "env-a", &vmmdv1.StopContainerRequest{RequestId: request.RequestId, ActorNamespaceId: "a", ContainerId: "main"})
	unlock()
	if err != nil || response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED || response.GetContainer().GetInstanceId() != "" || guest.stopHits != 1 {
		t.Fatalf("exact active stop = %+v, %v", response, err)
	}
	assertListStatus(t, client, context.Background(), listExecutions(), hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED)
}

func TestExecTerminalExitPreservesOutputDespiteClosedInput(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal=%v", terminal), func(t *testing.T) {
			controller, _ := executionTestFixture(t)
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
				if len(testExecutionSnapshot(t, controller)) != 0 {
					t.Fatalf("terminal exec still holds right: %+v", testExecutionSnapshot(t, controller))
				}
			} else {
				if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
					t.Fatalf("missing exit became success: %v", err)
				}
				if !testExecutionPresent(t, controller, "exec:fast") {
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
	controller, _ := executionTestFixture(t)
	database := controller.store
	accepted := make(chan struct{})
	guestContext := make(chan context.Context, 1)
	identity := auth.Identity{Role: auth.Agent, EnvironmentID: "env-a", NamespaceID: "env-a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	config := testSessionConfig(t, controller)
	config.Runtime = sessionRuntimeFunc(func(lifetime context.Context) hostv1.AgentRuntimeServiceServer {
		return &agentServer{controller: controller, buildLifetime: lifetime, openGuestBuild: func(ctx context.Context) (vmmdv1.VMMDService_BuildImageClient, io.Closer, error) {
			guestContext <- ctx
			return &revocationBuildGuestStream{testGuestClientStream: testGuestClientStream{ctx: ctx}, accepted: accepted}, io.NopCloser(nilReader{}), nil
		}}
	})
	controller.sessions = session.New(config)
	t.Cleanup(controller.sessions.Close)
	record := createTestSession(t, controller, "env-a", "", 0, identity)
	identity.SessionID = record.ID
	_, connection := dialTestSession(t, record)
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
	if err := controller.sessions.Revoke(stopCtx, identity.SessionID); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("explicit session stop did not cancel build")
	}
	retained := testExecutionPresent(t, controller, "build:revoke")
	if !retained {
		t.Fatal("cancellation was treated as terminal execution evidence")
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

func executionTestFixture(t *testing.T) (*Controller, hostv1.AgentRuntimeServiceClient) {
	t.Helper()
	controller := workloadTestController(t)
	setTestWorkloadInstance(t, controller, "env-a", "main", "instance-a")
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "env-a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	return controller, executionClient(t, controller, identity)
}

func releaseExecutionRequest(request *hostv1.AcquireExecutionRequest) *hostv1.ReleaseExecutionRequest {
	return &hostv1.ReleaseExecutionRequest{ContainerId: request.GetContainerId(), ExpectedInstanceId: request.GetExpectedInstanceId(), ExecutionId: request.GetExecutionId(), Kind: request.GetKind()}
}
