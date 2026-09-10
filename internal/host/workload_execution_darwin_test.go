//go:build darwin

package host

import (
	"context"
	"net"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestExecutionOwnershipFencesQuiesceAndLateCompletion(t *testing.T) {
	controller := workloadTestController()
	run, err := controller.admitWorkloadStart("env-a", "a", "main", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	run.inFlight = false
	controller.observeWorkloadRunLocked(run, &vmmdv1.Container{Id: "main", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}, time.Now())
	controller.mu.Unlock()
	identity := auth.Identity{Role: auth.Agent, SessionID: "session-a", EnvironmentID: "env-a", NamespaceID: "a", LeaseID: "lease-a"}
	controller.remoteLeases = map[string]*remoteLease{"lease-a": {id: "lease-a", environmentID: "env-a", state: hostv1.LeaseState_LEASE_STATE_ACTIVE, expiresAt: time.Now().Add(time.Minute)}}
	client := executionClient(t, controller, identity)
	ctx := context.Background()
	execution := &hostv1.ExecutionRequest{ExecutionId: "input-a", ContainerId: "main", ExpectedInstanceId: "instance-a", DeadlineUnixNano: time.Now().Add(time.Minute).UnixNano()}
	if _, err := client.AcquireExecution(ctx, execution); err != nil {
		t.Fatal(err)
	}
	firstDeadline := run.execution.deadline
	execution.DeadlineUnixNano = time.Now().Add(time.Hour).UnixNano()
	if _, err := client.AcquireExecution(ctx, execution); err != nil {
		t.Fatal(err)
	}
	if !run.execution.deadline.Equal(firstDeadline) {
		t.Fatal("idempotent acquire extended execution deadline")
	}
	quiesce := &hostv1.QuiesceContainerRequest{RequestId: "sleep-a", ContainerId: "main", ExpectedInstanceId: "instance-a"}
	if _, err := client.QuiesceContainer(ctx, quiesce); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce killed active execution: %v", err)
	}
	if _, err := client.ReleaseExecution(ctx, execution); err != nil {
		t.Fatal(err)
	}
	quiesce.MinimumIdleSeconds = 60
	if _, err := client.QuiesceContainer(ctx, quiesce); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quiesce ignored minimum idle period: %v", err)
	}
	cancelBeforeQuiesce := &hostv1.QuiesceContainerRequest{
		RequestId:          quiesce.RequestId,
		ContainerId:        quiesce.ContainerId,
		ExpectedInstanceId: quiesce.ExpectedInstanceId,
		MinimumIdleSeconds: quiesce.MinimumIdleSeconds,
		Cancel:             true,
	}
	if _, err := client.QuiesceContainer(ctx, cancelBeforeQuiesce); err != nil {
		t.Fatalf("idempotent cancel without a current quiesce failed: %v", err)
	}
	controller.mu.Lock()
	run.idleSince = time.Now().Add(-time.Minute)
	controller.mu.Unlock()
	if _, err := client.QuiesceContainer(ctx, quiesce); err != nil {
		t.Fatal(err)
	}
	next := &hostv1.ExecutionRequest{ExecutionId: "input-b", ContainerId: "main", ExpectedInstanceId: "instance-a"}
	if _, err := client.AcquireExecution(ctx, next); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("execution entered closing sandbox: %v", err)
	}
	staleCancel := &hostv1.QuiesceContainerRequest{
		RequestId:          "sleep-old",
		ContainerId:        quiesce.ContainerId,
		ExpectedInstanceId: quiesce.ExpectedInstanceId,
		MinimumIdleSeconds: quiesce.MinimumIdleSeconds,
		Cancel:             true,
	}
	if _, err := client.QuiesceContainer(ctx, staleCancel); status.Code(err) != codes.Aborted {
		t.Fatalf("different quiesce cancellation replaced current ownership: %v", err)
	}
	if err := controller.validateQuiescedStop(identity, &hostv1.StopContainerRequest{ContainerId: "main", ExpectedInstanceId: "instance-a", QuiesceRequestId: "sleep-old"}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale stop accepted: %v", err)
	}
	quiesce.Cancel = true
	if _, err := client.QuiesceContainer(ctx, quiesce); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireExecution(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReleaseExecution(ctx, execution); status.Code(err) != codes.Aborted {
		t.Fatalf("old completion cleared newer execution: %v", err)
	}
	controller.mu.Lock()
	if run.execution == nil || run.execution.id != "input-b" {
		t.Fatal("new execution was lost")
	}
	controller.remoteLeases["lease-a"].state = hostv1.LeaseState_LEASE_STATE_REVOKED
	controller.mu.Unlock()
	if _, err := client.ReleaseExecution(ctx, next); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revoked lease released ownership: %v", err)
	}
}

func executionClient(t *testing.T, controller *Controller, identity auth.Identity) hostv1.AgentRuntimeServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(identity)))
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{controller: controller})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///host", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection)
}
