//go:build darwin && e2e

package e2e

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"net"
	"testing"
	"time"
)

// Only this harness's temporary Host directory and newly allocated VM disk are targets.
func TestAllocationDisposalE2E(t *testing.T) {
	harness := startHostWithState(t, 16<<30)
	harness.start(t)
	t.Cleanup(func() { harness.stop(t, true) })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	type domain struct {
		id       string
		owner    *remoteE2EController
		endpoint string
		run      func() context.CancelFunc
		stop     context.CancelFunc
		runtimes map[string]hostv1.AgentRuntimeServiceClient
	}
	domains := make([]domain, 0, 2)
	for _, id := range []string{"precise-domain", "preserved-domain"} {
		controller := &remoteE2EController{registrationID: id, sessions: make(chan remoteE2ESession, 4), finish: make(chan struct{}), connections: make(chan int, 8), failures: make(chan string, 8), actions: make(chan remoteE2EAction)}
		endpoint, roots, stopServer := startRemoteController(t, controller)
		t.Cleanup(stopServer)
		pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(500, 1<<30, 256, 2<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(time.Second)}
		registration, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: id, RegistrationId: id, Endpoint: endpoint, DisplayName: id, ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := harness.client.CompleteRemoteEnrollment(ctx, &hostv1.CompleteRemoteEnrollmentRequest{RequestId: id + "-complete", RegistrationId: id, ExpectedRevision: registration.GetRegistration().GetRevision()}); err != nil {
			t.Fatal(err)
		}
		run := func() context.CancelFunc {
			connectorContext, stop := context.WithCancel(ctx)
			go func() {
				_ = remoteconnector.Run(connectorContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: id, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: &e2eCredentialStore{value: []byte("remote-e2e")}, TLSRoots: roots})
			}()
			return stop
		}
		current := domain{id: id, owner: controller, endpoint: endpoint, run: run, stop: run(), runtimes: make(map[string]hostv1.AgentRuntimeServiceClient)}
		t.Cleanup(current.stop)
		for len(current.runtimes) < 2 {
			select {
			case session := <-controller.sessions:
				connection, err := grpc.NewClient("passthrough:///precise-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return session.connection, nil }))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { connection.Close() })
				runtime := hostv1.NewAgentRuntimeServiceClient(connection)
				current.runtimes[session.allocationID] = runtime
				if _, err := runtime.CreateVolume(ctx, &hostv1.CreateVolumeRequest{RequestId: id + "-" + session.allocationID + "-data", VolumeId: "private"}); err != nil {
					t.Fatal(err)
				}
				writeDisposalSentinel(t, ctx, runtime, "private", id+"-"+session.allocationID)
				writeDisposalSentinel(t, ctx, runtime, "", id+"-workspace-"+session.allocationID)
			case failure := <-controller.failures:
				t.Fatal(failure)
			case <-ctx.Done():
				t.Fatal("runtime fixture timed out")
			}
		}
		<-controller.connections
		domains = append(domains, current)
	}
	target := &domains[0]
	registration, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: target.id})
	if err != nil {
		t.Fatal(err)
	}
	var allocation *hostv1.RemoteAllocation
	for _, item := range registration.GetRegistration().GetAllocations() {
		if item.GetAllocationId() == "remote-a" {
			allocation = item
		}
	}
	if allocation == nil {
		t.Fatal("exact target missing")
	}
	environment, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: allocation.GetEnvironmentId()})
	if err != nil {
		t.Fatal(err)
	}
	request := &remotev1.DisposeAllocation{AllocationId: "remote-a", ExpectedGeneration: allocation.GetGeneration(), ExpectedRevision: allocation.GetRevision(), RequestId: "precise-delete"}
	dispatch := func() *remotev1.CommandResult {
		reply := make(chan *remotev1.CommandResult, 1)
		command := &remotev1.ControllerCommand{CommandId: "precise-delete", Command: &remotev1.ControllerCommand_DisposeAllocation{DisposeAllocation: request}}
		select {
		case target.owner.actions <- remoteE2EAction{command: command, reply: reply}:
		case <-ctx.Done():
			t.Fatal("delete dispatch timed out")
		}
		select {
		case result := <-reply:
			return result
		case failure := <-target.owner.failures:
			t.Fatal(failure)
		case <-ctx.Done():
			t.Fatal("delete receipt timed out")
		}
		return nil
	}
	// The exact stale rejection returns a revision even for a target omitted by active inventory.
	request.ExpectedRevision++
	rejected := dispatch()
	if rejected.GetReason() != remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED || rejected.GetAllocation().GetRevision() != allocation.GetRevision() {
		t.Fatalf("stale exact receipt=%v", rejected)
	}
	request.ExpectedRevision = allocation.GetRevision()
	// Accept on the current management connection, then lose both connector and Host.
	pending := dispatch()
	if pending.GetReason() != remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE {
		t.Fatalf("accepted cleanup receipt=%v", pending)
	}
	for index := range domains {
		domains[index].stop()
	}
	harness.crashAndRestartHost(t)
	target.stop = target.run()
	t.Cleanup(target.stop)
	select {
	case <-target.owner.connections:
	case failure := <-target.owner.failures:
		t.Fatal(failure)
	case <-ctx.Done():
		t.Fatal("reconnect timed out")
	}
	for {
		result := dispatch()
		if result.GetOutcome() == remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			if result.GetAllocation().GetState() != hostv1.AllocationState_ALLOCATION_STATE_DISCARDED {
				t.Fatalf("false completion=%v", result)
			}
			break
		}
		if result.GetReason() != remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE {
			t.Fatalf("cleanup rejected=%v", result)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("persistent cleanup did not converge")
		}
	}
	if _, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: environment.GetEnvironment().GetId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted target readable: %v", err)
	}
	// Check actual bytes in both neighbors, not just their resource-row survival.
	for index := range domains {
		current := &domains[index]
		if index == 1 {
			current.stop = current.run()
			t.Cleanup(current.stop)
		}
		count := 2
		if index == 0 {
			count = 1
		}
		for range count {
			var session remoteE2ESession
			select {
			case session = <-current.owner.sessions:
			case failure := <-current.owner.failures:
				t.Fatal(failure)
			case <-ctx.Done():
				t.Fatal("neighbor session timed out")
			}
			connection, err := grpc.NewClient("passthrough:///preserved-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return session.connection, nil }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { connection.Close() })
			runtime := hostv1.NewAgentRuntimeServiceClient(connection)
			readDisposalSentinel(t, ctx, runtime, "private", current.id+"-"+session.allocationID)
			readDisposalSentinel(t, ctx, runtime, "", current.id+"-workspace-"+session.allocationID)
		}
	}
	replay := dispatch()
	if replay.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		t.Fatalf("receipt replay=%v", replay)
	}
}
