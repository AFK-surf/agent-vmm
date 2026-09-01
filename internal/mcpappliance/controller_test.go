package mcpappliance

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"github.com/AFK-surf/agent-vmm/internal/mcpserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type applianceControlStream struct {
	ctx       context.Context
	received  chan *remotev1.HostControlMessage
	onCommand func(*remotev1.ControllerCommand)
	mu        sync.Mutex
	commands  []*remotev1.ControllerCommand
	next      uint64
}

func (stream *applianceControlStream) Send(message *remotev1.ControllerControlMessage) error {
	command := message.GetCommand()
	if command == nil {
		return nil
	}
	stream.mu.Lock()
	stream.commands = append(stream.commands, command)
	if stream.next == 0 {
		stream.next = 1
	}
	admitted := command.GetSequence() == stream.next && validFrontendGeneration(command)
	if admitted {
		stream.next++
	}
	stream.mu.Unlock()
	if stream.onCommand != nil {
		stream.onCommand(command)
	}
	disposition := remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED
	if !admitted {
		disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED
	}
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Ack{Ack: &remotev1.CommandAck{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Disposition: disposition}}}
	if !admitted {
		return nil
	}
	result := &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: command.GetSequence() + 1}
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_EnsureAllocation:
		result.Result = &remotev1.CommandResult_Allocation{Allocation: &remotev1.Allocation{AllocationId: value.EnsureAllocation.GetAllocationId(), Generation: value.EnsureAllocation.GetGeneration(), ResourcesV2: value.EnsureAllocation.GetResourcesV2(), Revision: 10}}
	case *remotev1.ControllerCommand_AcquireLease:
		result.Result = &remotev1.CommandResult_Lease{Lease: &remotev1.Lease{AllocationId: value.AcquireLease.GetAllocationId(), LeaseId: "lease-" + value.AcquireLease.GetAllocationId(), Generation: 1, AllocationRevision: 11, ExpiresUnixMillis: time.Now().Add(5 * time.Minute).UnixMilli()}}
	case *remotev1.ControllerCommand_RenewLease:
		result.Result = &remotev1.CommandResult_Lease{Lease: &remotev1.Lease{LeaseId: value.RenewLease.GetLeaseId(), Generation: value.RenewLease.GetGeneration(), ExpiresUnixMillis: time.Now().Add(5 * time.Minute).UnixMilli()}}
	case *remotev1.ControllerCommand_ReleaseLease:
		result.Result = &remotev1.CommandResult_Lease{Lease: &remotev1.Lease{LeaseId: value.ReleaseLease.GetLeaseId(), Generation: value.ReleaseLease.GetGeneration(), AllocationRevision: 12}}
	case *remotev1.ControllerCommand_PlanCleanupAllocation:
		result.Result = &remotev1.CommandResult_CleanupPlan{CleanupPlan: &remotev1.CleanupPlan{AllocationId: value.PlanCleanupAllocation.GetAllocationId(), Revision: 19, Kind: value.PlanCleanupAllocation.GetKind(), VolumeIds: []string{"volume-b", "volume-a"}, Impacts: []*hostv1.CleanupImpact{{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "volume-b"}, Effect: "delete", Irreversible: true}, {Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "volume-a"}, Effect: "delete", Irreversible: true}, {Resource: &hostv1.ResourceReference{Kind: "container", ResourceId: "service"}, Effect: "delete", Irreversible: true}}}}
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		state := hostv1.AllocationState_ALLOCATION_STATE_RETAINED
		if value.ExecuteCleanupAllocation.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
			state = hostv1.AllocationState_ALLOCATION_STATE_DISCARDED
		}
		result.Result = &remotev1.CommandResult_Allocation{Allocation: &remotev1.Allocation{AllocationId: value.ExecuteCleanupAllocation.GetAllocationId(), State: state}}
	case *remotev1.ControllerCommand_AcquireHostLoopbackFrontend:
		expires := min(time.Duration(value.AcquireHostLoopbackFrontend.GetDurationSeconds())*time.Second, 2*time.Second)
		result.Result = &remotev1.CommandResult_HostLoopbackFrontendLease{HostLoopbackFrontendLease: &hostv1.HostLoopbackFrontendLease{FrontendLeaseId: "frontend-" + value.AcquireHostLoopbackFrontend.GetAssignmentId(), Generation: 1, RegistrationId: "registration", ConnectionInstance: "connection", ConnectionEpoch: 7, AllocationId: value.AcquireHostLoopbackFrontend.GetAllocationId(), AllocationLeaseId: value.AcquireHostLoopbackFrontend.GetAllocationLeaseId(), AllocationLeaseGeneration: value.AcquireHostLoopbackFrontend.GetAllocationLeaseGeneration(), AllocationRevision: command.GetTargetRevision(), AssignmentId: value.AcquireHostLoopbackFrontend.GetAssignmentId(), Endpoint: "127.0.0.1:4567", AccessToken: make([]byte, 32), ExpiresAt: timestamppb.New(time.Now().Add(expires)), EffectiveCapacity: applianceHello().GetHostLoopbackFrontend()}}
	case *remotev1.ControllerCommand_RenewHostLoopbackFrontend:
		result.Result = &remotev1.CommandResult_HostLoopbackFrontendLease{HostLoopbackFrontendLease: &hostv1.HostLoopbackFrontendLease{FrontendLeaseId: value.RenewHostLoopbackFrontend.GetFrontendLeaseId(), Generation: value.RenewHostLoopbackFrontend.GetGeneration(), RegistrationId: "registration", ConnectionInstance: "connection", ConnectionEpoch: 7, AllocationId: "allocation", AllocationLeaseId: value.RenewHostLoopbackFrontend.GetAllocationLeaseId(), AllocationLeaseGeneration: value.RenewHostLoopbackFrontend.GetAllocationLeaseGeneration(), AllocationRevision: 11, AssignmentId: "assignment", Endpoint: "127.0.0.1:4567", AccessToken: make([]byte, 32), ExpiresAt: timestamppb.New(time.Now().Add(time.Duration(value.RenewHostLoopbackFrontend.GetDurationSeconds()) * time.Second)), EffectiveCapacity: applianceHello().GetHostLoopbackFrontend()}}
	}
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Result{Result: result}}
	return nil
}

func validFrontendGeneration(command *remotev1.ControllerCommand) bool {
	if request := command.GetRenewHostLoopbackFrontend(); request != nil {
		return command.GetLeaseGeneration() == request.GetGeneration()
	}
	if request := command.GetReleaseHostLoopbackFrontend(); request != nil {
		return command.GetLeaseGeneration() == request.GetGeneration()
	}
	return true
}

func (stream *applianceControlStream) Recv() (*remotev1.HostControlMessage, error) {
	select {
	case message := <-stream.received:
		return message, nil
	case <-stream.ctx.Done():
		return nil, io.EOF
	}
}

func (stream *applianceControlStream) Context() context.Context { return stream.ctx }

func TestConcurrentClientsReceiveIndependentAllocations(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &applianceControlStream{ctx: ctx, received: make(chan *remotev1.HostControlMessage, 16)}
	hello := applianceHello()
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}}
	session, err := controllerremote.AcceptControl(stream, "registration")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	controller := NewController(&Identity{}, t.TempDir(), nil)
	liveCtx, stopLive := context.WithCancel(ctx)
	live := &controlConnection{session: session, requests: make(chan commandRequest), done: make(chan struct{}), hello: hello, close: stopLive}
	controller.install(live)
	go func() {
		_ = controller.runControl(liveCtx, live)
		controller.uninstall(live)
	}()

	type allocated struct {
		allocation string
		lease      *remotev1.Lease
		err        error
	}
	results := make(chan allocated, 2)
	for _, clientID := range []string{"client-a", "client-b"} {
		go func() {
			allocation := "allocation-" + clientID
			lease, _, err := controller.allocate(ctx, clientID, allocation)
			results <- allocated{allocation: allocation, lease: lease, err: err}
		}()
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("allocate errors: %v, %v", first.err, second.err)
	}
	if first.allocation == second.allocation || first.lease.GetAllocationId() == second.lease.GetAllocationId() {
		t.Fatalf("clients shared allocation: %+v %+v", first, second)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	seen := make(map[string]bool)
	for _, command := range stream.commands {
		if acquire := command.GetAcquireLease(); acquire != nil {
			if command.GetTargetRevision() != 10 {
				t.Fatalf("acquire target revision = %d", command.GetTargetRevision())
			}
			seen[acquire.GetAllocationId()] = true
		}
	}
	if !seen["allocation-client-a"] || !seen["allocation-client-b"] {
		t.Fatalf("acquired allocations = %v", seen)
	}
}

func TestAllocationRejectsIncompleteCapacity(t *testing.T) {
	t.Parallel()
	controller := NewController(&Identity{}, t.TempDir(), nil)
	controller.live = &controlConnection{hello: &remotev1.HostHello{Capacity: &remotev1.Capacity{}}, done: make(chan struct{})}
	if _, _, err := controller.allocate(context.Background(), "client", "allocation"); err == nil {
		t.Fatal("allocate accepted incomplete capacity")
	}
}

func TestAllocationLeaseRenewsBeforeExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &applianceControlStream{ctx: ctx, received: make(chan *remotev1.HostControlMessage, 16)}
	hello := applianceHello()
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}}
	session, err := controllerremote.AcceptControl(stream, "registration")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	controller := NewController(&Identity{}, t.TempDir(), nil)
	liveCtx, stopLive := context.WithCancel(ctx)
	live := &controlConnection{session: session, requests: make(chan commandRequest), done: make(chan struct{}), hello: hello, close: stopLive}
	controller.install(live)
	go func() { _ = controller.runControl(liveCtx, live) }()
	leaseContext, stopLease := context.WithCancel(ctx)
	lease := &remotev1.Lease{AllocationId: "allocation", LeaseId: "lease", Generation: 2, AllocationRevision: 11, ExpiresUnixMillis: time.Now().Add(3 * time.Second).UnixMilli()}
	go controller.renewAllocationLease(leaseContext, live, "client", lease, stopLease)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stream.mu.Lock()
		var renewed *remotev1.RenewLease
		for _, command := range stream.commands {
			if command.GetRenewLease() != nil {
				renewed = command.GetRenewLease()
			}
		}
		stream.mu.Unlock()
		if renewed != nil {
			if renewed.GetLeaseId() != "lease" || renewed.GetGeneration() != 2 || renewed.GetDurationSeconds() != 300 {
				t.Fatalf("renew request = %#v", renewed)
			}
			stopLease()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("allocation lease was not renewed")
}

func TestCleanupOwnerUsesExactAllocationLeaseFence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &applianceControlStream{ctx: ctx, received: make(chan *remotev1.HostControlMessage, 16)}
	hello := applianceHello()
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}}
	session, err := controllerremote.AcceptControl(stream, "registration")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	controller := NewController(&Identity{}, t.TempDir(), nil)
	liveCtx, stopLive := context.WithCancel(ctx)
	live := &controlConnection{session: session, requests: make(chan commandRequest), done: make(chan struct{}), hello: hello, close: stopLive}
	controller.install(live)
	go func() { _ = controller.runControl(liveCtx, live) }()
	owner := &clientCleanupOwner{controller: controller, live: live, clientID: "client", allocationID: "allocation", lease: &remotev1.Lease{AllocationId: "allocation", LeaseId: "allocation-lease", Generation: 2, AllocationRevision: 11}}
	plan, err := owner.PlanCleanup(ctx, "discard")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != "discard" || plan.Revision != 19 || len(plan.Impacts) != 3 || !slices.Equal(plan.VolumeIDs, []string{"volume-a", "volume-b"}) || plan.Impacts[0].Resource.ID != "volume-b" {
		t.Fatalf("cleanup plan = %#v", plan)
	}
	if err := owner.ExecuteCleanup(ctx, mcpserver.CleanupExecution{Kind: "discard", Revision: plan.Revision, ConfirmedVolumeIDs: []string{"volume-b", "volume-a"}}); err != nil {
		t.Fatal(err)
	}
	if !owner.completedSuccessfully() {
		t.Fatal("successful cleanup did not consume the allocation lease")
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	var execute *remotev1.ExecuteCleanupAllocation
	var targetRevision uint64
	for _, command := range stream.commands {
		if command.GetExecuteCleanupAllocation() != nil {
			execute = command.GetExecuteCleanupAllocation()
			targetRevision = command.GetTargetRevision()
		}
	}
	if execute == nil || execute.GetAllocationId() != "allocation" || execute.GetExpectedLeaseId() != "allocation-lease" || execute.GetExpectedLeaseGeneration() != 2 || execute.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_DISCARD || execute.GetPlanRevision() != 19 || targetRevision != 19 || !slices.Equal(execute.GetConfirmedVolumeIds(), []string{"volume-a", "volume-b"}) {
		t.Fatalf("cleanup execute = %#v target=%d", execute, targetRevision)
	}
}

func TestServeSessionRoutesOnlyTheExactLease(t *testing.T) {
	t.Parallel()
	controller := NewController(&Identity{}, t.TempDir(), nil)
	controller.live = &controlConnection{hello: applianceHello(), done: make(chan struct{})}
	key := sessionKey{allocationID: "allocation", leaseID: "lease", generation: 2}
	waiter := make(chan controllerremote.SessionConnection, 1)
	controller.waiters[key] = waiter
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	remotev1.RegisterRemoteControllerServiceServer(server, controller)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := remotev1.NewRemoteControllerServiceClient(connection).ServeSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Header{Header: &remotev1.SessionHeader{RegistrationId: "registration", ConnectionEpoch: 7, AllocationId: "allocation", LeaseId: "lease", Generation: 2}}}); err != nil {
		t.Fatal(err)
	}
	var runtimeConnection controllerremote.SessionConnection
	select {
	case runtimeConnection = <-waiter:
	case <-ctx.Done():
		t.Fatal("session was not routed")
	}
	if _, err := runtimeConnection.Write([]byte("controller-to-host")); err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Recv()
	if err != nil || string(frame.GetData()) != "controller-to-host" {
		t.Fatalf("controller frame = %q, %v", frame.GetData(), err)
	}
	if err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Data{Data: []byte("host-to-controller")}}); err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 32)
	count, err := runtimeConnection.Read(value)
	if err != nil || string(value[:count]) != "host-to-controller" {
		t.Fatalf("runtime read = %q, %v", value[:count], err)
	}
	runtimeConnection.Close()
}

func applianceHello() *remotev1.HostHello {
	features := append([]string(nil), controllerremote.RequiredFeatures...)
	return &remotev1.HostHello{ProtocolVersion: controllerremote.ProtocolVersion, HostApiVersion: "host.v1", RegistrationId: "registration", ConnectionInstance: "connection", ConnectionEpoch: 7, InventoryWatermark: 1, PoolRevision: 3, SupportedFeatures: features, RequiredFeatures: features, Capacity: &remotev1.Capacity{MaxEnvironments: 8, PerEnvironmentLimits: &hostv1.ResourceLimits{CpuMillis: uint32Pointer(500), MemoryBytes: uint64Pointer(1 << 30), Pids: uint32Pointer(256), DiskBytes: uint64Pointer(2 << 30)}, MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, MaxLeaseSeconds: uint32((5 * time.Minute) / time.Second)}, HostLoopbackFrontend: &hostv1.HostLoopbackFrontendCapacity{MaxActiveLeases: 16, MaxActiveLeasesPerAllocation: 16, MaxLeaseSeconds: 60, MaxConcurrentConnections: 64, MaxConcurrentConnectionsPerLease: 16, MaxFrameBytes: 64 << 10, MaxConnectionBytes: 1 << 30, ConnectionDeadlineSeconds: 3600, ConnectionIdleSeconds: 300, MaxUseCount: 1024}}
}

func uint32Pointer(value uint32) *uint32 { return &value }
func uint64Pointer(value uint64) *uint64 { return &value }
