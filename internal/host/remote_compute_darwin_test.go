//go:build darwin

package host

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestAllocationAdmission(t *testing.T) {
	registration := store.RemoteRegistration{
		ID: "registration", MaxEnvironments: 2,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 1 << 30, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL),
	}
	limits := func(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
	}
	existing := []store.Environment{{CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30}}
	tests := []struct {
		name         string
		environments []store.Environment
		limits       *hostv1.ResourceLimits
		egress       hostv1.EgressMode
		code         codes.Code
	}{
		{name: "accepted", environments: nil, limits: limits(1000, 1<<30, 128, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.OK},
		{name: "count", environments: append(existing, existing...), limits: limits(1, 16<<20, 1, 1), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "per cpu", limits: limits(1001, 1<<30, 128, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "per memory", limits: limits(1000, (1<<30)+1, 128, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "per pids", limits: limits(1000, 1<<30, 129, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "per disk", limits: limits(1000, 1<<30, 128, (4<<30)+1), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "aggregate", environments: []store.Environment{{CPUMillis: 1500, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30}}, limits: limits(1000, 1<<30, 128, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, code: codes.ResourceExhausted},
		{name: "egress", limits: limits(1000, 1<<30, 128, 4<<30), egress: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, code: codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := admitAllocation(registration, test.environments, test.limits, test.egress)
			if status.Code(err) != test.code {
				t.Fatalf("code=%v want=%v err=%v", status.Code(err), test.code, err)
			}
		})
	}
}

func TestRemoteRuntimeMethodFilter(t *testing.T) {
	for _, method := range []string{
		hostv1.AgentRuntimeService_PlanResetEnvironment_FullMethodName,
		hostv1.AgentRuntimeService_PlanDiscardEnvironment_FullMethodName,
		hostv1.AgentRuntimeService_RequestReset_FullMethodName,
		hostv1.AgentRuntimeService_RequestDiscard_FullMethodName,
	} {
		if !remoteRuntimeDenied(method) {
			t.Fatalf("remote lifecycle method was allowed: %s", method)
		}
	}
	for _, method := range []string{
		hostv1.AgentRuntimeService_GetEnvironment_FullMethodName,
		hostv1.AgentRuntimeService_BuildImage_FullMethodName,
		hostv1.AgentRuntimeService_ImportImage_FullMethodName,
		hostv1.AgentRuntimeService_ForwardTCP_FullMethodName,
		hostv1.AgentRuntimeService_DeleteImage_FullMethodName,
	} {
		if remoteRuntimeDenied(method) {
			t.Fatalf("ordinary runtime method was denied: %s", method)
		}
	}
	for _, method := range []string{
		hostv1.AgentRuntimeService_CreateServiceExport_FullMethodName,
		hostv1.AgentRuntimeService_ListServiceExports_FullMethodName,
		hostv1.AgentRuntimeService_GetServiceExport_FullMethodName,
		hostv1.AgentRuntimeService_RevokeServiceExport_FullMethodName,
		hostv1.AgentRuntimeService_CreateServiceImport_FullMethodName,
		hostv1.AgentRuntimeService_CreatePersonalServiceImport_FullMethodName,
		hostv1.AgentRuntimeService_ListServiceImports_FullMethodName,
		hostv1.AgentRuntimeService_GetServiceImport_FullMethodName,
		hostv1.AgentRuntimeService_RevokeServiceImport_FullMethodName,
	} {
		if remoteRuntimeDenied(method) {
			t.Fatalf("service method was denied: %s", method)
		}
	}
	if !remoteRuntimeDenied("/agentvmm.host.v1.AgentRuntimeService/FutureOperatorMethod") {
		t.Fatal("unknown future runtime method was allowed")
	}
}

func TestSessionProjectionHidesHostIdentity(t *testing.T) {
	value := sessionEnvironmentFromStore(store.Environment{ID: "host-id", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation"})
	if value.GetId() != "" || value.GetNamespaceId() != "" || value.GetOwner() != nil || value.GetOrigin() != hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE {
		t.Fatalf("unsafe session projection: %+v", value)
	}
}

func TestRemoteLeaseLifecycleAndInventoryScope(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration := store.RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", ProtocolVersion: "1", MaxEnvironments: 2,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 1 << 30, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
		EnrollmentState: 2, ConnectorPhase: 1,
	}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "local", NamespaceID: "local-namespace", DisplayName: "local", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	remote, err := database.CreateEnvironment(ctx, store.Environment{ID: "remote", NamespaceID: "remote-namespace", DisplayName: "remote", CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationState: 1})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string)}
	listener := bufconn.Listen(1 << 20)
	grpcServer := controller.NewRemoteComputeServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); listener.Close() })
	connection, err := grpc.NewClient("passthrough:///remote", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		return invoke(metadata.AppendToOutgoingContext(ctx, "x-agent-vmm-registration", registration.ID), method, request, reply, connection, options...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := hostv1.NewRemoteComputeServiceClient(connection)
	allocationRevision := func() uint64 {
		environment, err := database.GetEnvironment(ctx, remote.ID)
		if err != nil {
			t.Fatalf("get allocation revision: %v", err)
		}
		return environment.PolicyRevision
	}
	if _, err := client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: "other"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-registration transport identity error=%v", err)
	}
	state, err := client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: registration.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.GetRegistration().GetAllocations()) != 1 || state.GetRegistration().GetAllocations()[0].GetAllocationId() != "allocation" {
		t.Fatalf("remote inventory leaked or omitted environments: %+v", state)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", ConnectionEpoch: 1, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-b", ConnectionEpoch: 2, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); status.Code(err) != codes.Aborted {
		t.Fatalf("replaced connection admitted a lease: %v", err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", ConnectionEpoch: 3, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", ConnectionEpoch: 1, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale same-instance disconnect was accepted: %v", err)
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision() + 1}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale allocation revision admitted a lease: %v", err)
	}
	first, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), WorkloadRef: "work", ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()})
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	controller.remoteLeases[first.GetLease().GetLeaseId()].sessionID = "replacement-session"
	controller.mu.Unlock()
	if _, err := client.RevokeRuntimeSession(ctx, &hostv1.RevokeRuntimeSessionRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: first.GetLease().GetLeaseId(), Generation: first.GetLease().GetGeneration(), SessionId: "stale-session"}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale runtime session revoke error=%v", err)
	}
	controller.mu.Lock()
	if sessionID := controller.remoteLeases[first.GetLease().GetLeaseId()].sessionID; sessionID != "replacement-session" {
		controller.mu.Unlock()
		t.Fatalf("stale revoke cleared replacement session %q", sessionID)
	}
	controller.remoteLeases[first.GetLease().GetLeaseId()].sessionID = ""
	controller.mu.Unlock()
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING,
	}); err != nil {
		t.Fatal(err)
	}
	first, err = client.RenewLease(ctx, &hostv1.RenewLeaseRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a",
		LeaseId:            first.GetLease().GetLeaseId(), Generation: first.GetLease().GetGeneration(), Duration: durationpb.New(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("drain interrupted an existing lease: %v", err)
	}
	if _, err := client.EnsureAllocation(ctx, &hostv1.EnsureAllocationRequest{
		RegistrationId: registration.ID,
		AllocationId:   "new-while-draining", DisplayLabel: "new", PolicyRevision: state.GetRegistration().GetPoolPolicyRevision(),
		Limits:             &hostv1.ResourceLimits{CpuMillis: testOptional(uint32(1)), MemoryBytes: testOptional(uint64(1)), Pids: testOptional(uint32(1)), DiskBytes: testOptional(uint64(1))},
		EgressMode:         hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		ConnectionInstance: "connection-a",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("draining admitted a new allocation: %v", err)
	}
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenRuntimeSession(ctx, &hostv1.OpenRuntimeSessionRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: first.GetLease().GetLeaseId(), Generation: first.GetLease().GetGeneration(), AllocationId: "other"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-allocation session error=%v", err)
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second active lease error=%v", err)
	}
	renewed, err := client.RenewLease(ctx, &hostv1.RenewLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: first.GetLease().GetLeaseId(), Generation: first.GetLease().GetGeneration(), Duration: durationpb.New(500 * time.Millisecond)})
	if err != nil || renewed.GetLease().GetGeneration() != first.GetLease().GetGeneration() || !renewed.GetLease().GetExpiresAt().AsTime().After(first.GetLease().GetExpiresAt().AsTime()) {
		t.Fatalf("renewed=%+v err=%v", renewed, err)
	}
	released, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: renewed.GetLease().GetLeaseId(), Generation: renewed.GetLease().GetGeneration()})
	if err != nil || released.GetLease().GetState() != hostv1.LeaseState_LEASE_STATE_RELEASED {
		t.Fatalf("released=%+v err=%v", released, err)
	}
	if _, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: renewed.GetLease().GetLeaseId(), Generation: renewed.GetLease().GetGeneration()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second release error=%v", err)
	}
	type admissionResult struct {
		kind  string
		lease *hostv1.RemoteLease
		err   error
	}
	results := make(chan admissionResult, 2)
	go func() {
		response, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()})
		results <- admissionResult{kind: "lease", lease: response.GetLease(), err: err}
	}()
	go func() {
		_, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{
			RegistrationId:     registration.ID,
			ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING,
		})
		results <- admissionResult{kind: "drain", err: err}
	}()
	firstRace, secondRace := <-results, <-results
	if firstRace.err != nil && firstRace.kind == "drain" || secondRace.err != nil && secondRace.kind == "drain" {
		t.Fatalf("drain failed during race: first=%v second=%v", firstRace.err, secondRace.err)
	}
	if firstRace.kind == "drain" && secondRace.kind == "lease" && secondRace.err == nil {
		t.Fatal("lease was admitted after drain returned")
	}
	for _, result := range []admissionResult{firstRace, secondRace} {
		if result.kind == "lease" && result.err == nil {
			if _, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: result.lease.GetLeaseId(), Generation: result.lease.GetGeneration()}); err != nil {
				t.Fatalf("raced lease release failed: %v", err)
			}
		}
	}
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{
		RegistrationId:     registration.ID,
		ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED,
	}); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetEnvironment(ctx, remote.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		t.Fatalf("released allocation=%+v err=%v", current, err)
	}
	current, err = database.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retained allocation admitted a lease: %v", err)
	}
	current, err = database.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_READY))
	if err != nil {
		t.Fatal(err)
	}
	expiring, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(20 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()})
	if err != nil || expiring.GetLease().GetState() != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		t.Fatalf("expiring=%+v err=%v", expiring, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err = database.GetEnvironment(ctx, remote.ID)
		if err == nil && current.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expired allocation=%+v err=%v", current, err)
}

func testOptional[T any](value T) *T { return &value }
