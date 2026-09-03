//go:build darwin

package host

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type scopedObservationClientStub struct {
	vmmdv1.VMMDServiceClient
	healthCalls     int
	usageNamespaces []string
}

func (client *scopedObservationClientStub) ComponentHealth(context.Context, *vmmdv1.ComponentHealthRequest, ...grpc.CallOption) (*vmmdv1.ComponentHealthResponse, error) {
	client.healthCalls++
	return &vmmdv1.ComponentHealthResponse{Components: []*vmmdv1.ComponentStatus{{Component: "vmmd", State: vmmdv1.ComponentState_COMPONENT_STATE_HEALTHY, Reason: "/private/reason", ObservedAtUnixNano: time.Now().UnixNano()}}}, nil
}

func (client *scopedObservationClientStub) GetNamespaceUsage(_ context.Context, request *vmmdv1.GetNamespaceUsageRequest, _ ...grpc.CallOption) (*vmmdv1.GetUsageResponse, error) {
	client.usageNamespaces = append(client.usageNamespaces, request.GetNamespaceId())
	return &vmmdv1.GetUsageResponse{Usage: &vmmdv1.Usage{MemoryBytes: 10}}, nil
}

func TestCollectScopedRemoteObservationUsesOneHealthCallAndExactNamespaces(t *testing.T) {
	client := new(scopedObservationClientStub)
	observedAt := timestamppb.Now()
	response, err := collectScopedRemoteObservation(context.Background(), client, []store.Environment{{NamespaceID: "one"}, {NamespaceID: "two"}}, &hostv1.ComponentHealth{Component: "host-egress", State: hostv1.HealthState_HEALTH_STATE_HEALTHY, Reason: "/private/egress", ObservedAt: observedAt}, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if client.healthCalls != 1 || len(client.usageNamespaces) != 2 || client.usageNamespaces[0] != "one" || client.usageNamespaces[1] != "two" {
		t.Fatalf("calls: health=%d namespaces=%v", client.healthCalls, client.usageNamespaces)
	}
	if response.GetUsage().GetMemoryBytes() != 20 || response.GetUsage().GetStale() {
		t.Fatalf("usage=%+v", response.GetUsage())
	}
	for _, component := range response.GetHealth().GetComponents() {
		if component.GetReason() != "" {
			t.Fatalf("remote health leaked component reason: %+v", component)
		}
	}
}

func TestAllocationAdmissionV2KeepsFiveCapacityDimensionsIndependent(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registration := store.RemoteRegistration{ID: "registration", MaxEnvironments: 2,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 2000, PerEnvMemoryBytes: 2 << 30, PerEnvPIDs: 256, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL)}
	hostCapacity := resourcev2.Capacity{CPUGuaranteeMillis: 4000, CPUMaxMillis: 6000, MemoryHighBytes: 8 << 30, MemoryMaxBytes: 8 << 30, PIDMax: 1024, WritableQuotaBytes: 24 << 30}
	base := resourcev2.Contract{CPUGuaranteeMillis: 1000, CPUMaxMillis: 2000, CPUWeight: 100, MemoryHighBytes: 1 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 8 << 30}
	if err := admitAllocationV2(context.Background(), database, registration, nil, base, hostCapacity, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatalf("valid independent contract rejected: %v", err)
	}
	for _, test := range []struct {
		name      string
		dimension string
		mutate    func(*resourcev2.Contract)
	}{
		{name: "cpu guarantee", dimension: "cpu_guarantee", mutate: func(value *resourcev2.Contract) { value.CPUGuaranteeMillis = 2001 }},
		{name: "cpu max", dimension: "cpu_max", mutate: func(value *resourcev2.Contract) { value.CPUMaxMillis = 2001 }},
		{name: "memory", dimension: "memory", mutate: func(value *resourcev2.Contract) { value.MemoryMaxBytes++ }},
		{name: "pids", dimension: "pids", mutate: func(value *resourcev2.Contract) { value.PIDMax++ }},
		{name: "writable", dimension: "writable", mutate: func(value *resourcev2.Contract) { value.WritableQuotaBytes++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			contract := base
			test.mutate(&contract)
			err := admitAllocationV2(context.Background(), database, registration, nil, contract, hostCapacity, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatal("over-capacity contract admitted")
			}
			if got := capacityDimensionMetadata(err); got != test.dimension {
				t.Fatalf("capacity dimension = %q, want %q", got, test.dimension)
			}
		})
	}
}

func TestEightGiBHostCapacityAdmitsFourProductTemplates(t *testing.T) {
	capacity := remoteWorkloadCapacity(8, 8<<30, 32<<30)
	if capacity.MemoryHighBytes != 9<<29 {
		t.Fatalf("8 GiB VM memory-high capacity = %d, want 4.5 GiB", capacity.MemoryHighBytes)
	}
	external := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 2000, CPUWeight: 5000, MemoryHighBytes: 1 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	meeting := resourcev2.Contract{CPUGuaranteeMillis: 2000, CPUMaxMillis: 3000, CPUWeight: 8000, MemoryHighBytes: 1 << 30, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	// Codex, Pi, and Claude use the external profile; Meeting has its own max.
	used := resourcev2.Capacity{}.Add(external).Add(external).Add(external)
	if err := used.AdmitsShared(meeting, capacity); err != nil {
		t.Fatalf("soft memory-high profile was rejected: %v (capacity=%+v used=%+v)", err, capacity, used)
	}
	overBudget := meeting
	overBudget.MemoryHighBytes = (1536 << 20) + 1
	if err := used.AdmitsShared(overBudget, capacity); err == nil {
		t.Fatal("memory-high profile above 4.5 GiB was admitted")
	}
}

func TestEnvironmentCountLimitIsTerminalPolicyConflictNotDimensionlessCapacity(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registration := store.RemoteRegistration{ID: "registration", MaxEnvironments: 1}
	err = admitAllocationV2(context.Background(), database, registration,
		[]store.Environment{{ID: "retained"}}, resourcev2.Contract{}, resourcev2.Capacity{}, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("environment slot exhaustion code = %s, want FailedPrecondition: %v", status.Code(err), err)
	}
	if got := capacityDimensionMetadata(err); got != "" {
		t.Fatalf("environment slot exhaustion invented capacity dimension %q", got)
	}
}

func TestAllocationProjectionPreservesGuestPhysicalCapacityDimension(t *testing.T) {
	for _, dimension := range []string{"storage_headroom", "import_slot"} {
		guestErr := resourceErrorWithMetadata(codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", "guest_capacity", dimension,
			"Guest capacity is unavailable", map[string]string{"resource_dimension": dimension})
		got := allocationProjectionError(guestErr, "projection failed")
		if status.Code(got) != codes.ResourceExhausted {
			t.Fatalf("dimension %q code=%v", dimension, status.Code(got))
		}
		found := false
		for _, detail := range status.Convert(got).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetMetadata()["resource_dimension"] == dimension {
				found = true
			}
		}
		if !found {
			t.Fatalf("dimension %q was lost across Host projection: %v", dimension, got)
		}
	}
}

func capacityDimensionMetadata(err error) string {
	value, _ := status.FromError(err)
	for _, detail := range value.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetMetadata()["resource_dimension"]
		}
	}
	return ""
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
	contract := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, CPUWeight: 100, MemoryHighBytes: 768 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30}
	remote, _, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "remote", NamespaceID: "remote-namespace", DisplayName: "remote", EgressMode: 1, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationState: 1},
		store.WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: registration.ID, EnvironmentID: "remote", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 4000, CPUMaxMillis: 4000, MemoryHighBytes: 4 << 30, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 16 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle.admission.Store(1)
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
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
	// Model the old timer resuming after it observed the original deadline but
	// after RenewLease won remoteAdmissionMu. Deadline revalidation at that same
	// linearization point must make the stale timer a no-op.
	staleTimerNow := first.GetLease().GetExpiresAt().AsTime().Add(time.Nanosecond)
	if expired, err := controller.expireLease(renewed.GetLease().GetLeaseId(), renewed.GetLease().GetGeneration(), staleTimerNow); err != nil || expired != nil {
		t.Fatalf("stale pre-renew timer expired current lease=%+v err=%v", expired, err)
	}
	controller.mu.Lock()
	activeAfterRenew := controller.remoteLeases[renewed.GetLease().GetLeaseId()]
	controller.mu.Unlock()
	if activeAfterRenew == nil || !activeAfterRenew.expiresAt.Equal(renewed.GetLease().GetExpiresAt().AsTime()) {
		t.Fatalf("stale timer changed renewed lease=%+v", activeAfterRenew)
	}
	released, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: renewed.GetLease().GetLeaseId(), Generation: renewed.GetLease().GetGeneration()})
	if err != nil || released.GetLease().GetState() != hostv1.LeaseState_LEASE_STATE_RELEASED {
		t.Fatalf("released=%+v err=%v", released, err)
	}
	if _, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: renewed.GetLease().GetLeaseId(), Generation: renewed.GetLease().GetGeneration()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second release error=%v", err)
	}
	controller.vmLifecycle.closeAdmission()
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING}); err != nil {
		t.Fatalf("closed runtime admission blocked draining: %v", err)
	}
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED}); status.Code(err) != codes.Unavailable {
		t.Fatalf("closed runtime admission enabled registration: %v", err)
	}
	generation, _ := controller.vmLifecycle.admissionToken()
	controller.vmLifecycle.admission.Store(generation<<1 | 1)
	if _, err := client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED}); err != nil {
		t.Fatalf("healthy runtime could not re-enable registration: %v", err)
	}
	beforeRestart, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()})
	if err != nil {
		t.Fatal(err)
	}
	controller.invalidateRuntimeCapabilities(ctx)
	controller.mu.Lock()
	_, retainedLease := controller.remoteLeases[beforeRestart.GetLease().GetLeaseId()]
	_, retainedAllocationCapability := controller.allocationLeases[remote.ID]
	controller.mu.Unlock()
	if retainedLease || retainedAllocationCapability {
		t.Fatal("VM invalidation retained a short-lived lease capability")
	}
	afterInvalidation, err := database.GetEnvironment(ctx, remote.ID)
	if err != nil || afterInvalidation.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		t.Fatalf("VM invalidation did not preserve the allocation as ready: environment=%+v err=%v", afterInvalidation, err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 4,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err == nil {
		t.Fatal("recovery window admitted a connector before final commit")
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); err == nil {
		t.Fatal("recovery window admitted a lease before final commit")
	}
	if _, err := client.EnsureAllocation(ctx, &hostv1.EnsureAllocationRequest{RegistrationId: registration.ID, AllocationId: "blocked-during-recovery"}); err == nil {
		t.Fatal("recovery window admitted an allocation before final commit")
	}
	if _, err := database.GetRemoteEnvironment(ctx, registration.ID, "blocked-during-recovery"); err == nil {
		t.Fatal("closed admission persisted a draining allocation")
	}
	controller.mu.Lock()
	leaseCount := len(controller.remoteLeases)
	controller.mu.Unlock()
	if leaseCount != 0 {
		t.Fatalf("closed admission produced %d in-memory leases", leaseCount)
	}
	generation, _ = controller.vmLifecycle.admissionToken()
	controller.vmLifecycle.admission.Store(generation<<1 | 1) // models the owner's final healthy commit
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 4,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()})
	if err != nil {
		t.Fatalf("same allocation remained blocked after VM recovery: %v", err)
	}
	if _, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: afterRestart.GetLease().GetLeaseId(), Generation: afterRestart.GetLease().GetGeneration()}); err != nil {
		t.Fatal(err)
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
	// The two RPCs are concurrent. Channel delivery order is not the
	// linearization order: a lease may commit first and its goroutine may be
	// descheduled before publishing the result while drain commits second.
	// Such a lease is valid. The durable assertion is that, after both calls
	// return, admission is drained and no later lease can be acquired.
	for _, result := range []admissionResult{firstRace, secondRace} {
		if result.kind == "lease" && result.err == nil {
			if _, err := client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", LeaseId: result.lease.GetLeaseId(), Generation: result.lease.GetGeneration()}); err != nil {
				t.Fatalf("raced lease release failed: %v", err)
			}
		}
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("lease was admitted after concurrent drain settled: %v", err)
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
	if _, err := client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", AllocationId: "allocation",
		ExpectedPolicyRevision: current.PolicyRevision, ExpectedGeneration: 6,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale generation release error=%v", err)
	}
	unchanged, err := database.GetEnvironment(ctx, remote.ID)
	if err != nil || unchanged.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) || unchanged.PolicyRevision != current.PolicyRevision {
		t.Fatalf("stale generation release mutated allocation: %+v err=%v", unchanged, err)
	}
	releasedAllocation, err := client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", AllocationId: "allocation",
		ExpectedPolicyRevision: current.PolicyRevision, ExpectedGeneration: 7,
	})
	if err != nil || releasedAllocation.GetAllocation().GetState() != hostv1.AllocationState_ALLOCATION_STATE_RETAINED || releasedAllocation.GetAllocation().GetGeneration() != 7 {
		t.Fatalf("exact generation release=%+v err=%v", releasedAllocation, err)
	}
	releaseObligation, err := database.GetWorkloadReservationByEnvironment(ctx, remote.ID)
	if err != nil || releaseObligation.Phase != store.WorkloadReservationReleaseRequired {
		t.Fatalf("exact release did not atomically create Guest obligation=%+v err=%v", releaseObligation, err)
	}
	current, err = database.GetEnvironment(ctx, remote.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", Duration: durationpb.New(500 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: allocationRevision()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retained allocation admitted a lease: %v", err)
	}
	expiringEnvironment, _, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "remote-expiring", NamespaceID: "remote-expiring-namespace", DisplayName: "expiring", EgressMode: 1, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation-expiring", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		store.WorkloadReservation{AllocationID: "allocation-expiring", Generation: 8, RegistrationID: registration.ID, EnvironmentID: "remote-expiring", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 4000, CPUMaxMillis: 4000, MemoryHighBytes: 4 << 30, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 16 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation-expiring", 8, []byte("expiring-receipt")); err != nil {
		t.Fatal(err)
	}
	expiring, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation-expiring", Duration: durationpb.New(20 * time.Millisecond), ConnectionInstance: "connection-a", ExpectedPolicyRevision: expiringEnvironment.PolicyRevision})
	if err != nil || expiring.GetLease().GetState() != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		t.Fatalf("expiring=%+v err=%v", expiring, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err = database.GetEnvironment(ctx, expiringEnvironment.ID)
		if err == nil && current.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
			reservation, reservationErr := database.GetWorkloadReservation(ctx, "allocation-expiring", 8)
			if reservationErr != nil || reservation.Phase != store.WorkloadReservationAdmitted {
				t.Fatalf("expired lease changed durable allocation reservation: reservation=%+v err=%v", reservation, reservationErr)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expired allocation=%+v err=%v", current, err)
}

func TestDisconnectGraceUsesPersistedDisconnectFence(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration := store.RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", ProtocolVersion: "1",
		MaxEnvironments: 1, AggregateCPUMillis: 1, AggregateMemoryBytes: 1, AggregatePIDs: 1, AggregateDiskBytes: 1,
		PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
		MaxLeaseSeconds: 60, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED), EnrollmentState: 2, ConnectorPhase: 1,
	}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteConnection(ctx, registration.ID, "first", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
		t.Fatal(err)
	}
	environment, _, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "environment", NamespaceID: "namespace", DisplayName: "remote", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1,
		OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
	}, store.WorkloadReservation{AllocationID: "allocation", Generation: 1, RegistrationID: registration.ID, EnvironmentID: "environment",
		Resources: resourcev2.Contract{CPUGuaranteeMillis: 1, CPUMaxMillis: 1, CPUWeight: 1, MemoryHighBytes: 1, MemoryMaxBytes: 1, PIDMax: 1, WritableQuotaBytes: 1}},
		resourcev2.Capacity{CPUGuaranteeMillis: 2, CPUMaxMillis: 2, MemoryHighBytes: 2, MemoryMaxBytes: 2, PIDMax: 2, WritableQuotaBytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	projection := store.RemoteLeaseProjection{LeaseID: "lease", RegistrationID: registration.ID, AllocationID: "allocation", AllocationGeneration: 1, EnvironmentID: environment.ID, WorkloadRef: "work", Generation: 1, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}
	environment, err = database.AcquireRemoteLeaseProjection(ctx, projection, environment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string)}
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	controller.remoteLeases[projection.LeaseID] = &remoteLease{id: projection.LeaseID, registrationID: registration.ID, allocationID: projection.AllocationID, environmentID: environment.ID, workloadRef: projection.WorkloadRef, generation: projection.Generation, expiresAt: projection.ExpiresAt, state: hostv1.LeaseState_LEASE_STATE_ACTIVE}
	controller.allocationLeases[environment.ID] = projection.LeaseID

	firstDisconnect, err := database.SetRemoteConnection(ctx, registration.ID, "first", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
	if err != nil {
		t.Fatal(err)
	}
	controller.disconnectRemoteLeases(registration.ID, "first", firstDisconnect.DisconnectedAt, 100*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, err := database.SetRemoteConnection(ctx, registration.ID, "second", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	secondDisconnect, err := database.SetRemoteConnection(ctx, registration.ID, "second", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
	if err != nil {
		t.Fatal(err)
	}
	controller.disconnectRemoteLeases(registration.ID, "second", secondDisconnect.DisconnectedAt, 300*time.Millisecond)
	time.Sleep(130 * time.Millisecond)
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED) {
		t.Fatalf("old disconnect timer revoked replacement grace: environment=%+v err=%v", current, err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err = database.GetEnvironment(ctx, environment.ID)
		if err == nil && current.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("current disconnect grace did not return allocation to ready: environment=%+v err=%v", current, err)
}

func TestConcurrentLeaseExpiryAndExplicitReleaseHaveSingleTerminalOwner(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration := store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 1,
		AggregateCPUMillis: 1000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 4 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 2 << 30, PerEnvPIDs: 256, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 250, CPUMaxMillis: 500, CPUWeight: 100, MemoryHighBytes: 512 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 2 << 30}
	environment, _, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		store.WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: registration.ID, EnvironmentID: "environment", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, MemoryHighBytes: 2 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation", 7, []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	projection := store.RemoteLeaseProjection{LeaseID: "lease", RegistrationID: registration.ID, AllocationID: "allocation", AllocationGeneration: 7,
		EnvironmentID: environment.ID, WorkloadRef: "task", Generation: 1, ExpiresAt: time.Now(), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}
	environment, err = database.AcquireRemoteLeaseProjection(ctx, projection, environment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string)}
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	controller.remoteLeases[projection.LeaseID] = &remoteLease{id: projection.LeaseID, registrationID: registration.ID, allocationID: projection.AllocationID,
		environmentID: environment.ID, workloadRef: projection.WorkloadRef, generation: projection.Generation, allocationGeneration: projection.AllocationGeneration,
		expiresAt: projection.ExpiresAt, state: hostv1.LeaseState_LEASE_STATE_ACTIVE}
	controller.allocationLeases[environment.ID] = projection.LeaseID
	type result struct {
		lease *remoteLease
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	go func() {
		<-start
		lease, finishErr := controller.finishLease("lease", 1, hostv1.LeaseState_LEASE_STATE_EXPIRED, true, false, false)
		results <- result{lease: lease, err: finishErr}
	}()
	go func() {
		<-start
		lease, finishErr := controller.finishLease("lease", 1, hostv1.LeaseState_LEASE_STATE_RELEASED, false, false, false)
		results <- result{lease: lease, err: finishErr}
	}()
	close(start)
	first, second := <-results, <-results
	winners := 0
	var terminal hostv1.LeaseState
	for _, candidate := range []result{first, second} {
		if candidate.lease != nil {
			winners++
			terminal = candidate.lease.state
		}
	}
	if winners != 1 {
		t.Fatalf("terminal lease claim winners=%d first=%+v second=%+v", winners, first, second)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil {
		t.Fatal(err)
	}
	if terminal != hostv1.LeaseState_LEASE_STATE_EXPIRED && terminal != hostv1.LeaseState_LEASE_STATE_RELEASED {
		t.Fatalf("unexpected terminal winner %s", terminal)
	}
	if current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) || reservation.Phase != store.WorkloadReservationAdmitted {
		t.Fatalf("lease terminal winner retired allocation: terminal=%s environment=%+v reservation=%+v", terminal, current, reservation)
	}
}

func TestConnectionReplacementRevokesActiveLeaseWithoutAdmissionDeadlock(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration := store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 1,
		AggregateCPUMillis: 1000, AggregateMemoryBytes: 2 << 30, AggregatePIDs: 256, AggregateDiskBytes: 4 << 30,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 2 << 30, PerEnvPIDs: 256, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 1, State: 1,
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED), EnrollmentState: 2, ConnectorPhase: 1}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 250, CPUMaxMillis: 500, CPUWeight: 100, MemoryHighBytes: 512 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 2 << 30}
	environment, _, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		store.WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: registration.ID, EnvironmentID: "environment", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, MemoryHighBytes: 2 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation", 7, []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	environment, err = database.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), remoteLeases: make(map[string]*remoteLease), allocationLeases: make(map[string]string)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle.admission.Store(1)
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
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}); err != nil {
		t.Fatal(err)
	}
	lease, err := client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: registration.ID, AllocationId: "allocation", WorkloadRef: "task", Duration: durationpb.New(time.Minute), ConnectionInstance: "connection-a", ExpectedPolicyRevision: environment.PolicyRevision})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, replaceErr := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-b", ConnectionEpoch: 2, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED})
		result <- replaceErr
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("connection replacement failed after exact revoke: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection replacement deadlocked while revoking an active lease")
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	reservation, reservationErr := database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservationErr != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) || reservation.Phase != store.WorkloadReservationAdmitted {
		t.Fatalf("replacement did not revoke only the lease: environment=%+v reservation=%+v err=%v reservationErr=%v", current, reservation, err, reservationErr)
	}
	controller.mu.Lock()
	active := controller.remoteLeases[lease.GetLease().GetLeaseId()]
	controller.mu.Unlock()
	if active != nil {
		t.Fatalf("replaced connection retained active lease=%+v", active)
	}
	updated, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil || updated.ConnectionInstance != "connection-b" || updated.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		t.Fatalf("replacement connection=%+v err=%v", updated, err)
	}
}

func testOptional[T any](value T) *T { return &value }
