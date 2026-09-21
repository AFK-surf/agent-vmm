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

func TestCollectScopedRemoteObservationUsesCachedUsageWithoutNamespaceFanout(t *testing.T) {
	client := new(scopedObservationClientStub)
	observedAt := timestamppb.Now()
	response, err := collectScopedRemoteObservation(context.Background(), client, &hostv1.Usage{MemoryBytes: 20, SampledAt: observedAt}, &hostv1.ComponentHealth{Component: "host-egress", State: hostv1.HealthState_HEALTH_STATE_HEALTHY, Reason: "/private/egress", ObservedAt: observedAt}, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if client.healthCalls != 1 || len(client.usageNamespaces) != 0 {
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

func TestPrivateControllerMayConnectWhileWorkloadAdmissionIsClosed(t *testing.T) {
	for _, test := range []struct {
		name         string
		registration store.RemoteRegistration
		state        hostv1.RemoteConnectionState
		want         bool
	}{
		{
			name:         "private controller connects during VM commit",
			registration: store.RemoteRegistration{OwnerKind: store.RemoteRegistrationPrivate},
			state:        hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
			want:         false,
		},
		{
			name:         "external controller remains fenced",
			registration: store.RemoteRegistration{OwnerKind: store.RemoteRegistrationExternal},
			state:        hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
			want:         true,
		},
		{
			name:         "disconnect remains available while fenced",
			registration: store.RemoteRegistration{OwnerKind: store.RemoteRegistrationExternal},
			state:        hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED,
			want:         false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := connectionRequiresRemoteAdmission(test.registration, test.state); got != test.want {
				t.Fatalf("connectionRequiresRemoteAdmission() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestRetireRemoteRegistrationFailsClosedWhenWorkloadStopIsUnavailable(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	_, err = database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2",
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1,
		StopGraceSeconds: 10,
		State:            int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState:  int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
		EnrollmentState:  int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUMaxMillis: 1000, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30}
	for _, id := range []string{"one", "two"} {
		_, err := database.BeginWorkloadAdmission(ctx, store.Environment{
			ID: id, NamespaceID: id, OwnerKind: store.OwnerRemoteRegistration,
			RegistrationID: "registration", AllocationID: id, AllocationGeneration: 1,
			AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
			CPUMillis:       1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30,
		}, contract)
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), allocationRetirementWake: make(chan struct{}, 1)}
	if err := controller.retireRemoteRegistration(ctx, "registration"); err == nil {
		t.Fatal("registration retirement succeeded without stopping its workloads")
	}
	for _, id := range []string{"one", "two"} {
		environment, err := database.GetEnvironment(ctx, id)
		if err != nil || environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
			t.Fatalf("allocation %q = %+v, %v", id, environment, err)
		}
	}
}

func TestPrivateControllerConnectionPersistsWhileWorkloadAdmissionIsClosed(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "private-registration", OwnerKind: store.RemoteRegistrationPrivate,
		InstallationID: "private-installation",
		Endpoint:       "private", DisplayName: "private", ProtocolVersion: "2",
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State:           int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
		EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	listener := bufconn.Listen(1 << 20)
	grpcServer := controller.NewRemoteComputeServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///remote", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		return invoke(metadata.AppendToOutgoingContext(ctx, "x-agent-vmm-registration", registration.ID), method, request, reply, connection, options...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := hostv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "private-controller", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatalf("private controller connection was blocked by closed workload admission: %v", err)
	}
	stored, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil || stored.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		t.Fatalf("private controller connection was not persisted: registration=%+v err=%v", stored, err)
	}
}

func TestAllocationAdmissionSharesCPUAndMemoryButKeepsPIDAndDiskLimits(t *testing.T) {
	registration := store.RemoteRegistration{ID: "registration",
		PerEnvPIDs: 256, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL)}
	hostCapacity := resourcev2.Contract{CPUMaxMillis: 6000, MemoryMaxBytes: 8 << 30, PIDMax: 1024, WritableQuotaBytes: 24 << 30}
	base := resourcev2.Contract{CPUMaxMillis: 16000, MemoryMaxBytes: 64 << 30, PIDMax: 256, WritableQuotaBytes: 8 << 30}
	if err := admitAllocationV2(registration, base, hostCapacity, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatalf("valid independent contract rejected: %v", err)
	}
	for _, test := range []struct {
		name      string
		dimension string
		mutate    func(*resourcev2.Contract)
	}{
		{name: "pids", dimension: "pids", mutate: func(value *resourcev2.Contract) { value.PIDMax++ }},
		{name: "writable", dimension: "writable", mutate: func(value *resourcev2.Contract) { value.WritableQuotaBytes++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			contract := base
			test.mutate(&contract)
			err := admitAllocationV2(registration, contract, hostCapacity, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatal("over-capacity contract admitted")
			}
			if got := capacityDimensionMetadata(err); got != test.dimension {
				t.Fatalf("capacity dimension = %q, want %q", got, test.dimension)
			}
		})
	}
}

func TestEnvironmentCountDoesNotGateAllocationAdmission(t *testing.T) {
	registration := store.RemoteRegistration{ID: "registration", MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL)}
	if err := admitAllocationV2(registration, resourcev2.Contract{}, resourcev2.Contract{}, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatalf("old environment count policy still gates one allocation: %v", err)
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

func TestRemoteAllocationInventoryAndConnectionFences(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", ProtocolVersion: "1",
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUMaxMillis: 1000, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "remote", NamespaceID: "remote-namespace", DisplayName: "remote", EgressMode: 1,
		OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
		AllocationGeneration: 7, CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30,
	}, resourcev2.Contract{CPUMaxMillis: 4000, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 16 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle.admission.Store(1)
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	listener := bufconn.Listen(1 << 20)
	grpcServer := controller.NewRemoteComputeServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///remote",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
			return invoke(metadata.AppendToOutgoingContext(ctx, "x-agent-vmm-registration", registration.ID), method, request, reply, connection, options...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := hostv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: "other"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-registration transport identity error=%v", err)
	}
	state, err := client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: registration.ID})
	if err != nil || len(state.GetRegistration().GetAllocations()) != 1 ||
		state.GetRegistration().GetAllocations()[0].GetAllocationId() != "allocation" {
		t.Fatalf("scoped inventory=%+v err=%v", state, err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", ConnectionEpoch: 2,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale connection mutation error=%v", err)
	}
	if _, err := client.OpenRuntimeSession(ctx, &hostv1.OpenRuntimeSessionRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: "allocation",
		AllocationGeneration: 6, ExecutionOwnerId: "runtime:workload:1",
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale allocation generation opened a session: %v", err)
	}
	if _, err := client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: "allocation",
		ExpectedPolicyRevision: environment.PolicyRevision, ExpectedGeneration: 6,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale generation release error=%v", err)
	}
	unchanged, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || unchanged.AllocationGeneration != 7 ||
		unchanged.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) ||
		unchanged.Resources() != contract {
		t.Fatalf("stale commands changed durable allocation facts: %+v err=%v", unchanged, err)
	}
}

func TestControllerDisconnectPreservesAcceptedAllocation(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "1",
		PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteConnection(ctx, registration.ID, "first", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
		t.Fatal(err)
	}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration,
		RegistrationID: registration.ID, AllocationID: "allocation",
		AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY), AllocationGeneration: 1,
		CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1,
	}, resourcev2.Contract{CPUMaxMillis: 2, MemoryMaxBytes: 2, PIDMax: 2, WritableQuotaBytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database, sessions: make(map[string]*sessionServer)}
	if _, err := database.SetRemoteConnection(ctx, registration.ID, "first", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED)); err != nil {
		t.Fatal(err)
	}
	controller.disconnectRemoteRegistration(registration.ID, "first")
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) ||
		current.AllocationGeneration != 1 {
		t.Fatalf("controller disconnect changed accepted allocation: %+v err=%v", current, err)
	}
}

func TestConnectionReplacementFencesOldTransportWithoutRevokingAcceptedAllocation(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2",
		PerEnvPIDs: 256, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration,
		RegistrationID: registration.ID, AllocationID: "allocation",
		AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY), AllocationGeneration: 7,
		CPUMillis: 500, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 2 << 30,
	}, resourcev2.Contract{CPUMaxMillis: 1000, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle.admission.Store(1)
	listener := bufconn.Listen(1 << 20)
	grpcServer := controller.NewRemoteComputeServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///remote",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
			return invoke(metadata.AppendToOutgoingContext(ctx, "x-agent-vmm-registration", registration.ID), method, request, reply, connection, options...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := hostv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", ConnectionEpoch: 2,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) ||
		current.AllocationGeneration != 7 {
		t.Fatalf("replacement changed accepted allocation: %+v err=%v", current, err)
	}
	updated, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil || updated.ConnectionInstance != "connection-b" ||
		updated.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		t.Fatalf("replacement connection=%+v err=%v", updated, err)
	}
	if _, err := client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", AllocationId: "allocation",
		ExpectedPolicyRevision: environment.PolicyRevision, ExpectedGeneration: 7,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("old transport release code=%s err=%v", status.Code(err), err)
	}
}

func testOptional[T any](value T) *T { return &value }
