//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestPrivateControllerConnectionPersistsWhileWorkloadAdmissionIsClosed(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "private-registration", OwnerKind: store.RemoteRegistrationPrivate,
		InstallationID: "private-installation",
		Endpoint:       "private", DisplayName: "private", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State:           int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
		EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{manager: &vmm.Manager{}, store: database})
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
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
	client := connectorv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
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

func TestRemoteRuntimeRecoveryUsesHandlerAuthorityAndKeepsControllerLifecycle(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	identity := auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "environment", NamespaceID: "namespace", RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 7}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity)))
	hostv1.RegisterAgentRuntimeServiceServer(server, &agentServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	client := hostv1.NewAgentRuntimeServiceClient(connection)
	ctx := context.Background()
	for _, invoke := range []func(context.Context, *hostv1.ObserveImageOperationRequest) error{func(ctx context.Context, request *hostv1.ObserveImageOperationRequest) error {
		_, err := client.ObserveImageOperation(ctx, request)
		return err
	}, func(ctx context.Context, request *hostv1.ObserveImageOperationRequest) error {
		_, err := client.CancelImageOperation(ctx, &hostv1.CancelImageOperationRequest{Kind: request.GetKind(), TargetAllocationId: request.GetTargetAllocationId(), TargetAllocationGeneration: request.GetTargetAllocationGeneration()})
		return err
	}} {
		if err := invoke(ctx, &hostv1.ObserveImageOperationRequest{Kind: hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("recovery did not reach exact build identity validation: %v", err)
		}
		for _, target := range []struct {
			id         string
			generation uint64
		}{{"neighbor", 7}, {"allocation", 6}} {
			if err := invoke(ctx, &hostv1.ObserveImageOperationRequest{Kind: hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, TargetAllocationId: target.id, TargetAllocationGeneration: target.generation}); status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != "copy observation target differs from current allocation" {
				t.Fatalf("copy target authority bypassed: %v", err)
			}
		}
	}
	if _, err := hostv1.NewOperatorServiceClient(connection).GetVM(ctx, &hostv1.GetVMRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("runtime exposed operator service: %v", err)
	}
}

func TestRemoteAllocationInventoryAndConnectionFences(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "remote", NamespaceID: "remote-namespace", DisplayName: "remote", EgressMode: 1,
		OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
		AllocationGeneration: 7, PIDs: 128, DiskBytes: 4 << 30,
	}, resourcev2.Contract{PIDMax: 512, WritableQuotaBytes: 16 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{manager: &vmm.Manager{}, store: database})
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	admitTestLifecycle(t, controller)
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
	client := connectorv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.GetRemoteState(ctx, &connectorv1.GetRemoteStateRequest{RegistrationId: "other"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-registration transport identity error=%v", err)
	}
	state, err := client.GetRemoteState(ctx, &connectorv1.GetRemoteStateRequest{RegistrationId: registration.ID})
	if err != nil || len(state.GetRegistration().GetAllocations()) != 1 ||
		state.GetRegistration().GetAllocations()[0].GetAllocationId() != "allocation" {
		t.Fatalf("scoped inventory=%+v err=%v", state, err)
	}
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", ConnectionEpoch: 2,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale connection mutation error=%v", err)
	}
	if _, err := client.OpenRuntimeSession(ctx, &connectorv1.OpenRuntimeSessionRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: "allocation",
		AllocationGeneration: 6, ExecutionOwnerId: "runtime:workload:1",
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale allocation generation opened a session: %v", err)
	}
	pool, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil {
		t.Fatal(err)
	}
	pool.PerEnvPIDs++
	pool, err = database.UpdateRemotePool(ctx, pool, pool.Revision)
	if err != nil {
		t.Fatal(err)
	}
	repeated := &connectorv1.EnsureAllocationRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: "allocation", Generation: 7,
		PolicyRevision: registration.PoolPolicyRevision, EgressMode: hostv1.EgressMode(1), Limits: &hostv1.ResourceLimits{Pids: proto.Uint32(128), DiskBytes: proto.Uint64(4 << 30)}}
	for _, field := range []string{"pids", "disk_bytes"} {
		incomplete := proto.Clone(repeated).(*connectorv1.EnsureAllocationRequest)
		if field == "pids" {
			incomplete.Limits.Pids = nil
		} else {
			incomplete.Limits.DiskBytes = nil
		}
		if _, err := client.EnsureAllocation(ctx, incomplete); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("missing %s error = %v", field, err)
		}
	}
	_, repeatErr := client.EnsureAllocation(ctx, repeated)
	partial := false
	for _, detail := range status.Convert(repeatErr).Details() {
		if allocation, ok := detail.(*hostv1.RemoteAllocation); ok && allocation.GetAllocationId() == "allocation" && allocation.GetGeneration() == 7 && allocation.GetEnvironmentId() == "" {
			partial = true
		}
	}
	if !partial || status.Code(repeatErr) != codes.Unavailable {
		t.Fatalf("same identity was blocked by changed pool policy: %v", repeatErr)
	}
	(*repeated.Limits.Pids)++
	if _, err := client.EnsureAllocation(ctx, repeated); status.Code(err) != codes.Aborted {
		t.Fatalf("changed parameters bypassed policy revision: %v", err)
	}
	repeated.PolicyRevision = pool.PoolPolicyRevision
	if _, err := client.EnsureAllocation(ctx, repeated); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed parameters replaced durable allocation: %v", err)
	}

	currentRegistration, err := database.GetRemoteRegistration(ctx, registration.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteRegistrationState(ctx, registration.ID, currentRegistration.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING)); err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct {
		allocation string
		generation uint64
		code       codes.Code
	}{
		{"allocation", 6, codes.Aborted},
		{"neighbor", 7, codes.NotFound},
	} {
		if _, err := client.OpenRuntimeSession(ctx, &connectorv1.OpenRuntimeSessionRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: target.allocation, AllocationGeneration: target.generation, ExecutionOwnerId: "runtime:workload:1"}); status.Code(err) != target.code {
			t.Fatalf("draining recovery bypassed allocation scope: %v", err)
		}
	}
	if _, err := client.EnsureAllocation(ctx, &connectorv1.EnsureAllocationRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-b", AllocationId: "new-allocation", Generation: 1, PolicyRevision: environment.PolicyRevision}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("draining admitted a new allocation: %v", err)
	}
	if _, err := client.OpenRuntimeSession(ctx, &connectorv1.OpenRuntimeSessionRequest{RegistrationId: registration.ID, ConnectionInstance: "connection-a", AllocationId: "allocation", AllocationGeneration: 7, ExecutionOwnerId: "runtime:workload:1"}); status.Code(err) != codes.Aborted {
		t.Fatalf("draining allowed a stale controller connection: %v", err)
	}
	if _, err := client.ReleaseAllocation(ctx, &connectorv1.ReleaseAllocationRequest{
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
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
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
		PIDs: 1, DiskBytes: 1,
	}, resourcev2.Contract{PIDMax: 2, WritableQuotaBytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database})
	admitTestLifecycle(t, controller)
	if _, err := controller.remote.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "first", ConnectionEpoch: 1, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&remoteComputeServer{controller: controller}).SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{RegistrationId: registration.ID, ConnectionInstance: "first", ConnectionEpoch: 1, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED}); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) ||
		current.AllocationGeneration != 1 {
		t.Fatalf("controller disconnect changed accepted allocation: %+v err=%v", current, err)
	}
}

func TestConnectionReplacementFencesOldTransportWithoutRevokingAcceptedAllocation(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller", PerEnvPIDs: 256, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1,
		ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{
		ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration,
		RegistrationID: registration.ID, AllocationID: "allocation",
		AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY), AllocationGeneration: 7,
		PIDs: 128, DiskBytes: 2 << 30,
	}, resourcev2.Contract{PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{manager: &vmm.Manager{}, store: database})
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	admitTestLifecycle(t, controller)
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
	client := connectorv1.NewRemoteComputeServiceClient(connection)
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", ConnectionEpoch: 1,
		State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{
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
	if _, err := client.ReleaseAllocation(ctx, &connectorv1.ReleaseAllocationRequest{
		RegistrationId: registration.ID, ConnectionInstance: "connection-a", AllocationId: "allocation",
		ExpectedPolicyRevision: environment.PolicyRevision, ExpectedGeneration: 7,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("old transport release code=%s err=%v", status.Code(err), err)
	}
}

func testOptional[T any](value T) *T { return &value }
func TestAcceptedRemoteCleanupRetirementAndMissingNamespaceRecovery(t *testing.T) {
	for _, test := range []struct {
		name              string
		retained, missing bool
		responseMissing   bool
	}{
		{name: "missing namespace", missing: true},
		{name: "already retained missing namespace", retained: true, missing: true},
		{name: "stop unavailable"},
		{name: "reset response allocation disappeared", responseMissing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			databasePath := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			_, err = database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
			if err != nil {
				t.Fatal(err)
			}
			state := hostv1.AllocationState_ALLOCATION_STATE_READY
			if test.retained {
				state = hostv1.AllocationState_ALLOCATION_STATE_RETAINED
			}
			environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 1, AllocationState: int32(state), PIDs: 128, DiskBytes: 4 << 30}, resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30})
			if err != nil {
				t.Fatal(err)
			}
			controller := newTestController(&Controller{manager: &vmm.Manager{}, store: database})

			if _, err := database.SetRemoteConnection(ctx, "registration", "connection", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
				t.Fatal(err)
			}
			guest := &cleanupMissingNamespaceGuest{missing: test.missing, empty: test.responseMissing}
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer()
			vmmdv1.RegisterVMMDServiceServer(grpcServer, guest)
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()
			connection, err := grpc.NewClient("passthrough:///cleanup", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if err := controller.guestConnection.Connect(testGuestConnectionDialer{connection}, "boot"); err != nil {
				t.Fatal(err)
			}
			published := 0
			configureCleanupTestOwner(controller, &published)
			guestDB, err := sql.Open("sqlite", databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer guestDB.Close()
			guestDB.SetMaxOpenConns(1)
			if _, err := guestDB.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
				t.Fatal(err)
			}
			kind := hostv1.CleanupKind_CLEANUP_KIND_DISCARD
			if test.responseMissing {
				kind = hostv1.CleanupKind_CLEANUP_KIND_RESET
				// Model an allocation disappearing after the committed cleanup, before its response lookup.
				if _, err := guestDB.ExecContext(ctx, `CREATE TRIGGER hide_reset_allocation AFTER UPDATE OF state ON operations
WHEN NEW.state = 2 BEGIN UPDATE environments SET allocation_id = 'replaced' WHERE id = NEW.environment_id; END`); err != nil {
					t.Fatal(err)
				}
			}
			planned, err := controller.cleanup.PlanCleanupAllocation(ctx, &connectorv1.PlanCleanupAllocationRequest{RegistrationId: "registration", ConnectionInstance: "connection", AllocationId: "allocation", Kind: kind})
			if err != nil {
				t.Fatal(err)
			}
			_, executionErr := controller.cleanup.ExecuteCleanupAllocation(ctx, &connectorv1.ExecuteCleanupAllocationRequest{RegistrationId: "registration", ConnectionInstance: "connection", AllocationId: "allocation", Kind: kind, PlanRevision: planned.GetPlan().GetRevision()})

			var operationID string
			if err := guestDB.QueryRowContext(ctx, "SELECT id FROM operations WHERE environment_id=?", environment.ID).Scan(&operationID); err != nil {
				t.Fatal(err)
			}
			storedOperation, err := database.GetOperation(ctx, operationID)
			if err != nil {
				t.Fatal(err)
			}

			result := storedOperation
			if test.responseMissing {
				if storedOperation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) || status.Code(executionErr) != codes.Unavailable {
					t.Fatalf("committed reset response: operation=%+v err=%v", storedOperation, executionErr)
				}
				return
			}
			if test.missing && executionErr != nil {
				t.Fatal(executionErr)
			}
			if !test.missing && status.Code(executionErr) != codes.Unavailable {
				t.Fatalf("accepted failure=%v", executionErr)
			}
			current, err := database.GetEnvironment(ctx, environment.ID)
			if test.missing {
				if result.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) || !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("missing namespace discard: %+v %+v %v", result, current, err)
				}
			} else if result.State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) || storedOperation.ErrorReason != "CLEANUP_FAILED" || err != nil || current.AllocationState != int32(state) {
				t.Fatalf("failed stop changed allocation: %+v %+v %v", storedOperation, current, err)
			}
		})
	}
}

type cleanupMissingNamespaceGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	missing bool
	empty   bool
}

func (guest *cleanupMissingNamespaceGuest) ListContainers(_ context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	if guest.missing {
		return nil, status.Error(codes.NotFound, "namespace missing")
	}
	if request.GetPageSize() > 0 && !guest.empty {
		return nil, status.Error(codes.Unavailable, "stop unavailable")
	}
	return &vmmdv1.ListContainersResponse{}, nil
}
func (guest *cleanupMissingNamespaceGuest) ListVolumes(context.Context, *vmmdv1.ListVolumesRequest) (*vmmdv1.ListVolumesResponse, error) {
	return &vmmdv1.ListVolumesResponse{}, nil
}
func (guest *cleanupMissingNamespaceGuest) ListImages(context.Context, *vmmdv1.ListImagesRequest) (*vmmdv1.ListImagesResponse, error) {
	return &vmmdv1.ListImagesResponse{}, nil
}
func (guest *cleanupMissingNamespaceGuest) GetNamespace(context.Context, *vmmdv1.GetNamespaceRequest) (*vmmdv1.GetNamespaceResponse, error) {
	return nil, status.Error(codes.NotFound, "namespace missing")
}
