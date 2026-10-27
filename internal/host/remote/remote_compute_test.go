//go:build darwin || linux

package remote

import (
	"context"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	_, err = database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "registration", Endpoint: "controller", DisplayName: "controller",
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1,
		StopGraceSeconds: 10,
		State:            int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState:  int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
		EnrollmentState:  int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30}
	for _, id := range []string{"one", "two"} {
		_, err := database.BeginWorkloadAdmission(ctx, store.Environment{
			ID: id, NamespaceID: id, OwnerKind: store.OwnerRemoteRegistration,
			RegistrationID: "registration", AllocationID: id, AllocationGeneration: 1,
			AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
			PIDs:            128, DiskBytes: 4 << 30,
		}, contract)
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := newTestOwner(&Owner{manager: &vmm.Manager{}, store: database, allocationRetirementWake: make(chan struct{}, 1)})
	if err := controller.RetireRegistration(ctx, "registration"); err == nil {
		t.Fatal("registration retirement succeeded without stopping its workloads")
	}
	for _, id := range []string{"one", "two"} {
		environment, err := database.GetEnvironment(ctx, id)
		if err != nil || environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
			t.Fatalf("allocation %q = %+v, %v", id, environment, err)
		}
	}
}

func TestRuntimeTeardownAfterAllocationDiscard(t *testing.T) {
	for _, operation := range []string{"revoke", "release"} {
		for _, afterLock := range []bool{false, true} {
			for _, staleConnection := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/after-lock=%t/stale-connection=%t", operation, afterLock, staleConnection), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
					if err != nil {
						t.Fatal(err)
					}
					defer database.Close()
					if _, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
						t.Fatal(err)
					}
					if _, err := database.SetRemoteConnection(ctx, "registration", "connection", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
						t.Fatal(err)
					}
					environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration,
						RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 1, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), PIDs: 128, DiskBytes: 4 << 30}, resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30})
					if err != nil {
						t.Fatal(err)
					}
					workloads := &testWorkloads{locking: make(chan string, 1)}
					owner := newTestOwner(&Owner{store: database, workloads: workloads})
					var unlock func()
					if afterLock {
						unlock, err = workloads.environments.Lock(ctx, environment.ID)
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if unlock != nil {
								unlock()
							}
						}()
					}
					discard := func() {
						if err := database.DiscardEnvironment(ctx, environment.ID); err != nil {
							t.Fatal(err)
						}
						if staleConnection {
							if _, err := database.SetRemoteConnection(ctx, "registration", "replacement", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
								t.Fatal(err)
							}
						}
					}
					if !afterLock {
						discard()
					}
					result := make(chan error, 1)
					go func() {
						var err error
						if operation == "revoke" {
							_, err = owner.RevokeRuntimeSession(ctx, &connectorv1.RevokeRuntimeSessionRequest{RegistrationId: "registration", ConnectionInstance: "connection", AllocationId: "allocation", AllocationGeneration: environment.AllocationGeneration, SessionId: "session"})
						} else {
							_, err = owner.ReleaseAllocation(ctx, &connectorv1.ReleaseAllocationRequest{RegistrationId: "registration", ConnectionInstance: "connection", AllocationId: "allocation", ExpectedGeneration: environment.AllocationGeneration, ExpectedPolicyRevision: environment.PolicyRevision})
						}
						result <- err
					}()
					if afterLock {
						select {
						case <-workloads.locking:
						case <-ctx.Done():
							t.Fatal("teardown did not reach the environment lock")
						}
						discard()
						unlock()
						unlock = nil
					}
					select {
					case err := <-result:
						wantCode := codes.Aborted
						if operation == "release" && !staleConnection {
							wantCode = codes.NotFound
						}
						if status.Code(err) != wantCode {
							t.Fatalf("teardown after discard = %v, want %s", err, wantCode)
						}
						reason, _ := hostv1.ErrorReasonOf(err)
						if (reason == hostv1.ErrorReason_ERROR_REASON_STALE_RUNTIME_SESSION) != (operation == "revoke" && !staleConnection) {
							t.Fatalf("teardown reason = %s, stale connection = %t", reason, staleConnection)
						}
					case <-ctx.Done():
						t.Fatal("teardown did not finish after discard")
					}
				})
			}
		}
	}
}

func TestAllocationAdmissionEnforcesPIDAndDiskLimits(t *testing.T) {
	registration := store.RemoteRegistration{ID: "registration",
		PerEnvPIDs: 256, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL)}
	hostCapacity := resourcev2.Contract{PIDMax: 1024, WritableQuotaBytes: 24 << 30}
	base := resourcev2.Contract{PIDMax: 256, WritableQuotaBytes: 8 << 30}
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
		guestErr := hostv1.NewError(codes.ResourceExhausted, hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED, "Guest capacity is unavailable", hostv1.WithResource("guest_capacity", dimension), hostv1.WithMetadata(map[string]string{"resource_dimension": dimension}))
		got := allocationProjectionError(guestErr, &hostv1.RemoteAllocation{AllocationId: "allocation", Generation: 2, Revision: 7})
		if status.Code(got) != codes.Unavailable {
			t.Fatalf("dimension %q code=%v", dimension, status.Code(got))
		}
		found := false
		foundAllocation := false
		for _, detail := range status.Convert(got).Details() {
			if allocation, ok := detail.(*hostv1.RemoteAllocation); ok && allocation.GetAllocationId() == "allocation" && allocation.GetGeneration() == 2 && allocation.GetRevision() == 7 {
				foundAllocation = true
			}
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetMetadata()["resource_dimension"] == dimension {
				found = true
			}
		}
		if !foundAllocation {
			t.Fatal("saved allocation identity was lost")
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

func TestNormalRetirementPagesEveryContainerAndRequiresObservedStop(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := newTestOwner(&Owner{store: database})
	environment := store.Environment{ID: "retained", NamespaceID: "private"}
	guest := &retirementGuest{containerCount: 101}
	client, closeGuest := startRetirementGuest(t, guest)
	defer closeGuest()
	if err := controller.stopRemoteNamespace(context.Background(), client, environment, 1); err != nil {
		t.Fatal(err)
	}
	if guest.stops.Load() != 101 {
		t.Fatalf("actual stops=%d", guest.stops.Load())
	}
	guest = &retirementGuest{unresolved: true, containerCount: 101}
	client, closeSecond := startRetirementGuest(t, guest)
	defer closeSecond()
	if err := controller.stopRemoteNamespace(context.Background(), client, environment, 1); err == nil {
		t.Fatal("unobserved stop allowed Retained")
	}
	if guest.stops.Load() != 1 {
		t.Fatal("retirement continued after unconfirmed stop")
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.NotFound} {
		guest := &retirementGuest{containerCount: 2, stopError: status.Error(code, "stop failed")}
		client, closeGuest := startRetirementGuest(t, guest)
		err := controller.stopRemoteNamespace(context.Background(), client, environment, 1)
		closeGuest()
		if status.Code(err) != code || guest.stops.Load() != 1 {
			t.Fatalf("stop failure swallowed: code=%s stops=%d err=%v", code, guest.stops.Load(), err)
		}
	}

	for _, test := range []struct {
		name                          string
		listCode, namespaceCode, want codes.Code
		reads                         int32
	}{
		{"exact namespace absent", codes.NotFound, codes.NotFound, codes.OK, 1},
		{"namespace present", codes.NotFound, codes.OK, codes.NotFound, 1},
		{"namespace observation unavailable", codes.NotFound, codes.Unavailable, codes.Unavailable, 1},
		{"inventory unavailable", codes.Unavailable, codes.NotFound, codes.Unavailable, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			guest := &retirementGuest{listError: status.Error(test.listCode, "inventory failed"), namespaceError: status.Error(test.namespaceCode, "namespace observation")}
			client, closeGuest := startRetirementGuest(t, guest)
			defer closeGuest()
			err := controller.stopRemoteNamespace(context.Background(), client, environment, 1)
			if status.Code(err) != test.want || guest.stops.Load() != 0 || guest.namespaceReads.Load() != test.reads {
				t.Fatalf("err=%v stops=%d namespace reads=%d", err, guest.stops.Load(), guest.namespaceReads.Load())
			}
			if test.reads != 0 && guest.queriedNamespace != environment.NamespaceID {
				t.Fatalf("observed namespace %q", guest.queriedNamespace)
			}
		})
	}
}

func TestRemoteRegistrationProjectionExpiresStaleHeartbeat(t *testing.T) {
	registration := store.RemoteRegistration{
		ConnectionState:     int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED),
		ConnectorPhase:      int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED),
		ConnectorObservedAt: time.Now().Add(-time.Minute),
	}
	projection := registrationToProto(registration)
	if projection.GetConnectionState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED ||
		projection.GetConnectorStatus().GetFailureCode() != "HEARTBEAT_STALE" {
		t.Fatalf("stale connector projected online: %+v", projection)
	}
}
