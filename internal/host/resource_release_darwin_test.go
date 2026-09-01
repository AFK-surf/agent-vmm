//go:build darwin

package host

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type durableReservationServer struct {
	vmmdv1.UnimplementedVMMDServiceServer
	path                                  string
	dropReserve, dropRelease, dropConfirm bool
}

type durableReservationState struct {
	Receipt *vmmdv1.WorkloadResourceReceipt `json:"receipt"`
	State   string                          `json:"state"`
}

func (server *durableReservationServer) load() (*durableReservationState, error) {
	data, err := os.ReadFile(server.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	value := new(durableReservationState)
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (server *durableReservationServer) persist(value *durableReservationState) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(server.path, data, 0o600)
}

func (server *durableReservationServer) ReserveWorkloadResources(_ context.Context, request *vmmdv1.ReserveWorkloadResourcesRequest) (*vmmdv1.WorkloadResourceReceipt, error) {
	value, err := server.load()
	if err != nil {
		return nil, err
	}
	digest := contractFromGuestRPC(request.GetResources()).Digest(request.GetAllocationId(), request.GetGeneration(), request.GetNamespaceId())
	want := &vmmdv1.WorkloadResourceReceipt{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: digest[:]}
	if value == nil {
		value = &durableReservationState{Receipt: want, State: "reserved"}
		if err := server.persist(value); err != nil {
			return nil, err
		}
	} else if value.State != "reserved" || value.Receipt.GetAllocationId() != want.GetAllocationId() || value.Receipt.GetGeneration() != want.GetGeneration() {
		return nil, status.Error(codes.Aborted, "reservation identity is terminal or conflicting")
	}
	if server.dropReserve {
		server.dropReserve = false
		return nil, status.Error(codes.Unavailable, "reserve response lost")
	}
	return value.Receipt, nil
}

func contractFromGuestRPC(value *vmmdv1.WorkloadResourcesV2) resourcev2.Contract {
	return resourcev2.Contract{CPUGuaranteeMillis: uint64(value.GetCpuGuaranteeMillis()), CPUMaxMillis: uint64(value.GetCpuMaxMillis()), CPUWeight: uint64(value.GetCpuWeight()), MemoryHighBytes: value.GetMemoryHighBytes(), MemoryMaxBytes: value.GetMemoryMaxBytes(), PIDMax: uint64(value.GetPidMax()), WritableQuotaBytes: value.GetWritableQuotaBytes()}
}

func (server *durableReservationServer) ReleaseWorkloadResources(_ context.Context, request *vmmdv1.ReleaseWorkloadResourcesRequest) (*vmmdv1.ReleaseWorkloadResourcesResponse, error) {
	value, err := server.load()
	if err != nil {
		return nil, err
	}
	if value == nil || value.Receipt.GetAllocationId() != request.GetAllocationId() || value.Receipt.GetGeneration() != request.GetGeneration() {
		return nil, status.Error(codes.Aborted, "release identity conflict")
	}
	value.State = "released"
	if err := server.persist(value); err != nil {
		return nil, err
	}
	if server.dropRelease {
		server.dropRelease = false
		return nil, status.Error(codes.Unavailable, "release response lost")
	}
	return &vmmdv1.ReleaseWorkloadResourcesResponse{Released: value.Receipt}, nil
}

func (server *durableReservationServer) ConfirmWorkloadResourceRelease(_ context.Context, request *vmmdv1.ConfirmWorkloadResourceReleaseRequest) (*vmmdv1.ConfirmWorkloadResourceReleaseResponse, error) {
	value, err := server.load()
	if err != nil {
		return nil, err
	}
	receipt := &vmmdv1.WorkloadResourceReceipt{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: request.GetContractSha256()}
	if value != nil {
		if value.State != "released" || value.Receipt.GetAllocationId() != request.GetAllocationId() || value.Receipt.GetGeneration() != request.GetGeneration() {
			return nil, status.Error(codes.Aborted, "confirm identity conflict")
		}
		receipt = value.Receipt
		if err := os.Remove(server.path); err != nil {
			return nil, err
		}
	}
	if server.dropConfirm {
		server.dropConfirm = false
		return nil, status.Error(codes.Unavailable, "confirm response lost")
	}
	return &vmmdv1.ConfirmWorkloadResourceReleaseResponse{Confirmed: receipt}, nil
}

func startDurableReservationServer(t *testing.T, server *durableReservationServer) (vmmdv1.VMMDServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	connection, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return vmmdv1.NewVMMDServiceClient(connection), func() { connection.Close(); grpcServer.Stop(); listener.Close() }
}

type lossReleaseClient struct {
	vmmdv1.VMMDServiceClient
	releaseCalls int
	confirmCalls int
	tombstone    bool
}

func (client *lossReleaseClient) ReleaseWorkloadResources(_ context.Context, request *vmmdv1.ReleaseWorkloadResourcesRequest, _ ...grpc.CallOption) (*vmmdv1.ReleaseWorkloadResourcesResponse, error) {
	client.releaseCalls++
	client.tombstone = true
	if client.releaseCalls == 1 {
		return nil, status.Error(codes.Unavailable, "release response lost")
	}
	return &vmmdv1.ReleaseWorkloadResourcesResponse{Released: &vmmdv1.WorkloadResourceReceipt{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: request.GetContractSha256()}}, nil
}

func (client *lossReleaseClient) ConfirmWorkloadResourceRelease(_ context.Context, request *vmmdv1.ConfirmWorkloadResourceReleaseRequest, _ ...grpc.CallOption) (*vmmdv1.ConfirmWorkloadResourceReleaseResponse, error) {
	client.confirmCalls++
	client.tombstone = false
	if client.confirmCalls == 1 {
		return nil, status.Error(codes.Unavailable, "confirm response lost")
	}
	return &vmmdv1.ConfirmWorkloadResourceReleaseResponse{Confirmed: &vmmdv1.WorkloadResourceReceipt{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: request.GetContractSha256()}}, nil
}

func TestReleaseLossAndRestartConvergesThroughExistingReservationReconcile(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 1,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 4 << 30, AggregatePIDs: 512, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 2000, PerEnvMemoryBytes: 4 << 30, PerEnvPIDs: 512, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, CPUWeight: 100, MemoryHighBytes: 768 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 2 << 30}
	environment, _, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "workload", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationState: 2},
		store.WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: "registration", EnvironmentID: "environment", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 1000, CPUMaxMillis: 1500, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	digest := contract.Digest("allocation", 7, "namespace")
	if _, err := database.CommitGuestReservation(ctx, "allocation", 7, digest[:]); err != nil {
		t.Fatal(err)
	}
	reservation, err := database.BeginWorkloadRelease(ctx, "allocation", 7, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	client := new(lossReleaseClient)
	controller := &Controller{store: database}
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); status.Code(err) != codes.Unavailable || !client.tombstone {
		t.Fatalf("lost release response err=%v tombstone=%v", err, client.tombstone)
	}
	reservation, err = database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservation.Phase != store.WorkloadReservationReleasing {
		t.Fatalf("restart fence=%+v err=%v", reservation, err)
	}
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); status.Code(err) != codes.Unavailable || client.tombstone {
		t.Fatalf("lost confirm response err=%v tombstone=%v", err, client.tombstone)
	}
	reservation, err = database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservation.Phase != store.WorkloadReservationReleasedPendingConfirm {
		t.Fatalf("pending confirmation=%+v err=%v", reservation, err)
	}
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); err != nil {
		t.Fatalf("idempotent confirmation after restart: %v", err)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetAllocationState(ctx, environment.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, environment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetWorkloadReservation(ctx, "allocation", 7); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Host reservation survived confirmed environment cleanup: %v", err)
	}
}

func TestAutomaticLeaseExpiryWithLostReserveConvergesAcrossHostAndGuestRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "host.db")
	guestState := filepath.Join(directory, "guest-reservation.json")
	database, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 1,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 4 << 30, AggregatePIDs: 512, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 2000, PerEnvMemoryBytes: 4 << 30, PerEnvPIDs: 512, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, CPUWeight: 100, MemoryHighBytes: 768 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 2 << 30}
	environment, reservation, err := database.BeginWorkloadAdmission(ctx,
		store.Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "workload", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		store.WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: "registration", EnvironmentID: "environment", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 1000, CPUMaxMillis: 1500, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	client, stopGuest := startDurableReservationServer(t, &durableReservationServer{path: guestState, dropReserve: true})
	if _, err := controller.reconcileGuestReservation(ctx, client, environment, reservation); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost reserve response error=%v", err)
	}
	stopGuest()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller = &Controller{store: database}
	reservation, err = database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservation.Phase != store.WorkloadReservationLogical || len(reservation.GuestReceipt) != 0 {
		t.Fatalf("Host restart logical fence=%+v err=%v", reservation, err)
	}
	environment, err = database.AcquireRemoteLeaseProjection(ctx, store.RemoteLeaseProjection{LeaseID: "lease", RegistrationID: "registration", AllocationID: "allocation", EnvironmentID: environment.ID,
		WorkloadRef: "task", Generation: 1, AllocationGeneration: 7, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}, environment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	// A Host crash loses the in-memory lease owner. Open must atomically turn
	// the exact persisted projection into a release obligation before any
	// controller can observe and recommit the allocation as READY.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller.store = database
	environment, err = database.GetEnvironment(ctx, environment.ID)
	reservation, loadErr := database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || loadErr != nil || reservation.Phase != store.WorkloadReservationReleaseRequired || environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		t.Fatalf("automatic expiry release obligation=%+v environment=%+v err=%v loadErr=%v", reservation, environment, err, loadErr)
	}
	client, stopGuest = startDurableReservationServer(t, &durableReservationServer{path: guestState})
	reservation, err = controller.recoverGuestReservationForRelease(ctx, client, environment, reservation)
	stopGuest()
	if err != nil || reservation.Phase != store.WorkloadReservationReleasing {
		t.Fatalf("terminal exact reserve recovery=%+v err=%v", reservation, err)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		t.Fatalf("terminal reserve recovery revived environment=%+v err=%v", current, err)
	}
	client, stopGuest = startDurableReservationServer(t, &durableReservationServer{path: guestState, dropRelease: true})
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost release response error=%v", err)
	}
	stopGuest()
	reservation, err = database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservation.Phase != store.WorkloadReservationReleasing {
		t.Fatalf("release restart fence=%+v err=%v", reservation, err)
	}
	client, stopGuest = startDurableReservationServer(t, &durableReservationServer{path: guestState, dropConfirm: true})
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost confirm response error=%v", err)
	}
	stopGuest()
	reservation, err = database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || reservation.Phase != store.WorkloadReservationReleasedPendingConfirm {
		t.Fatalf("confirm restart fence=%+v err=%v", reservation, err)
	}
	client, stopGuest = startDurableReservationServer(t, &durableReservationServer{path: guestState})
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); err != nil {
		t.Fatalf("idempotent exact confirm after Guest restart: %v", err)
	}
	stopGuest()
	current, err = database.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetAllocationState(ctx, environment.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, environment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(guestState); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Guest tombstone survived confirmed cleanup: %v", err)
	}
	if _, err := database.GetWorkloadReservation(ctx, "allocation", 7); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Host reservation survived confirmed cleanup: %v", err)
	}
}
