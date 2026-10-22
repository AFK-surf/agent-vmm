//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type retirementGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	path             string
	dropDelete       bool
	containerCount   int
	stops            atomic.Int32
	unresolved       bool
	stopError        error
	listError        error
	namespaceError   error
	namespaceReads   atomic.Int32
	queriedNamespace string
}

func (server *retirementGuest) DeleteNamespace(_ context.Context, request *vmmdv1.DeleteNamespaceRequest) (*vmmdv1.DeleteNamespaceResponse, error) {
	if request.GetNamespaceId() != "namespace" {
		return nil, status.Error(codes.InvalidArgument, "wrong namespace")
	}
	if _, err := os.Stat(filepath.Join(server.path, "private-data")); err == nil {
		return nil, status.Error(codes.FailedPrecondition, "namespace is not empty")
	}
	if err := os.Remove(filepath.Join(server.path, "namespace")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if server.dropDelete {
		server.dropDelete = false
		return nil, status.Error(codes.Unavailable, "delete response lost")
	}
	return &vmmdv1.DeleteNamespaceResponse{}, nil
}

func startRetirementGuest(t *testing.T, server *retirementGuest) (vmmdv1.VMMDServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(grpcServer, server)
	go grpcServer.Serve(listener)
	connection, err := grpc.NewClient("passthrough:///retirement-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	return vmmdv1.NewVMMDServiceClient(connection), func() { connection.Close(); grpcServer.Stop(); listener.Close() }
}

func TestNamespaceRetirementResponseLossAndRestartPreserveIdentity(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "host.db")
	database, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", PerEnvPIDs: 512, PerEnvDiskBytes: 8 << 30, MaxEgressMode: 1, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "workload", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 7, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 2 << 30}, store.Environment{CPUMillis: 2000, MemoryBytes: 4 << 30, PIDs: 512, DiskBytes: 8 << 30}.Resources())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "namespace"), []byte("namespace"), 0600); err != nil {
		t.Fatal(err)
	}
	client, closeGuest := startRetirementGuest(t, &retirementGuest{path: root, dropDelete: true})
	controller := &Controller{store: database, egress: egress.New()}
	if err := controller.retireEmptyAllocationWithClient(ctx, client, environment); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost response: %v", err)
	}
	closeGuest()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	recovered, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || recovered.AllocationGeneration != 7 || recovered.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		t.Fatalf("retirement lost identity: %+v err=%v", recovered, err)
	}
	candidates, err := database.ListRetiringAllocations(ctx, recovered.UpdatedAt.Add(time.Nanosecond), 8)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("expired retirement kept retrying after downtime: %+v err=%v", candidates, err)
	}
	recovered, err = database.RequeueAllocationRetirement(ctx, recovered.ID, recovered.PolicyRevision)
	if err != nil {
		t.Fatalf("explicit retirement retry: %v", err)
	}
	candidates, err = database.ListRetiringAllocations(ctx, recovered.UpdatedAt.Add(-time.Nanosecond), 8)
	if err != nil || len(candidates) != 1 || candidates[0].ID != recovered.ID {
		t.Fatalf("explicit retry did not restore bounded retirement: %+v err=%v", candidates, err)
	}
	client, closeGuest = startRetirementGuest(t, &retirementGuest{path: root})
	defer closeGuest()
	controller = &Controller{store: database, egress: egress.New()}
	if err := controller.retireEmptyAllocationWithClient(ctx, client, recovered); err != nil {
		t.Fatalf("deleted namespace retry: %v", err)
	}
	controller.sourcePolicyPublisher = func(context.Context, string) error { return errors.New("publication failed") }
	if err := controller.finalizeAllocationRetirement(ctx, recovered); err == nil {
		t.Fatal("publication failure discarded allocation")
	}
	if _, err := database.GetEnvironment(ctx, recovered.ID); err != nil {
		t.Fatal(err)
	}
	controller.sourcePolicyPublisher = func(context.Context, string) error { return nil }
	if err := controller.finalizeAllocationRetirement(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetEnvironment(ctx, recovered.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("completed retirement remains: %v", err)
	}
	if err := controller.retireRemoteEnvironment(ctx, recovered); err != nil {
		t.Fatalf("registration revocation after concurrent empty allocation retirement: %v", err)
	}
}

func TestNamespaceRetirementKeepsPrivateData(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "private-data"), []byte("owned bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	client, closeGuest := startRetirementGuest(t, &retirementGuest{path: root})
	defer closeGuest()
	controller := &Controller{}
	if err := controller.retireEmptyAllocationWithClient(context.Background(), client, store.Environment{NamespaceID: "namespace"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("nonempty retirement: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, "private-data"))
	if err != nil || string(data) != "owned bytes" {
		t.Fatalf("private data changed: %q err=%v", data, err)
	}
}

func (g *retirementGuest) ListContainers(_ context.Context, r *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	if r.GetPageSize() != 32 || r.GetLimit() != 0 {
		return nil, errors.New("retirement did not request a bounded cursor page")
	}
	if g.listError != nil {
		return nil, g.listError
	}
	page := &vmmdv1.ListContainersResponse{}
	for n := 0; n < g.containerCount; n++ {
		id := fmt.Sprintf("container-%03d", n)
		if id > r.GetAfterContainerId() {
			page.Containers = append(page.Containers, &vmmdv1.Container{Id: id, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING})
			if len(page.Containers) == 32 {
				page.NextCursor = id
				break
			}
		}
	}
	return page, nil
}
func (g *retirementGuest) GetNamespace(_ context.Context, r *vmmdv1.GetNamespaceRequest) (*vmmdv1.GetNamespaceResponse, error) {
	g.namespaceReads.Add(1)
	g.queriedNamespace = r.GetNamespaceId()
	if g.namespaceError != nil {
		return nil, g.namespaceError
	}
	return &vmmdv1.GetNamespaceResponse{Namespace: &vmmdv1.Namespace{Id: r.GetNamespaceId()}}, nil
}

func (g *retirementGuest) InspectContainer(_ context.Context, r *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	return &vmmdv1.InspectContainerResponse{Container: &vmmdv1.Container{Id: r.GetContainerId(), State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}

func (g *retirementGuest) StopContainer(_ context.Context, r *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	g.stops.Add(1)
	if g.stopError != nil {
		return nil, g.stopError
	}
	state := vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	if g.unresolved {
		state = vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
	}
	return &vmmdv1.StopContainerResponse{Container: &vmmdv1.Container{Id: r.GetContainerId(), State: state}}, nil
}

func TestResumeRemoteRevocationsRetiresOnlyInterruptedRevocations(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	contract := resourcev2.Contract{CPUMaxMillis: 1000, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30}
	for _, id := range []string{"ready", "revoking"} {
		_, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
			ID: id, Endpoint: id, DisplayName: id, ProtocolVersion: "2",
			PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 10,
			State:           int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
			ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
			EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1, EnrollmentRequestID: id,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.BeginWorkloadAdmission(ctx, store.Environment{
			ID: id, NamespaceID: id, OwnerKind: store.OwnerRemoteRegistration,
			RegistrationID: id, AllocationID: id, AllocationGeneration: 1,
			AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
			CPUMillis:       1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30,
		}, contract); err != nil {
			t.Fatal(err)
		}
	}
	controller := &Controller{manager: &vmm.Manager{}, store: database, sessions: make(map[string]*sessionServer), allocationRetirementWake: make(chan struct{}, 1)}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle.snapshot = func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "boot"} }
	registration, err := database.GetRemoteRegistration(ctx, "revoking")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginRemoteRevocation(ctx, registration.ID, registration.Revision); err != nil {
		t.Fatal(err)
	}
	var resume revocationResume
	controller.resumeRemoteRevocations(ctx, &resume)
	if resume.scanned {
		t.Fatal("revocations resumed before admission opened")
	}
	generation, _ := controller.vmLifecycle.admissionToken()
	if !controller.vmLifecycle.openAdmission(generation, "boot") {
		t.Fatal("admission did not open")
	}
	// The unavailable Guest fails every retirement, so only a tried registration has a retry.
	controller.resumeRemoteRevocations(ctx, &resume)
	retry, ok := resume.retries["revoking"]
	if !resume.scanned || len(resume.retries) != 1 || !ok {
		t.Fatalf("resumed revocations = %+v", resume)
	}
	controller.resumeRemoteRevocations(ctx, &resume)
	if resume.retries["revoking"] != retry {
		t.Fatalf("revocation retried before its backoff: %+v", resume.retries)
	}
	retry.at = time.Now()
	resume.retries["revoking"] = retry
	controller.resumeRemoteRevocations(ctx, &resume)
	if len(resume.retries) != 1 || resume.retries["revoking"].delay != 2*time.Second {
		t.Fatalf("failed revocation retry = %+v", resume.retries)
	}
}
