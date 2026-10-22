//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type domainCleanupGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	mu                          sync.Mutex
	namespaces                  map[string]bool
	containers                  map[string]*vmmdv1.Container
	volumes                     map[string]*vmmdv1.Volume
	failFirstDelete, loseDelete bool
	deleted                     []string
	database                    *sql.DB
}

func (guest *domainCleanupGuest) ListContainers(_ context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if !guest.namespaces[request.GetActorNamespaceId()] {
		return nil, status.Error(codes.NotFound, "namespace absent")
	}
	response := &vmmdv1.ListContainersResponse{}
	if container := guest.containers[request.GetActorNamespaceId()]; container != nil {
		response.Containers = append(response.Containers, container)
	}
	return response, nil
}

func (guest *domainCleanupGuest) ListVolumes(_ context.Context, request *vmmdv1.ListVolumesRequest) (*vmmdv1.ListVolumesResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	response := &vmmdv1.ListVolumesResponse{}
	if volume := guest.volumes[request.GetActorNamespaceId()]; volume != nil {
		response.Volumes = append(response.Volumes, volume)
	}
	return response, nil
}

func (guest *domainCleanupGuest) ListImages(_ context.Context, request *vmmdv1.ListImagesRequest) (*vmmdv1.ListImagesResponse, error) {
	return &vmmdv1.ListImagesResponse{Images: []*vmmdv1.Image{{Reference: request.GetActorNamespaceId() + "/image", GenerationId: "image-generation", Digest: "sha256:image"}}}, nil
}

func (guest *domainCleanupGuest) GetNamespace(_ context.Context, request *vmmdv1.GetNamespaceRequest) (*vmmdv1.GetNamespaceResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if !guest.namespaces[request.GetNamespaceId()] {
		return nil, status.Error(codes.NotFound, "namespace absent")
	}
	return &vmmdv1.GetNamespaceResponse{Namespace: &vmmdv1.Namespace{Id: request.GetNamespaceId()}}, nil
}

func (guest *domainCleanupGuest) InspectContainer(_ context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	return &vmmdv1.InspectContainerResponse{Container: guest.containers[request.GetActorNamespaceId()]}, nil
}

func (guest *domainCleanupGuest) DeleteContainer(ctx context.Context, request *vmmdv1.DeleteContainerRequest) (*vmmdv1.DeleteContainerResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	namespace := request.GetActorNamespaceId()
	var accepted int
	if err := guest.database.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE environment_id=? AND state=?`, namespace, hostv1.OperationState_OPERATION_STATE_RUNNING).Scan(&accepted); err != nil || accepted != 1 {
		return nil, status.Errorf(codes.Internal, "cleanup must be durably accepted before deleting: count=%d err=%v", accepted, err)
	}
	container := guest.containers[namespace]
	if container == nil || request.GetContainerId() != container.GetId() || request.GetExpectedGenerationId() != container.GetGenerationId() {
		return nil, status.Error(codes.Aborted, "wrong container generation")
	}
	if guest.failFirstDelete {
		guest.failFirstDelete = false
		return nil, status.Error(codes.Unavailable, "first child failed before mutation")
	}
	delete(guest.containers, namespace)
	guest.deleted = append(guest.deleted, namespace+"/container")
	return &vmmdv1.DeleteContainerResponse{}, nil
}

func (guest *domainCleanupGuest) DeleteVolume(_ context.Context, request *vmmdv1.DeleteVolumeRequest) (*vmmdv1.DeleteVolumeResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	namespace := request.GetActorNamespaceId()
	volume := guest.volumes[namespace]
	if volume == nil || request.GetVolumeId() != volume.GetId() || request.GetExpectedGenerationId() != volume.GetGenerationId() {
		return nil, status.Error(codes.Aborted, "wrong volume generation")
	}
	delete(guest.volumes, namespace)
	guest.deleted = append(guest.deleted, namespace+"/volume")
	return &vmmdv1.DeleteVolumeResponse{}, nil
}

func (guest *domainCleanupGuest) DeleteNamespace(_ context.Context, request *vmmdv1.DeleteNamespaceRequest) (*vmmdv1.DeleteNamespaceResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	namespace := request.GetNamespaceId()
	if guest.containers[namespace] != nil || guest.volumes[namespace] != nil {
		return nil, status.Error(codes.FailedPrecondition, "namespace is not empty")
	}
	delete(guest.namespaces, namespace)
	guest.deleted = append(guest.deleted, namespace+"/namespace")
	if guest.loseDelete {
		guest.loseDelete = false
		return nil, status.Error(codes.Unavailable, "namespace deleted but response lost")
	}
	return &vmmdv1.DeleteNamespaceResponse{}, nil
}

func TestApplianceDomainCleanupRunsHeldEntryAndRecoversOriginalParent(t *testing.T) {
	for _, recoverFailures := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%t", recoverFailures), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "host.db")
			controller, installation := updateDrainFixtureAt(t, path)
			guestDB, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer guestDB.Close()
			guestDB.SetMaxOpenConns(1)
			guest := &domainCleanupGuest{namespaces: map[string]bool{}, containers: map[string]*vmmdv1.Container{}, volumes: map[string]*vmmdv1.Volume{}, database: guestDB, failFirstDelete: recoverFailures, loseDelete: recoverFailures}
			for index := range 3 {
				id := fmt.Sprintf("environment-%d", index)
				if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: id, NamespaceID: id, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: installation.RegistrationID, AllocationID: id, AllocationGeneration: 1, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
					t.Fatal(err)
				}
				guest.namespaces[id] = true
				guest.containers[id] = &vmmdv1.Container{Id: id + "-container", GenerationId: "container-generation", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
				guest.volumes[id] = &vmmdv1.Volume{Id: id + "-volume", GenerationId: "volume-generation"}
			}
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer()
			vmmdv1.RegisterVMMDServiceServer(grpcServer, guest)
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()
			defer listener.Close()
			connection, err := grpc.NewClient("passthrough:///remove-domain", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := vmmdv1.NewVMMDServiceClient(connection)
			published := 0
			configure := func() {
				controller.manager = &vmm.Manager{}
				controller.vmLifecycle = &VMLifecycleOwner{}
				controller.sourcePolicyPublisher = func(context.Context, string) error { published++; return nil }
			}
			configure()
			server := &applianceAdminServer{controller: controller}
			request := &appliancev1.ExecuteRemoveRequest{InstallationId: installation.ID, RequestId: "remove-domain", ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ConfirmedResourceIds: []string{installation.DataVolumeID}}
			parent, err := callAsOperator(t, func(ctx context.Context) (store.ApplianceOperation, error) {
				return server.acceptApplianceRemove(ctx, request)
			})
			if err != nil {
				t.Fatal(err)
			}
			readChildren := func() []store.Operation {
				t.Helper()
				rows, err := guestDB.QueryContext(ctx, `SELECT request_id FROM operations WHERE kind=? ORDER BY started_at, id`, "environment-CLEANUP_KIND_DISCARD")
				if err != nil {
					t.Fatal(err)
				}
				var requestIDs []string
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						t.Fatal(err)
					}
					requestIDs = append(requestIDs, id)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				var operations []store.Operation
				for _, id := range requestIDs {
					operation, err := controller.store.OperationByRequestID(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					operations = append(operations, operation)
				}
				return operations
			}
			resume := func() {
				t.Helper()
				if err := controller.store.Close(); err != nil {
					t.Fatal(err)
				}
				database, err := store.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				controller = &Controller{store: database}
				t.Cleanup(func() { database.Close() })
				configure()
				server = &applianceAdminServer{controller: controller}
				recovered, err := server.acceptApplianceRemove(ctx, request)
				if err != nil || recovered.ID != parent.ID || recovered.RequestID != parent.RequestID || recovered.ExpectedRevision != parent.ExpectedRevision || recovered.DeadlineUnixMillis != parent.DeadlineUnixMillis || string(recovered.Receipt) != string(parent.Receipt) || recovered.State != parent.State || recovered.Phase != parent.Phase {
					t.Fatalf("parent changed across reopen: %+v err=%v", recovered, err)
				}
				parent = recovered
			}
			// The caller already owns the VM lifecycle, inventory transition, and installation.
			runDomain := func() error {
				controller.vmLifecycle.operationMu.Lock()
				defer controller.vmLifecycle.operationMu.Unlock()
				releaseInventory, err := controller.lockInventoryTransition(ctx)
				if err != nil {
					return err
				}
				defer releaseInventory()
				unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
				defer unlock()
				return server.discardApplianceDomainWithClient(ctx, client, parent, installation)
			}
			if recoverFailures {
				operator := &operatorServer{controller: controller}
				before, err := operator.planCleanupWithClient(ctx, client, "environment-0", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, true)
				if err != nil {
					t.Fatal(err)
				}
				if err := runDomain(); status.Code(err) != codes.Unavailable {
					t.Fatalf("first child failure: %v", err)
				}
				children := readChildren()
				if len(children) != 1 || children[0].State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) || children[0].EnvironmentID != "environment-0" {
					t.Fatalf("first failed child: %+v", children)
				}
				after, err := operator.planCleanupWithClient(ctx, client, "environment-0", hostv1.CleanupKind_CLEANUP_KIND_DISCARD, true)
				if err != nil || before.Revision != after.Revision {
					t.Fatalf("first failure changed inventory: before=%v after=%v err=%v", before, after, err)
				}
				resume()
				if err := runDomain(); status.Code(err) != codes.Unavailable {
					t.Fatalf("lost namespace response: %v", err)
				}
				children = readChildren()
				if len(children) != 2 || children[1].State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) || children[0].RequestID == children[1].RequestID {
					t.Fatalf("fresh attributable retry: %+v", children)
				}
				if _, err := controller.store.GetEnvironment(ctx, "environment-0"); err != nil {
					t.Fatalf("lost ACK was treated as completed: %v", err)
				}
				resume()
			}
			if err := runDomain(); err != nil {
				t.Fatal(err)
			}
			children := readChildren()
			wantChildren, wantPublished := 3, 3
			if recoverFailures {
				wantChildren, wantPublished = 5, 2
			}
			if len(children) != wantChildren || published != wantPublished {
				t.Fatalf("children=%+v published=%d", children, published)
			}
			seen := map[string]bool{}
			for index, child := range children {
				if !strings.HasPrefix(child.RequestID, "remove-"+parent.ID+"-"+child.EnvironmentID+"-") || seen[child.RequestID] {
					t.Fatalf("unattributable child: %+v", child)
				}
				seen[child.RequestID] = true
				want := hostv1.OperationState_OPERATION_STATE_SUCCEEDED
				if recoverFailures && index < 2 {
					want = hostv1.OperationState_OPERATION_STATE_FAILED
				}
				if child.State != int32(want) || want == hostv1.OperationState_OPERATION_STATE_FAILED && child.ErrorReason != "CLEANUP_FAILED" {
					t.Fatalf("historical receipt changed: %+v", child)
				}
			}
			if environments, err := controller.store.ListRemoteEnvironments(ctx, installation.RegistrationID); err != nil || len(environments) != 0 {
				t.Fatalf("remaining domain: %+v err=%v", environments, err)
			}
			guest.mu.Lock()
			if len(guest.namespaces) != 0 || len(guest.containers) != 0 || len(guest.volumes) != 0 || len(guest.deleted) != 9 {
				t.Fatalf("Guest data remains: namespaces=%v containers=%v volumes=%v deleted=%v", guest.namespaces, guest.containers, guest.volumes, guest.deleted)
			}
			guest.mu.Unlock()
			if err := runDomain(); err != nil || len(readChildren()) != wantChildren {
				t.Fatalf("successful whole-domain ACK replay created children: %v", err)
			}
			current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := controller.store.CommitApplianceRemove(ctx, store.ApplianceRemove{InstallationID: installation.ID, RegistrationID: installation.RegistrationID, OperationID: parent.ID, CommitRevision: current.Revision})
			if err != nil || committed.ID != parent.ID || committed.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED) {
				t.Fatalf("empty-domain commit: %+v err=%v", committed, err)
			}
			if _, err := controller.store.GetEnvironment(ctx, "environment-0"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("environment reappeared: %v", err)
			}
		})
	}
}
