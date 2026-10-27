//go:build darwin

package host

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type hostShareConsumersGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	containers       []*vmmdv1.Container
	failStop         bool
	stopped          []string
	pages            []*vmmdv1.ListContainersRequest
	projections      []*vmmdv1.ApplyHostSharesRequest
	failProjection   bool
	unavailableShare string
}

func (guest *hostShareConsumersGuest) ApplyHostShares(_ context.Context, request *vmmdv1.ApplyHostSharesRequest) (*vmmdv1.ApplyHostSharesResponse, error) {
	guest.projections = append(guest.projections, proto.Clone(request).(*vmmdv1.ApplyHostSharesRequest))
	if guest.failProjection {
		return nil, status.Error(codes.Unavailable, "projection response was lost")
	}
	response := &vmmdv1.ApplyHostSharesResponse{}
	for _, share := range request.GetShares() {
		if share.GetShareId() != guest.unavailableShare {
			response.AppliedShareIds = append(response.AppliedShareIds, share.GetShareId())
		}
	}
	return response, nil
}

func (guest *hostShareConsumersGuest) ListContainers(_ context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	if request.GetPageSize() == 0 || request.GetPageSize() > vmmdv1.MaxContainerPageSize || len(request.GetAfterContainerId()) > 200 {
		return nil, status.Error(codes.InvalidArgument, "container page exceeds its read budget")
	}
	guest.pages = append(guest.pages, proto.Clone(request).(*vmmdv1.ListContainersRequest))
	ordered := append([]*vmmdv1.Container(nil), guest.containers...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].GetId() < ordered[j].GetId() })
	response := &vmmdv1.ListContainersResponse{}
	for _, container := range ordered {
		if container.GetId() <= request.GetAfterContainerId() {
			continue
		}
		if len(response.Containers) == int(request.GetPageSize()) {
			response.NextCursor = response.Containers[len(response.Containers)-1].GetId()
			break
		}
		response.Containers = append(response.Containers, proto.Clone(container).(*vmmdv1.Container))
	}
	return response, nil
}
func (guest *hostShareConsumersGuest) InspectContainer(_ context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	for _, value := range guest.containers {
		if value.GetId() == request.GetContainerId() {
			return &vmmdv1.InspectContainerResponse{Container: proto.Clone(value).(*vmmdv1.Container)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "missing")
}
func (guest *hostShareConsumersGuest) StopContainer(_ context.Context, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	if guest.failStop {
		return nil, status.Error(codes.Unavailable, "Guest could not confirm writer exit")
	}
	for _, value := range guest.containers {
		if value.GetId() == request.GetContainerId() {
			guest.stopped = append(guest.stopped, value.GetId())
			value.State = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
			return &vmmdv1.StopContainerResponse{Container: proto.Clone(value).(*vmmdv1.Container)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func TestHostShareSourceLossWaitsForEveryConsumerAndRequiresNextBoot(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "owner", EnvironmentID: "service", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"}); err != nil {
		t.Fatal(err)
	}
	source := t.TempDir()
	controller := newTestController(&Controller{store: database})
	setupTestHostShareOwner(controller, nil)
	created, err := controller.hostShares.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "create", EnvironmentId: "service", SourcePath: source})
	if err != nil {
		t.Fatal(err)
	}
	value, err := database.GetHostShare(ctx, created.GetShare().GetId())
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source, source+".original"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(source + ".original")
	guest := &hostShareConsumersGuest{failStop: true}
	ids := []string{"writer", "reader", "peer"}
	for i := 0; i < 65; i++ {
		ids = append(ids, fmt.Sprintf("consumer-%03d", i))
	}
	consumers := []string{}
	for _, id := range ids {
		share := value.ID
		if id == "peer" || strings.HasPrefix(id, "consumer-") && id != "consumer-031" && id != "consumer-032" && id != "consumer-064" {
			share = "unrelated"
		} else {
			consumers = append(consumers, id)
		}
		guest.containers = append(guest.containers, &vmmdv1.Container{Id: id, GenerationId: "generation", InstanceId: id, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, Volumes: []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: share}, ReadOnly: id == "reader"}}})
		if err := database.SetManagedContainerRunning(ctx, "service", id, "generation", true); err != nil {
			t.Fatal(err)
		}
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///guest", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := vmmdv1.NewVMMDServiceClient(connection)
	if err := controller.hostShares.ReconcileHostShares(ctx, client); err == nil {
		t.Fatalf("unconfirmed writer completed suspension: %v", err)
	}
	current, err := database.GetHostShare(ctx, value.ID)
	if err != nil || current.State != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE || !strings.Contains(current.UnavailableReason, "stop the VM") {
		t.Fatal(current, err)
	}
	guest.failStop = false
	guest.pages = nil
	if err := controller.hostShares.ReconcileHostShares(ctx, client); err != nil {
		t.Fatal(err)
	}
	sort.Strings(consumers)
	if !slices.Equal(guest.stopped, consumers) || guest.containers[2].GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("consumer settlement=%v want=%v peer=%v", guest.stopped, consumers, guest.containers[2])
	}
	if len(guest.pages) != 3 || guest.pages[0].GetAfterContainerId() != "" || guest.pages[1].GetAfterContainerId() != "consumer-031" || guest.pages[2].GetAfterContainerId() != "consumer-063" {
		t.Fatalf("consumer scan did not cover all three bounded pages: %v", guest.pages)
	}
	for _, page := range guest.pages {
		if page.GetPageSize() != vmmdv1.MaxContainerPageSize || page.GetActorNamespaceId() != "service" {
			t.Fatalf("wrong page scope: %v", page)
		}
	}
	if err := os.Rename(source+".original", source); err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.ReconcileHostShares(ctx, client); err != nil {
		t.Fatal(err)
	}
	current, err = database.GetHostShare(ctx, value.ID)
	if err != nil || current.State != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE || len(guest.projections[len(guest.projections)-1].Shares) != 0 {
		t.Fatalf("source returned without a new boot: %+v %v", current, err)
	}
	for _, id := range ids {
		managed, err := database.GetManagedContainer(ctx, "service", id)
		if err != nil || !managed.DesiredRunning {
			t.Fatalf("storage suspension changed desired state: %+v %v", managed, err)
		}
	}
}

func TestHostShareRebindRecoveryUsesFrozenCandidateAfterReopen(t *testing.T) {
	for _, mode := range []string{"running", "interrupted-current", "interrupted-replay", "newer-revoke", "environment-removed", "failed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { database.Close() }()
			if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
				t.Fatal(err)
			}
			owner, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "owner", EnvironmentID: "service", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"})
			if err != nil {
				t.Fatal(err)
			}
			controller := newTestController(&Controller{store: database})
			operator := &operatorServer{controller: controller}
			source := t.TempDir()
			created, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CreateHostShareResponse, error) {
				return operator.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "create", EnvironmentId: "service", SourcePath: source, ReadOnly: true})
			})
			if err != nil {
				t.Fatal(err)
			}
			current, err := database.GetHostShare(ctx, created.GetShare().GetId())
			if err != nil {
				t.Fatal(err)
			}
			request := &hostv1.RebindHostShareRequest{RequestId: "rebind", ShareId: current.ID, ExpectedRevision: current.Revision, SourcePath: source, ReadOnly: false}
			candidate := current
			candidate.Revision++
			candidate.ReadOnly = false
			candidate.State = hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE
			body, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			operation := store.Operation{ID: "pending-rebind", RequestID: request.RequestId, EnvironmentID: "service", Kind: "host-share-rebind", State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "accepted", RequestSHA256: runtimeRequestHash(request), Receipt: body}
			if _, err := database.BeginHostShareMutation(ctx, operation, candidate, current.Revision); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "interrupted-current", "interrupted-replay":
				operation.State = int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED)
				operation.ErrorReason = "SIDECAR_RESTARTED"
				if err := database.UpdateOperation(ctx, operation); err != nil {
					t.Fatal(err)
				}
				current, err = database.GetHostShare(ctx, current.ID)
				if err != nil {
					t.Fatal(err)
				}
				current.State = hostv1.HostShareState_HOST_SHARE_STATE_REVOKED
				current.UnavailableReason = "container page exceeds its read budget"
				if err := database.UpdateHostShare(ctx, current, current.Revision); err != nil {
					t.Fatal(err)
				}
			case "newer-revoke":
				revoked := candidate
				revoked.Revision++
				revoked.State = hostv1.HostShareState_HOST_SHARE_STATE_REVOKED
				body, err = json.Marshal(revoked)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := database.BeginHostShareMutation(ctx, store.Operation{ID: "new-revoke", RequestID: "new-revoke", EnvironmentID: "service", Kind: "host-share-revoke", State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Receipt: body}, revoked, candidate.Revision); err != nil {
					t.Fatal(err)
				}
			case "environment-removed":
				if _, err := database.BeginAgentIntegrationRemoval(ctx, "remove", owner.ID, owner.Revision); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
				t.Fatal(err)
			}
			controller = newTestController(&Controller{store: database})
			operator = &operatorServer{controller: controller}
			if mode == "failed" {
				controller.manager, err = vmm.NewManager(vmm.Config{CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30, BundleDirectory: t.TempDir(), InstanceDirectory: t.TempDir(), SerialLog: filepath.Join(t.TempDir(), "serial")})
				if err != nil {
					t.Fatal(err)
				}
				controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
				controller.manager.FailRecovery(fmt.Errorf("previous boot failed"))
				shares := setupTestHostShareOwner(controller, nil)
				if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
					t.Fatal(err)
				}
				if len(shares.directories) != 1 {
					t.Fatal("FAILED recovery omitted pending static share")
				}
			}
			if mode != "interrupted-replay" {
				if err := controller.hostShares.ReconcileHostShares(ctx, nil); err != nil {
					t.Fatal(err)
				}
			}
			response, err := callAsOperator(t, func(ctx context.Context) (*hostv1.RebindHostShareResponse, error) {
				return operator.RebindHostShare(ctx, request)
			})
			if mode == "newer-revoke" || mode == "environment-removed" {
				if status.Code(err) != codes.Aborted {
					t.Fatalf("old rebind crossed newer authority: %v %v", response, err)
				}
				return
			}
			if err != nil || response.GetShare().GetReadOnly() || response.GetShare().GetRevision() != candidate.Revision || response.GetShare().GetState() != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE {
				t.Fatalf("original RW candidate lost: %v %v", response, err)
			}
			persisted, err := database.GetHostShare(ctx, candidate.ID)
			if err != nil || persisted.ReadOnly || persisted.Source != candidate.Source || persisted.OperationID != operation.ID || (mode != "failed" && persisted.UnavailableReason != "VM is stopped") {
				t.Fatalf("wrong recovered authority: %+v %v", persisted, err)
			}
			completed, err := database.GetOperation(ctx, operation.ID)
			if err != nil || completed.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) || completed.ErrorReason != "" {
				t.Fatalf("original operation did not settle: %+v %v", completed, err)
			}
		})
	}
}
