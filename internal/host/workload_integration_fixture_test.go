//go:build darwin || linux

package host

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func workloadTestController(t *testing.T) *Controller {
	t.Helper()
	database, err := openTestStore(filepath.Join(t.TempDir(), "workload.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return newTestControllerWithWorkloads(t, &Controller{store: database}, testWorkloadState{workloadRuns: make(map[string]*testWorkloadRun), workloadRunsRecovered: true, workloadPressureAt: time.Now()})
}
func workloadStart(namespace string) *vmmdv1.StartContainerRequest {
	return &vmmdv1.StartContainerRequest{RequestId: "start-" + namespace, ActorNamespaceId: namespace, ContainerId: "main"}
}

func workloadGuestClient(t *testing.T, guest vmmdv1.VMMDServiceServer) vmmdv1.VMMDServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///guest", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return vmmdv1.NewVMMDServiceClient(connection)
}

type workloadGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	mu                        sync.Mutex
	states                    map[string]*vmmdv1.Container
	entered, release          chan struct{}
	starting, maxStarting     int
	loseStop                  bool
	omitStoppedInstance       bool
	usageUnavailable          bool
	systemUsageErr            error
	notFound                  map[string]bool
	pool                      *vmmdv1.GetUsageResponse
	inspectHits               map[string]int
	inspectDelay              time.Duration
	inspecting, maxInspecting int
	stopHits                  int
}

func (g *workloadGuest) InspectContainer(ctx context.Context, r *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	g.mu.Lock()
	if g.inspectHits != nil {
		g.inspectHits[r.GetActorNamespaceId()]++
	}
	g.inspecting++
	g.maxInspecting = max(g.maxInspecting, g.inspecting)
	delay := g.inspectDelay
	g.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inspecting--
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if g.notFound[r.GetActorNamespaceId()] {
		return nil, status.Error(codes.NotFound, "container was deleted")
	}
	c := g.states[r.GetActorNamespaceId()]
	if c == nil {
		c = &vmmdv1.Container{Id: r.GetContainerId(), State: vmmdv1.ContainerState_CONTAINER_STATE_CREATED}
	}
	return &vmmdv1.InspectContainerResponse{Container: proto.Clone(c).(*vmmdv1.Container)}, nil
}
func (g *workloadGuest) ListContainers(_ context.Context, r *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	container := g.states[r.GetActorNamespaceId()]
	if container == nil {
		return &vmmdv1.ListContainersResponse{}, nil
	}
	return &vmmdv1.ListContainersResponse{Containers: []*vmmdv1.Container{proto.Clone(container).(*vmmdv1.Container)}}, nil
}
func (g *workloadGuest) StartContainer(ctx context.Context, r *vmmdv1.StartContainerRequest) (*vmmdv1.StartContainerResponse, error) {
	g.mu.Lock()
	g.starting++
	g.maxStarting = max(g.maxStarting, g.starting)
	entered, release := g.entered, g.release
	g.entered = nil
	g.mu.Unlock()
	if entered != nil {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.starting--
	c := &vmmdv1.Container{Id: r.GetContainerId(), NamespaceId: r.GetActorNamespaceId(), InstanceId: "instance-" + r.GetActorNamespaceId(), State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}
	g.states[r.GetActorNamespaceId()] = c
	return &vmmdv1.StartContainerResponse{Container: proto.Clone(c).(*vmmdv1.Container)}, nil
}
func (g *workloadGuest) StopContainer(_ context.Context, r *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopHits++
	instanceID := ""
	if current := g.states[r.GetActorNamespaceId()]; current != nil {
		instanceID = current.GetInstanceId()
	}
	if g.omitStoppedInstance {
		instanceID = ""
	}
	c := &vmmdv1.Container{Id: r.GetContainerId(), InstanceId: instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
	g.states[r.GetActorNamespaceId()] = c
	if g.loseStop {
		return nil, status.Error(codes.Unavailable, "stop response lost")
	}
	return &vmmdv1.StopContainerResponse{Container: c}, nil
}
func (g *workloadGuest) GetSystemUsage(context.Context, *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	if g.systemUsageErr != nil {
		return nil, g.systemUsageErr
	}
	if g.pool != nil {
		return proto.Clone(g.pool).(*vmmdv1.GetUsageResponse), nil
	}
	return &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{MemoryBytes: 1 << 30}, WorkloadMemoryLimitBytes: 9 << 30}, nil
}

func (g *workloadGuest) GetNamespaceUsage(context.Context, *vmmdv1.GetNamespaceUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.usageUnavailable {
		return nil, status.Error(codes.Unavailable, "usage unavailable")
	}
	return &vmmdv1.GetUsageResponse{Usage: &vmmdv1.Usage{SampledAtUnixNano: time.Now().UnixNano()}}, nil
}

type workloadStopResultClient struct {
	vmmdv1.VMMDServiceClient
	stop func(*vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error)
}

func (client workloadStopResultClient) InspectContainer(context.Context, *vmmdv1.InspectContainerRequest, ...grpc.CallOption) (*vmmdv1.InspectContainerResponse, error) {
	return &vmmdv1.InspectContainerResponse{Container: &vmmdv1.Container{Id: "main", GenerationId: "generation-a", InstanceId: "instance-a", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}

func (client workloadStopResultClient) StopContainer(_ context.Context, request *vmmdv1.StopContainerRequest, _ ...grpc.CallOption) (*vmmdv1.StopContainerResponse, error) {
	return client.stop(request)
}

type cachedWorkloadStopGuest struct {
	workloadGuest
	receipts map[string]*vmmdv1.StopContainerResponse
}

func (guest *cachedWorkloadStopGuest) StopContainer(ctx context.Context, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	guest.mu.Lock()
	receipt := guest.receipts[request.GetRequestId()]
	guest.mu.Unlock()
	if receipt != nil {
		return proto.Clone(receipt).(*vmmdv1.StopContainerResponse), nil
	}
	receipt, err := guest.workloadGuest.StopContainer(ctx, request)
	if err == nil {
		guest.mu.Lock()
		guest.receipts[request.GetRequestId()] = proto.Clone(receipt).(*vmmdv1.StopContainerResponse)
		guest.mu.Unlock()
	}
	return receipt, err
}
