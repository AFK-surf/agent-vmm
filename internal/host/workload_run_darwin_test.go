//go:build darwin

package host

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestWorkloadStartsSerializeAndStoppedSandboxesReleaseCapacity(t *testing.T) {
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container), entered: make(chan struct{}), release: make(chan struct{})}
	client := workloadGuestClient(t, guest)
	controller := workloadTestController()
	first := make(chan error, 1)
	entered := guest.entered
	go func() {
		_, err := controller.startWorkloadContainer(context.Background(), client, "env-a", workloadStart("a"))
		first <- err
	}()
	<-entered
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-b", workloadStart("b")); capacityDimensionMetadata(err) != "run_slot" {
		t.Fatalf("parallel start: %v", err)
	}
	close(guest.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "c", "d"} {
		if _, err := controller.startWorkloadContainer(context.Background(), client, "env-"+id, workloadStart(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-e", workloadStart("e")); capacityDimensionMetadata(err) != "run_slot" {
		t.Fatalf("fifth start: %v", err)
	}
	// The stop commits at the Guest, but its reply is lost. No slot is released
	// until the ordinary node observation confirms task teardown.
	guest.mu.Lock()
	guest.loseStop = true
	guest.mu.Unlock()
	unlock := controller.lockEnvironment("env-a")
	_, err := controller.stopWorkloadContainerLocked(context.Background(), client, "env-a", &vmmdv1.StopContainerRequest{RequestId: "stop-a", ActorNamespaceId: "a", ContainerId: "main"})
	unlock()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("lost stop reply: %v", err)
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-e", workloadStart("e")); capacityDimensionMetadata(err) != "run_slot" {
		t.Fatalf("unknown stop released slot: %v", err)
	}
	if err := controller.sampleWorkloadRuns(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-e", workloadStart("e")); err != nil {
		t.Fatal(err)
	}
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.maxStarting != 1 {
		t.Fatalf("concurrent Guest starts=%d", guest.maxStarting)
	}
}

func TestUnknownStartDoesNotReleaseBeforeOriginalDeadline(t *testing.T) {
	controller := workloadTestController()
	now := time.Now()
	run, err := controller.admitWorkloadStart("env-a", "a", "main", now)
	if err != nil {
		t.Fatal(err)
	}
	run.inFlight = false
	run.uncertainStartUntil = now.Add(workloadStartBudget)
	stopped := &vmmdv1.Container{State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
	controller.mu.Lock()
	controller.observeWorkloadRunLocked(run, stopped, now.Add(time.Second))
	if len(controller.workloadRuns) != 1 {
		t.Fatal("an observation overtook an uncertain start and released capacity")
	}
	controller.observeWorkloadRunLocked(run, stopped, now.Add(workloadStartBudget))
	if len(controller.workloadRuns) != 0 {
		t.Fatal("confirmed expired start retained capacity")
	}
	controller.mu.Unlock()
}

func TestConfirmedAbsentContainersReleaseAllRunSlots(t *testing.T) {
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container), notFound: make(map[string]bool)}
	controller := workloadTestController()
	for _, id := range []string{"a", "b", "c", "d"} {
		run := &workloadRun{environmentID: "env-" + id, namespaceID: id, containerID: "main", instanceID: "instance-" + id}
		controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] = run
		guest.notFound[id] = true
	}
	client := workloadGuestClient(t, guest)
	if err := controller.sampleWorkloadRuns(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if len(controller.workloadRuns) != 0 {
		t.Fatalf("absent containers retained %d run slots", len(controller.workloadRuns))
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-e", workloadStart("e")); err != nil {
		t.Fatalf("start after four deleted containers: %v", err)
	}
}

func TestWorkloadMemoryPressureBlocksNewStartsButPreservesRunningInstance(t *testing.T) {
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container)}
	client := workloadGuestClient(t, guest)
	controller := workloadTestController()
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-a", workloadStart("a")); err != nil {
		t.Fatal(err)
	}
	controller.workloadMemoryPressure = true
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-b", workloadStart("b")); capacityDimensionMetadata(err) != "memory_pressure" {
		t.Fatalf("pressure admission: %v", err)
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-a", workloadStart("a")); err != nil {
		t.Fatalf("existing instance: %v", err)
	}
}

func workloadTestController() *Controller {
	return &Controller{workloadRuns: make(map[string]*workloadRun), workloadRunsRecovered: true, workloadPressureAt: time.Now()}
}
func workloadStart(namespace string) *vmmdv1.StartContainerRequest {
	return &vmmdv1.StartContainerRequest{RequestId: "start-" + namespace, ActorNamespaceId: namespace, ContainerId: "main"}
}

func workloadGuestClient(t *testing.T, guest *workloadGuest) vmmdv1.VMMDServiceClient {
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
	mu                    sync.Mutex
	states                map[string]*vmmdv1.Container
	entered, release      chan struct{}
	starting, maxStarting int
	loseStop              bool
	notFound              map[string]bool
}

func (g *workloadGuest) InspectContainer(_ context.Context, r *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.notFound[r.GetActorNamespaceId()] {
		return nil, status.Error(codes.NotFound, "container was deleted")
	}
	c := g.states[r.GetActorNamespaceId()]
	if c == nil {
		c = &vmmdv1.Container{Id: r.GetContainerId(), State: vmmdv1.ContainerState_CONTAINER_STATE_CREATED}
	}
	return &vmmdv1.InspectContainerResponse{Container: proto.Clone(c).(*vmmdv1.Container)}, nil
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
	c := &vmmdv1.Container{Id: r.GetContainerId(), State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
	g.states[r.GetActorNamespaceId()] = c
	if g.loseStop {
		return nil, status.Error(codes.Unavailable, "stop response lost")
	}
	return &vmmdv1.StopContainerResponse{Container: c}, nil
}
func (g *workloadGuest) GetSystemUsage(context.Context, *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	return &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{MemoryBytes: 1 << 30}, WorkloadMemoryLimitBytes: 9 << 30}, nil
}
func (g *workloadGuest) GetNamespaceUsage(context.Context, *vmmdv1.GetNamespaceUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	return &vmmdv1.GetUsageResponse{Usage: &vmmdv1.Usage{}}, nil
}
