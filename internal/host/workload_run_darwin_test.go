//go:build darwin

package host

import (
	"context"
	"fmt"
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

func TestIndependentWorkloadStartsOverlapAndUnknownStopsKeepInstanceFence(t *testing.T) {
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
	_, secondErr := controller.startWorkloadContainer(context.Background(), client, "env-b", workloadStart("b"))
	close(guest.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if secondErr != nil {
		t.Fatalf("independent start: %v", secondErr)
	}
	guest.mu.Lock()
	guest.loseStop = true
	guest.mu.Unlock()
	unlock := controller.lockEnvironment("env-a")
	_, err := controller.stopWorkloadContainerLocked(context.Background(), client, "env-a", &vmmdv1.StopContainerRequest{RequestId: "stop-a", ActorNamespaceId: "a", ContainerId: "main"})
	unlock()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("lost stop reply: %v", err)
	}
	if _, err := controller.admitWorkloadStart("env-a", "a", "main", time.Now()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unknown stop lost lifecycle fence: %v", err)
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-c", workloadStart("c")); err != nil {
		t.Fatalf("unknown stop blocked unrelated workload: %v", err)
	}
	if err := controller.sampleWorkloadRuns(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.startWorkloadContainer(context.Background(), client, "env-a", workloadStart("a")); err != nil {
		t.Fatal(err)
	}
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.maxStarting < 2 {
		t.Fatalf("independent starts did not overlap: %d", guest.maxStarting)
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

func TestPressureAndStaleSamplesDoNotBlockWorkloadStarts(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		guest := &workloadGuest{states: make(map[string]*vmmdv1.Container)}
		client := workloadGuestClient(t, guest)
		controller := workloadTestController()
		controller.workloadPressureAt = time.Time{}
		controller.workloadMemoryPressure = pressure
		for _, id := range []string{"a", "b"} {
			if _, err := controller.startWorkloadContainer(context.Background(), client, "env-"+id, workloadStart(id)); err != nil {
				t.Fatalf("pressure=%v start=%s: %v", pressure, id, err)
			}
		}
	}
}

func TestSixIdleWorkloadsRemainRunnable(t *testing.T) {
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container)}
	client := workloadGuestClient(t, guest)
	controller := workloadTestController()
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		if _, err := controller.startWorkloadContainer(context.Background(), client, "env-"+id, workloadStart(id)); err != nil {
			t.Fatalf("start idle workload %s: %v", id, err)
		}
	}
	if err := controller.sampleWorkloadRuns(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		response, err := controller.startWorkloadContainer(context.Background(), client, "env-"+id, workloadStart(id))
		if err != nil || response.GetContainer().GetInstanceId() != "instance-"+id {
			t.Fatalf("idle workload %s lost its running instance: %v, %v", id, response, err)
		}
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
	mu                        sync.Mutex
	states                    map[string]*vmmdv1.Container
	entered, release          chan struct{}
	starting, maxStarting     int
	loseStop                  bool
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

func TestWorkloadSamplingStillInspectsAndRetriesStopWhenPressureIsUnavailable(t *testing.T) {
	guest := &workloadGuest{
		states:         make(map[string]*vmmdv1.Container),
		inspectHits:    make(map[string]int),
		systemUsageErr: status.Error(codes.Unavailable, "pressure unavailable"),
	}
	controller := workloadTestController()
	run := &workloadRun{
		environmentID: "env-a", namespaceID: "a", containerID: "main", instanceID: "instance-a",
		stopping: true, stopDeadline: time.Now().Add(time.Minute), stopRequestID: "stop-a",
	}
	controller.mu.Lock()
	controller.setWorkloadRunLocked(workloadRunKey(run.environmentID, run.containerID), run)
	controller.mu.Unlock()
	guest.states[run.namespaceID] = &vmmdv1.Container{
		Id: run.containerID, InstanceId: run.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING,
	}

	err := controller.sampleWorkloadRuns(context.Background(), workloadGuestClient(t, guest))
	if err == nil {
		t.Fatal("missing pressure sample was reported as available")
	}
	guest.mu.Lock()
	inspectHits, stopHits := guest.inspectHits[run.namespaceID], guest.stopHits
	guest.mu.Unlock()
	if inspectHits != 1 || stopHits != 1 {
		t.Fatalf("pressure failure skipped exact work: inspect=%d stop=%d", inspectHits, stopHits)
	}
}
func (g *workloadGuest) GetNamespaceUsage(context.Context, *vmmdv1.GetNamespaceUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.usageUnavailable {
		return nil, status.Error(codes.Unavailable, "usage unavailable")
	}
	return &vmmdv1.GetUsageResponse{Usage: &vmmdv1.Usage{SampledAtUnixNano: time.Now().UnixNano()}}, nil
}

func TestWorkloadSamplingBoundsRPCFanoutAndAdvancesCursor(t *testing.T) {
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container), inspectHits: make(map[string]int), inspectDelay: 5 * time.Millisecond}
	controller := workloadTestController()
	for index := range 40 {
		id := fmt.Sprintf("env-%02d", index)
		run := &workloadRun{environmentID: id, namespaceID: id, containerID: "main", instanceID: "instance-" + id}
		controller.workloadRuns[workloadRunKey(id, "main")] = run
		guest.states[id] = &vmmdv1.Container{Id: "main", InstanceId: run.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}
	}
	client := workloadGuestClient(t, guest)
	for round := range 3 {
		if err := controller.sampleWorkloadRuns(context.Background(), client); err != nil {
			t.Fatal(err)
		}
		guest.mu.Lock()
		total := 0
		for _, hits := range guest.inspectHits {
			total += hits
		}
		covered, peak := len(guest.inspectHits), guest.maxInspecting
		guest.mu.Unlock()
		if total != (round+1)*16 || peak > 4 || covered != min((round+1)*16, 40) {
			t.Fatalf("round=%d RPCs=%d peak=%d covered=%d", round, total, peak, covered)
		}
	}
}
