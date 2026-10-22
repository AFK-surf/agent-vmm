//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
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
	controller := workloadTestController(t)
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
	controller := workloadTestController(t)
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
	controller := workloadTestController(t)
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
		controller := workloadTestController(t)
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
	controller := workloadTestController(t)
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

func workloadTestController(t *testing.T) *Controller {
	t.Helper()
	database, err := openTestStore(filepath.Join(t.TempDir(), "workload.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return &Controller{store: database, workloadRuns: make(map[string]*workloadRun), workloadRunsRecovered: true, workloadPressureAt: time.Now()}
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

func TestWorkloadSamplingStillInspectsAndRetriesStopWhenPressureIsUnavailable(t *testing.T) {
	guest := &workloadGuest{
		states:         make(map[string]*vmmdv1.Container),
		inspectHits:    make(map[string]int),
		systemUsageErr: status.Error(codes.Unavailable, "pressure unavailable"),
	}
	controller := workloadTestController(t)
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
	if inspectHits != 2 || stopHits != 1 {
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
	controller := workloadTestController(t)
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

func TestSuccessfulStopCannotSettleDifferentWorkloadRun(t *testing.T) {
	for _, test := range []struct {
		name    string
		replace bool
	}{{name: "different response instance"}, {name: "replaced run", replace: true}} {
		t.Run(test.name, func(t *testing.T) {
			controller := workloadTestController(t)
			run := &workloadRun{environmentID: "env-a", namespaceID: "a", containerID: "main", instanceID: "instance-a"}
			key := workloadRunKey(run.environmentID, run.containerID)
			controller.mu.Lock()
			controller.setWorkloadRunLocked(key, run)
			controller.mu.Unlock()
			current := run
			client := workloadStopResultClient{stop: func(*vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
				instanceID := "different-instance"
				if test.replace {
					instanceID = ""
					current = &workloadRun{environmentID: "env-a", namespaceID: "a", containerID: "main", instanceID: "replacement"}
					controller.mu.Lock()
					controller.setWorkloadRunLocked(key, current)
					controller.mu.Unlock()
				}
				return &vmmdv1.StopContainerResponse{Container: &vmmdv1.Container{Id: "main", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED, InstanceId: instanceID}}, nil
			}}
			unlock := controller.lockEnvironment("env-a")
			_, err := controller.stopWorkloadContainerLocked(context.Background(), client, "env-a", &vmmdv1.StopContainerRequest{RequestId: "stop", ActorNamespaceId: "a", ContainerId: "main"})
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			controller.mu.Lock()
			defer controller.mu.Unlock()
			if controller.workloadRuns[key] != current {
				t.Fatal("Stop settled a different current run")
			}
			if len(controller.stoppedWorkloadInstances) != 0 {
				t.Fatal("Stop recorded evidence for a different current run")
			}
		})
	}
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

func TestStopReceiptIsBoundToCapturedWorkloadInstance(t *testing.T) {
	guest := &cachedWorkloadStopGuest{workloadGuest: workloadGuest{states: make(map[string]*vmmdv1.Container), omitStoppedInstance: true}, receipts: make(map[string]*vmmdv1.StopContainerResponse)}
	client := workloadGuestClient(t, guest)
	controller := workloadTestController(t)
	request := &vmmdv1.StopContainerRequest{RequestId: "remote-stop-main", ActorNamespaceId: "a", ContainerId: "main"}
	key := workloadRunKey("env-a", "main")
	for index, instance := range []string{"instance-a", "instance-a", "instance-b"} {
		if index != 1 {
			guest.mu.Lock()
			guest.states["a"] = &vmmdv1.Container{Id: "main", NamespaceId: "a", InstanceId: instance, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}
			guest.mu.Unlock()
		}
		run := &workloadRun{environmentID: "env-a", namespaceID: "a", containerID: "main", instanceID: instance, executions: map[string]*workloadExecution{"exec": {}}}
		controller.mu.Lock()
		controller.setWorkloadRunLocked(key, run)
		controller.mu.Unlock()
		unlock := controller.lockEnvironment("env-a")
		response, err := controller.stopWorkloadContainerLocked(context.Background(), client, "env-a", request)
		unlock()
		if err != nil || response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			t.Fatalf("stop %d: response=%v err=%v", index, response, err)
		}
		guest.mu.Lock()
		state, hits := guest.states["a"].GetState(), guest.stopHits
		guest.mu.Unlock()
		expectedHits := 1
		if index == 2 {
			expectedHits = 2
		}
		if state != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED || hits != expectedHits {
			t.Fatalf("stop %d guest state=%v actual stops=%d want=%d", index, state, hits, expectedHits)
		}
		controller.mu.Lock()
		retained, evidence := controller.workloadRuns[key], controller.stoppedWorkloadInstances[key]
		controller.mu.Unlock()
		if retained != nil || evidence != instance {
			t.Fatalf("stop %d retained=%v evidence=%q want=%q", index, retained, evidence, instance)
		}
		if request.GetRequestId() != "remote-stop-main" || run.stopRequestID != "remote-stop-main" {
			t.Fatal("instance binding changed caller recovery request identity")
		}
	}
}

func TestStopRetryPreservesOriginalRequestFingerprint(t *testing.T) {
	for name, timeout := range map[string]*uint32{"omitted": nil, "immediate": optional(uint32(0)), "graceful": optional(uint32(17))} {
		for _, quiesced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/quiesced=%t", name, quiesced), func(t *testing.T) {
				controller := workloadTestController(t)
				run := &workloadRun{environmentID: "env-a", namespaceID: "a", containerID: "main", instanceID: "instance-a"}
				if quiesced {
					run.quiescing = true
					run.stopDeadline = time.Now().Add(time.Minute)
				}
				controller.setWorkloadRunLocked(workloadRunKey(run.environmentID, run.containerID), run)
				var original []byte
				calls := 0
				client := workloadStopResultClient{stop: func(request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
					calls++
					fingerprint := runtimeRequestHash(request)
					if calls == 1 {
						original = fingerprint
						return nil, status.Error(codes.Canceled, "first stop canceled")
					}
					if !bytes.Equal(fingerprint, original) {
						t.Fatal("retry changed request fingerprint")
					}
					return &vmmdv1.StopContainerResponse{Container: &vmmdv1.Container{Id: "main", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}}, nil
				}}
				request := &vmmdv1.StopContainerRequest{RequestId: "original-stop", ActorNamespaceId: "a", ContainerId: "main", TimeoutSeconds: timeout}
				unlock := controller.lockEnvironment("env-a")
				_, err := controller.stopWorkloadContainerLocked(t.Context(), client, "env-a", request)
				unlock()
				if status.Code(err) != codes.Canceled {
					t.Fatal(err)
				}
				controller.retryWorkloadStop(t.Context(), client, run)
				if calls != 2 || controller.workloadRuns[workloadRunKey("env-a", "main")] != nil {
					t.Fatalf("calls=%d retry did not settle", calls)
				}
			})
		}
	}
}
