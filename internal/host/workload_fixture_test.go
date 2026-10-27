//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/hostshare"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/testfixture"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	keychainfixture "github.com/AFK-surf/agent-vmm/internal/keychain/testfixture"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
)

type testWorkloadExecution struct{}
type testWorkloadRun struct {
	environmentID, namespaceID, containerID, instanceID, registrationID string
	starting, stopping, inFlight                                        bool
	executions                                                          map[string]*testWorkloadExecution
}
type testWorkloadState struct {
	workloadRuns          map[string]*testWorkloadRun
	workloadRunsRecovered bool
	workloadPressureAt    time.Time
	observed              map[string][]*hostv1.Container
}

var testWorkloadStates sync.Map

func newTestController(controller *Controller) *Controller {
	if controller.credentials == nil {
		controller.credentials = remoteconnector.NewKeychainCredentialStore(keychainfixture.NewPasswords())
	}
	if controller.trust == nil && controller.store != nil {
		var err error
		controller.trustKeyProvider = testfixture.NewMemoryKeys()
		controller.trust, err = hosttrust.New(context.Background(), controller.store, controller.trustKeyProvider)
		if err != nil {
			panic(err)
		}
	}
	if controller.egress == nil {
		controller.egress = egress.New()
	}
	if controller.appliances == nil {
		controller.appliances = assembleAppliances(controller, nil)
	}
	if controller.services == nil {
		controller.services = assembleServices(controller)
	}
	if controller.hostShares == nil {
		var manager hostshare.Manager
		if controller.manager != nil {
			manager = controller.manager
		}
		controller.hostShares = hostshare.New(hostshare.Config{Store: controller.store, Guest: &controller.guestConnection, Manager: manager, Inventory: &controller.inventory})
	}
	if controller.workloads == nil {
		controller.workloads = assembleWorkloads(controller)
	}
	controller.services.BindWorkloads(controller.workloads)
	controller.hostShares.BindWorkloads(controller.workloads)
	if controller.sessions == nil {
		controller.sessions = assembleSessions(controller)
	}
	if controller.remote == nil {
		controller.remote = assembleRemote(controller, remoteWorkloadCapacity(0))
	}
	if controller.cleanup == nil {
		controller.cleanup = assembleCleanup(controller)
	}
	controller.remote.BindCleanup(controller.cleanup)
	controller.appliances.BindRuntime(controller.services, controller.sessions, controller.workloads)
	controller.appliances.BindRemote(controller.remote)
	controller.appliances.BindCleanup(controller.cleanup)
	if controller.vmLifecycle != nil {
		controller.appliances.BindLifecycle(controller.vmLifecycle)
	}
	if controller.vmLifecycle != nil {
		controller.remote.BindLifecycle(controller.vmLifecycle)
	}
	return controller
}
func newTestControllerWithWorkloads(t *testing.T, controller *Controller, state testWorkloadState) *Controller {
	t.Helper()
	newTestController(controller)
	setTestWorkloadState(t, controller, state)
	return controller
}

type workloadSeedClient struct {
	vmmdv1.VMMDServiceClient
	containers map[string][]*vmmdv1.Container
}

func (client workloadSeedClient) ListContainers(_ context.Context, request *vmmdv1.ListContainersRequest, _ ...grpc.CallOption) (*vmmdv1.ListContainersResponse, error) {
	return &vmmdv1.ListContainersResponse{Containers: client.containers[request.GetActorNamespaceId()]}, nil
}
func setTestWorkloadState(t *testing.T, controller *Controller, state testWorkloadState) {
	t.Helper()
	ctx := context.Background()
	containers := make(map[string][]*vmmdv1.Container)
	for key, run := range state.workloadRuns {
		if run.environmentID == "" {
			parts := strings.SplitN(key, "\x00", 2)
			run.environmentID = parts[0]
			if len(parts) > 1 && run.containerID == "" {
				run.containerID = parts[1]
			}
		}
		if run.namespaceID == "" {
			run.namespaceID = run.environmentID
		}
		if run.containerID == "" {
			run.containerID = "main"
		}
		environment, err := controller.store.GetEnvironment(ctx, run.environmentID)
		if errors.Is(err, sql.ErrNoRows) {
			environment = store.Environment{ID: run.environmentID, NamespaceID: run.namespaceID, OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}
			if run.registrationID != "" {
				environment.OwnerKind = store.OwnerRemoteRegistration
				environment.RegistrationID = run.registrationID
				environment.AllocationID = run.environmentID
				environment.AllocationState = int32(hostv1.AllocationState_ALLOCATION_STATE_READY)
			}
			environment, err = controller.store.CreateEnvironment(ctx, environment)
		}
		if err != nil {
			t.Fatal(err)
		}
		run.namespaceID = environment.NamespaceID
		state := vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
		if run.instanceID == "" {
			state = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
		}
		containers[run.namespaceID] = append(containers[run.namespaceID], &vmmdv1.Container{Id: run.containerID, InstanceId: run.instanceID, State: state})
	}
	if err := controller.workloads.RecoverWorkloadRunsFromGuest(ctx, workloadSeedClient{containers: containers}); err != nil {
		t.Fatal(err)
	}
	for _, run := range state.workloadRuns {
		identity := auth.Identity{Role: auth.Agent, SessionID: "fixture-session", EnvironmentID: run.environmentID, NamespaceID: run.namespaceID, AllocationID: run.environmentID}
		for id := range run.executions {
			_, err := auth.UnaryInterceptor(identity)(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				return controller.workloads.AcquireExecution(ctx, &hostv1.AcquireExecutionRequest{ContainerId: run.containerID, ExpectedInstanceId: run.instanceID, ExecutionId: id, Kind: hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION})
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if run.stopping {
			if err := controller.workloads.AcceptWorkloadStopLocked(ctx, run.environmentID, &vmmdv1.StopContainerRequest{RequestId: "fixture-stop", ContainerId: run.containerID}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for environment, items := range state.observed {
		controller.workloads.RememberContainers(environment, items)
	}
	if !state.workloadRunsRecovered {
		controller.workloads.ResetWorkloadRecovery()
	}
	testWorkloadStates.Store(controller, state)
}
func recoverTestWorkloads(t *testing.T, controller *Controller) {
	t.Helper()
	state, _ := testWorkloadStates.Load(controller)
	value, _ := state.(testWorkloadState)
	value.workloadRunsRecovered = true
	setTestWorkloadState(t, controller, value)
}
func releaseTestWorkloadExecution(t *testing.T, controller *Controller, key, id string) {
	t.Helper()
	value, _ := testWorkloadStates.Load(controller)
	state := value.(testWorkloadState)
	run := state.workloadRuns[key]
	identity := auth.Identity{EnvironmentID: run.environmentID, NamespaceID: run.namespaceID, AllocationID: run.environmentID, SessionID: "fixture-session"}
	controller.workloads.ReleaseObservedExecution(identity, run.containerID, run.instanceID, id)
	delete(run.executions, id)
}
func testWorkloadInstance(controller *Controller, environment, container string) workload.Instance {
	value, _ := controller.workloads.Instance(environment, container)
	return value
}

func workloadRunKey(environment, container string) string { return environment + "\x00" + container }
func setTestWorkloadInstance(t *testing.T, controller *Controller, environment, container, instance string) {
	t.Helper()
	value, _ := testWorkloadStates.Load(controller)
	state, _ := value.(testWorkloadState)
	state.workloadRunsRecovered = true
	if state.workloadRuns == nil {
		state.workloadRuns = make(map[string]*testWorkloadRun)
	}
	key := workloadRunKey(environment, container)
	run := state.workloadRuns[key]
	if run == nil {
		run = &testWorkloadRun{environmentID: environment, containerID: container}
		state.workloadRuns[key] = run
	}
	run.instanceID = instance
	setTestWorkloadState(t, controller, state)
}

func testWorkloadExists(controller *Controller, environment, container string) bool {
	_, exists := controller.workloads.Instance(environment, container)
	return exists
}
func testExecutionSnapshot(t *testing.T, controller *Controller) []*hostv1.Execution {
	t.Helper()
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "env-a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	value, err := auth.UnaryInterceptor(identity)(t.Context(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return controller.workloads.ListExecutions(ctx, &hostv1.ListExecutionsRequest{ContainerId: "main", ExpectedInstanceId: "instance-a"})
	})
	if err != nil {
		t.Fatal(err)
	}
	return value.(*hostv1.ListExecutionsResponse).GetExecutions()
}
func testExecutionPresent(t *testing.T, controller *Controller, id string) bool {
	for _, execution := range testExecutionSnapshot(t, controller) {
		if execution.GetExecutionId() == id {
			return true
		}
	}
	return false
}

func observationTestRegistration(id string) store.RemoteRegistration {
	return store.RemoteRegistration{ID: id, Endpoint: id, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State: int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED), ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED), EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1}
}

func observationTestEnvironment(id, registrationID string) store.Environment {
	return store.Environment{ID: id, NamespaceID: id, DisplayName: id, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registrationID, AllocationID: id,
		PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}
}

func observeTestServiceListener(t *testing.T, controller *Controller, id string) *hostv1.ServiceListener {
	t.Helper()
	response, err := controller.services.ListServiceListeners(t.Context(), &hostv1.ListServiceListenersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, listener := range response.GetListeners() {
		if listener.GetId() == id {
			return listener
		}
	}
	return nil
}
