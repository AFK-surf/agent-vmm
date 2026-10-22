//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemoteWorkloadUsageScopesActiveCachedSamples(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, id := range []string{"registration-a", "registration-b"} {
		if _, err := database.CreateRemoteRegistration(ctx, observationTestRegistration(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, environment := range []store.Environment{
		observationTestEnvironment("environment-a", "registration-a"),
		observationTestEnvironment("environment-b", "registration-b"),
	} {
		if _, err := database.CreateEnvironment(ctx, environment); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	controller := &Controller{
		store: database, workloadRunsRecovered: true, workloadPressureAt: now,
		workloadRuns: map[string]*workloadRun{
			"a": {environmentID: "environment-a", registrationID: "registration-a", usage: &vmmdv1.Usage{CpuNanos: 7, MemoryBytes: 11, DiskBytes: 13, SampledAtUnixNano: now.UnixNano()}},
			"b": {environmentID: "environment-b", registrationID: "registration-b", usage: &vmmdv1.Usage{CpuNanos: 101, MemoryBytes: 103, DiskBytes: 107, SampledAtUnixNano: now.UnixNano()}},
		},
	}
	observedAt := timestamppb.New(now)
	usage := controller.remoteWorkloadUsage(ctx, "registration-a", observedAt)
	if usage.GetStale() || usage.GetCpuNanos() != 7 || usage.GetMemoryBytes() != 11 || usage.GetDiskBytes() != 13 {
		t.Fatalf("scoped usage = %+v", usage)
	}
}

func TestRemoteWorkloadUsageFailsClosedForIncompleteCache(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, observationTestRegistration("registration")); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, observationTestEnvironment("environment", "registration")); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, test := range []struct {
		name       string
		pressureAt time.Time
		usage      *vmmdv1.Usage
	}{
		{name: "missing sample", pressureAt: now},
		{name: "stale sample", pressureAt: now, usage: &vmmdv1.Usage{SampledAtUnixNano: now.Add(-3 * workloadSampleInterval).UnixNano()}},
		{name: "far future sample", pressureAt: now, usage: &vmmdv1.Usage{SampledAtUnixNano: now.Add(2 * workloadSampleInterval).UnixNano()}},
		{name: "stale pool pressure", pressureAt: now.Add(-3 * workloadSampleInterval), usage: &vmmdv1.Usage{SampledAtUnixNano: now.UnixNano()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := &Controller{store: database, workloadRunsRecovered: true, workloadPressureAt: test.pressureAt, workloadRuns: map[string]*workloadRun{"run": {environmentID: "environment", registrationID: "registration", usage: test.usage}}}
			usage := controller.remoteWorkloadUsage(ctx, "registration", timestamppb.New(now))
			if !usage.GetStale() || usage.GetUnavailableReason() != "active workload metrics are unavailable" {
				t.Fatalf("usage = %+v", usage)
			}
		})
	}
	controller := &Controller{store: database, workloadRunsRecovered: true, workloadPressureAt: now, workloadRuns: map[string]*workloadRun{"run": {environmentID: "environment", registrationID: "registration", usage: &vmmdv1.Usage{SampledAtUnixNano: now.Add(time.Second).UnixNano()}}}}
	if usage := controller.remoteWorkloadUsage(ctx, "registration", timestamppb.New(now)); usage.GetStale() {
		t.Fatalf("bounded Guest clock skew rejected fresh usage: %+v", usage)
	}
}

func observationTestRegistration(id string) store.RemoteRegistration {
	return store.RemoteRegistration{ID: id, Endpoint: id, ProtocolVersion: "2",
		PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State: int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED), ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED), EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1}
}

func observationTestEnvironment(id, registrationID string) store.Environment {
	return store.Environment{ID: id, NamespaceID: id, DisplayName: id, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registrationID, AllocationID: id,
		CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}
}

func TestWorkloadPressureRequiresSustainedWatermarksAndLocalOOM(t *testing.T) {
	controller := workloadTestController(t)
	now := time.Now()
	sample := func(memory uint64, psi float64, oom uint64) {
		t.Helper()
		controller.observeWorkloadPressureLocked(&vmmdv1.GetUsageResponse{
			WorkloadPool: &vmmdv1.Usage{MemoryBytes: memory}, WorkloadMemoryLimitBytes: 1000,
			WorkloadMemoryFullAvg10: psi, WorkloadOomEvents: oom,
		}, now)
	}
	for range 2 {
		sample(950, 0, 0)
	}
	if controller.workloadMemoryPressure {
		t.Fatal("transient high water entered pressure")
	}
	sample(850, 0, 0)
	for range 2 {
		sample(100, 11, 0)
	}
	if controller.workloadMemoryPressure {
		t.Fatal("interrupted high-water streak was retained")
	}
	sample(100, 11, 0)
	if !controller.workloadMemoryPressure {
		t.Fatal("sustained PSI did not enter pressure")
	}
	for range 2 {
		sample(700, 0, 0)
	}
	if !controller.workloadMemoryPressure {
		t.Fatal("pressure ended before three low samples")
	}
	sample(700, 0, 0)
	if controller.workloadMemoryPressure {
		t.Fatal("sustained low water did not exit pressure")
	}
	sample(100, 0, 1)
	if !controller.workloadMemoryPressure {
		t.Fatal("local OOM did not immediately enter pressure")
	}
	for range 3 {
		sample(100, 0, 1)
	}
	if controller.workloadMemoryPressure {
		t.Fatal("unchanged OOM counter kept pressure active")
	}
}

func TestWorkloadPressureSelectsOneOldestIdleTenantAndExpiresWithoutReplacement(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	controller := workloadTestController(t)
	controller.store = database
	guest := &workloadGuest{states: make(map[string]*vmmdv1.Container), pool: &vmmdv1.GetUsageResponse{
		WorkloadPool: &vmmdv1.Usage{MemoryBytes: 950}, WorkloadMemoryLimitBytes: 1000,
	}}
	for index, id := range []string{"a", "b"} {
		registration := "registration-" + id
		if _, err := database.CreateRemoteRegistration(ctx, observationTestRegistration(registration)); err != nil {
			t.Fatal(err)
		}
		environment := observationTestEnvironment("environment-"+id, registration)
		if _, err := database.CreateEnvironment(ctx, environment); err != nil {
			t.Fatal(err)
		}
		run := &workloadRun{environmentID: environment.ID, namespaceID: environment.NamespaceID, containerID: "main", instanceID: "instance-" + id, idleSince: time.Now().Add(-time.Duration(3-index) * time.Minute)}
		controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] = run
		guest.states[environment.NamespaceID] = &vmmdv1.Container{Id: "main", InstanceId: run.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}
	}
	client := workloadGuestClient(t, guest)
	for range 2 {
		if err := controller.sampleWorkloadRuns(ctx, client); err != nil {
			t.Fatal(err)
		}
		if controller.workloadReclaim != nil {
			t.Fatal("candidate without sustained pressure")
		}
	}
	if err := controller.sampleWorkloadRuns(ctx, client); err != nil {
		t.Fatal(err)
	}
	candidate := controller.workloadReclaim
	if candidate == nil || candidate.value.GetRegistrationId() != "registration-a" || candidate.value.GetContainerInstanceId() != "instance-a" {
		t.Fatalf("oldest idle selection: %+v", candidate)
	}
	candidate.run.quiescing = true
	candidate.run.quiesceRequestID = "quiet"
	candidate.value.ExpiresUnixMillis = time.Now().Add(-time.Second).UnixMilli()
	if err := controller.sampleWorkloadRuns(ctx, client); err != nil {
		t.Fatal(err)
	}
	if controller.workloadReclaim != nil || candidate.run.quiescing {
		t.Fatal("expired candidate retained stop authority or was immediately replaced")
	}
	if err := controller.sampleWorkloadRuns(ctx, client); err != nil || controller.workloadReclaim == nil {
		t.Fatalf("next fresh sample must select an idle candidate: %v", err)
	}
	guest.mu.Lock()
	guest.usageUnavailable = true
	guest.mu.Unlock()
	if err := controller.sampleWorkloadRuns(ctx, client); err != nil {
		t.Fatal(err)
	}
	if controller.workloadReclaim != nil {
		t.Fatal("unavailable usage retained or replaced a reclaim candidate")
	}
}
