//go:build darwin

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
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
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
			"a": {environmentID: "environment-a", usage: &vmmdv1.Usage{CpuNanos: 7, MemoryBytes: 11, DiskBytes: 13, SampledAtUnixNano: now.UnixNano()}},
			"b": {environmentID: "environment-b", usage: &vmmdv1.Usage{CpuNanos: 101, MemoryBytes: 103, DiskBytes: 107, SampledAtUnixNano: now.UnixNano()}},
		},
	}
	observedAt := timestamppb.New(now)
	usage := controller.remoteWorkloadUsage(ctx, "registration-a", observedAt)
	if usage.GetStale() || usage.GetCpuNanos() != 7 || usage.GetMemoryBytes() != 11 || usage.GetDiskBytes() != 13 {
		t.Fatalf("scoped usage = %+v", usage)
	}
}

func TestRemoteWorkloadUsageFailsClosedForIncompleteCache(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
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
		{name: "future sample", pressureAt: now, usage: &vmmdv1.Usage{SampledAtUnixNano: now.Add(time.Second).UnixNano()}},
		{name: "stale pool pressure", pressureAt: now.Add(-3 * workloadSampleInterval), usage: &vmmdv1.Usage{SampledAtUnixNano: now.UnixNano()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := &Controller{store: database, workloadRunsRecovered: true, workloadPressureAt: test.pressureAt, workloadRuns: map[string]*workloadRun{"run": {environmentID: "environment", usage: test.usage}}}
			usage := controller.remoteWorkloadUsage(ctx, "registration", timestamppb.New(now))
			if !usage.GetStale() || usage.GetUnavailableReason() != "active workload metrics are unavailable" {
				t.Fatalf("usage = %+v", usage)
			}
		})
	}
}

func observationTestRegistration(id string) store.RemoteRegistration {
	return store.RemoteRegistration{ID: id, Endpoint: id, ProtocolVersion: "2", MaxEnvironments: 4,
		PerEnvCPUMillis: 1000, PerEnvMemoryBytes: 1 << 30, PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 10,
		State: int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED), ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED), EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), ConnectorPhase: 1}
}

func observationTestEnvironment(id, registrationID string) store.Environment {
	return store.Environment{ID: id, NamespaceID: id, DisplayName: id, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registrationID, AllocationID: id,
		CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}
}
