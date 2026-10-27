package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func newWorkloadStore(t *testing.T) *Store {
	t.Helper()
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx := context.Background()
	_, err = database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", PerEnvPIDs: 512, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func TestConcurrentStoredWorkloadsShareLimitsAboveOldTotalGate(t *testing.T) {
	database := newWorkloadStore(t)
	ctx := context.Background()
	contract := resourcev2.Contract{PIDMax: 512, WritableQuotaBytes: 4 << 30}
	capacity := resourcev2.Contract{PIDMax: 512, WritableQuotaBytes: 4 << 30}
	results := make(chan error, 257)
	var workers sync.WaitGroup
	for i := 0; i < 257; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id := fmt.Sprintf("workload-%d", i)
			_, err := database.BeginWorkloadAdmission(ctx, Environment{ID: id, NamespaceID: id, OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: id, AllocationState: 1, AllocationGeneration: 1, PIDs: uint32(contract.PIDMax), DiskBytes: contract.WritableQuotaBytes}, capacity)
			results <- err
		}()
	}
	workers.Wait()
	close(results)
	admitted := 0
	for err := range results {
		if err != nil {
			t.Fatalf("admission failed: %v", err)
		}
		admitted++
	}
	if admitted != 257 {
		t.Fatalf("admitted=%d", admitted)
	}
	environments, err := database.ListRemoteEnvironments(ctx, "registration")
	if err != nil || len(environments) != 257 {
		t.Fatalf("stored=%d err=%v", len(environments), err)
	}
}

func TestNamespaceProjectionCannotReviveTerminalOrReplaceGeneration(t *testing.T) {
	database := newWorkloadStore(t)
	ctx := context.Background()
	fact := Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 7, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING), PIDs: 512, DiskBytes: 4 << 30}
	environment, err := database.BeginWorkloadAdmission(ctx, fact, fact.Resources())
	if err != nil {
		t.Fatal(err)
	}
	stale := environment
	stale.AllocationGeneration--
	if err := database.CommitNamespaceProjection(ctx, stale); err == nil {
		t.Fatal("stale generation committed projection")
	}
	if err := database.CommitNamespaceProjection(ctx, environment); err != nil {
		t.Fatal(err)
	}
	environment, err = database.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatal(err)
	}
	retired, err := database.SetAllocationState(ctx, environment.ID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CommitNamespaceProjection(ctx, environment); err == nil {
		t.Fatal("late namespace response revived terminal allocation")
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != retired.AllocationState || current.Resources() != fact.Resources() || current.AllocationGeneration != 7 {
		t.Fatalf("retirement lost durable facts: %+v err=%v", current, err)
	}
	waiting, err := database.ListRetiringAllocations(ctx, "", 8)
	if err != nil || len(waiting) != 1 || waiting[0].ID != current.ID {
		t.Fatalf("durable retirement missing: %+v err=%v", waiting, err)
	}
}
