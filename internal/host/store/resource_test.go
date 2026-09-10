package store

import (
	"context"
	"errors"
	"fmt"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
	_, err = database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 256,
		PerEnvCPUMillis: 2000, PerEnvMemoryBytes: 2 << 30, PerEnvPIDs: 512, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func TestConcurrentStoredWorkloadsShareLimitsAndStopAt256(t *testing.T) {
	database := newWorkloadStore(t)
	ctx := context.Background()
	contract := resourcev2.Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512, WritableQuotaBytes: 4 << 30}
	capacity := resourcev2.Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512, WritableQuotaBytes: 4 << 30}
	results := make(chan error, 257)
	var workers sync.WaitGroup
	for i := 0; i < 257; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id := fmt.Sprintf("workload-%d", i)
			_, err := database.BeginWorkloadAdmission(ctx, Environment{ID: id, NamespaceID: id, OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: id, AllocationState: 1, AllocationGeneration: 1, CPUMillis: uint32(contract.CPUMaxMillis), MemoryBytes: contract.MemoryMaxBytes, PIDs: uint32(contract.PIDMax), DiskBytes: contract.WritableQuotaBytes}, capacity)
			results <- err
		}()
	}
	workers.Wait()
	close(results)
	admitted, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrStoredWorkloadLimit):
			rejected++
		default:
			t.Fatalf("admission failed: %v", err)
		}
	}
	if admitted != 256 || rejected != 1 {
		t.Fatalf("admitted=%d rejected=%d", admitted, rejected)
	}
	environments, err := database.ListRemoteEnvironments(ctx, "registration")
	if err != nil || len(environments) != 256 {
		t.Fatalf("stored=%d err=%v", len(environments), err)
	}
}

func TestNamespaceProjectionCannotReviveTerminalOrReplaceGeneration(t *testing.T) {
	database := newWorkloadStore(t)
	ctx := context.Background()
	fact := Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 7, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING), CPUMillis: 2000, MemoryBytes: 2 << 30, PIDs: 512, DiskBytes: 4 << 30}
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
	lease := RemoteLeaseProjection{LeaseID: "lease", RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 6, EnvironmentID: environment.ID, WorkloadRef: "work", Generation: 1, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}
	if _, err := database.AcquireRemoteLeaseProjection(ctx, lease, environment.PolicyRevision); err == nil {
		t.Fatal("stale allocation acquired lease")
	}
	lease.AllocationGeneration = 7
	leased, err := database.AcquireRemoteLeaseProjection(ctx, lease, environment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CommitNamespaceProjection(ctx, environment); err != nil {
		t.Fatal(err)
	}
	unchanged, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || unchanged.AllocationState != leased.AllocationState || unchanged.PolicyRevision != leased.PolicyRevision {
		t.Fatalf("projection replay replaced lease: %+v err=%v", unchanged, err)
	}
	retired, err := database.FinishRemoteLeaseProjection(ctx, "registration", "lease", environment.ID, leased.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
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
	waiting, err := database.ListRetiringAllocations(ctx, current.UpdatedAt.Add(time.Nanosecond), 8)
	if err != nil || len(waiting) != 0 {
		t.Fatalf("expired retirement kept retrying: %+v err=%v", waiting, err)
	}
	requeued, err := database.RequeueAllocationRetirement(ctx, current.ID, current.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	if !requeued.UpdatedAt.After(current.UpdatedAt) || requeued.PolicyRevision != current.PolicyRevision {
		t.Fatalf("requeued retirement changed fence or did not refresh deadline: before=%+v after=%+v", current, requeued)
	}
	waiting, err = database.ListRetiringAllocations(ctx, current.UpdatedAt.Add(time.Nanosecond), 8)
	if err != nil || len(waiting) != 1 || waiting[0].ID != current.ID {
		t.Fatalf("explicit retirement retry = %+v err=%v", waiting, err)
	}
}
