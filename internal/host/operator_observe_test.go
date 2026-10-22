//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func TestGuestLayerResourcesKeepsPhysicalDiskCategoriesSeparate(t *testing.T) {
	openFDs := uint32(7)
	resources := guestLayerResources(&vmmdv1.GetUsageResponse{
		Usage:   &vmmdv1.Usage{MemoryBytes: 10, Pids: 2, SampledAtUnixNano: 17},
		OpenFds: &openFDs,
		Disk: &vmmdv1.DiskAccounting{
			UsedBytes: 11, CapacityBytes: 12, ImportReservationBytes: 3,
			SystemReserveBytes: 4, MetadataReserveBytes: 5,
		},
	})
	if resources.GetMemoryBytes() != 10 || resources.GetPids() != 2 || resources.GetOpenFds() != 7 || resources.GetSampledAt().AsTime().UnixNano() != 17 {
		t.Fatalf("guest usage = %+v", resources)
	}
	if disk := resources.GetDisk(); disk.GetUsedBytes() != 11 || disk.GetCapacityBytes() != 12 ||
		disk.GetImportReservationBytes() != 3 || disk.GetSystemReserveBytes() != 4 || disk.GetMetadataReserveBytes() != 5 {
		t.Fatalf("guest disk resources = %+v", disk)
	}
}

func TestSystemHealthSummaryDoesNotReadInventory(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	summary := controller.systemHealthSummary(context.Background())
	if summary.GetVm() == nil || summary.GetHealth().GetState() != hostv1.HealthState_HEALTH_STATE_UNAVAILABLE ||
		len(summary.GetEnvironments()) != 0 || len(summary.GetRemoteRegistrations()) != 0 {
		t.Fatalf("health summary = %+v", summary)
	}
}

func TestVMImpactSummaryBoundsKnownHostWorkloadsAndMarksOverflow(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for index := 0; index < 101; index++ {
		_, err = database.CreateEnvironment(context.Background(), store.Environment{ID: fmt.Sprintf("environment-%03d", index), NamespaceID: fmt.Sprintf("namespace-%03d", index), DisplayName: fmt.Sprintf("Environment %03d", index), OwnerKind: store.OwnerLocal})
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	summary, err := controller.vmImpactSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.GetComplete() || !summary.GetOverflow() || len(summary.GetKnownWorkloads()) != 100 {
		t.Fatalf("summary complete=%t overflow=%t known=%d issue=%q", summary.GetComplete(), summary.GetOverflow(), len(summary.GetKnownWorkloads()), summary.GetIssue())
	}
}

func TestVMMutationFenceAllowsMembershipAddedAfterWholeVMAcknowledgement(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	before, err := controller.vmImpactSummary(context.Background())
	if err != nil {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	_, err = database.CreateEnvironment(context.Background(), store.Environment{ID: "environment-new", NamespaceID: "namespace-new", DisplayName: "New", OwnerKind: store.OwnerLocal})
	if err != nil {
		t.Fatal(err)
	}
	fence := &hostv1.VMMutationFence{State: before.GetVm().GetState(), BootId: before.GetVm().GetBootId(), ObservedWorkloads: before.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	controller.inventoryTransition.Lock()
	err = controller.validateVMMutationFenceLocked(fence)
	controller.inventoryTransition.Unlock()
	if err != nil {
		t.Fatalf("whole-VM acknowledgement rejected newly added membership: %v", err)
	}
}

func TestInventoryTransitionWaitsForMutationWithoutFencingWholeVMByWorkloadRevision(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	environment, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "created", NamespaceID: "created", DisplayName: "Created", CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1, OwnerKind: store.OwnerLocal})
	if err != nil {
		t.Fatal(err)
	}
	before, err := controller.vmImpactSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fence := &hostv1.VMMutationFence{State: before.GetVm().GetState(), BootId: before.GetVm().GetBootId(), ObservedWorkloads: before.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}

	controller.inventoryTransition.RLock()
	result := make(chan error, 1)
	go func() {
		controller.inventoryTransition.Lock()
		defer controller.inventoryTransition.Unlock()
		result <- controller.validateVMMutationFenceLocked(fence)
	}()
	select {
	case err := <-result:
		t.Fatalf("transition did not wait for membership mutation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := database.UpdatePolicy(context.Background(), environment.ID, environment.PolicyRevision, store.Environment{CPUMillis: 2000, MemoryBytes: 2 << 30, PIDs: 256, DiskBytes: 8 << 30, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	controller.inventoryTransition.RUnlock()
	if err := <-result; err != nil {
		t.Fatalf("whole-VM acknowledgement rejected a workload revision change: %v", err)
	}
}

func TestInventoryTransitionBlocksNewMembershipMutationAfterValidationBegins(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	controller.inventoryTransition.Lock()
	started := make(chan struct{})
	finished := make(chan struct{})
	var mutationErr error
	var mutationMu sync.Mutex
	go func() {
		close(started)
		controller.inventoryTransition.RLock()
		defer controller.inventoryTransition.RUnlock()
		_, err := database.CreateEnvironment(context.Background(), store.Environment{ID: "created", NamespaceID: "created", OwnerKind: store.OwnerLocal})
		mutationMu.Lock()
		mutationErr = err
		mutationMu.Unlock()
		close(finished)
	}()
	<-started
	select {
	case <-finished:
		t.Fatal("membership mutation passed the active transition fence")
	case <-time.After(25 * time.Millisecond):
	}
	controller.inventoryTransition.Unlock()
	<-finished
	mutationMu.Lock()
	defer mutationMu.Unlock()
	if mutationErr != nil {
		t.Fatal(mutationErr)
	}
}

func TestVMImpactSummaryReportsStoreReadFailure(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	summary, err := controller.vmImpactSummary(context.Background())
	if err != nil || summary.GetComplete() || summary.GetIssue() == "" {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
}

func TestVMMutationFenceRequiresWholeVMImpactAcknowledgement(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database, manager: &vmm.Manager{}, observed: map[string][]*hostv1.Container{}}
	controller.inventoryTransition.Lock()
	err = controller.validateVMMutationFenceLocked(&hostv1.VMMutationFence{State: hostv1.VMState_VM_STATE_STOPPED})
	controller.inventoryTransition.Unlock()
	if err == nil {
		t.Fatal("VM mutation accepted without whole-VM impact acknowledgement")
	}
	for _, fence := range []*hostv1.VMMutationFence{
		{State: hostv1.VMState_VM_STATE_RUNNING, WholeVmImpactAcknowledged: true},
		{State: hostv1.VMState_VM_STATE_STOPPED, BootId: "different-boot", WholeVmImpactAcknowledged: true},
	} {
		if err := controller.validateVMMutationFence(context.Background(), fence); err == nil {
			t.Fatalf("VM mutation accepted stale state or boot: %v", fence)
		}
	}
}

func TestGuestLayerResourcesPreservesUnavailableReason(t *testing.T) {
	resources := guestLayerResources(&vmmdv1.GetUsageResponse{Usage: &vmmdv1.Usage{
		Unavailable: true, UnavailableReason: "usage source is not readable",
	}, OpenFdsUnavailableReason: "Guest FD source failed", DiskUnavailableReason: "Guest disk source failed"})
	if resources.MemoryBytes != nil || resources.Pids != nil || resources.OpenFds != nil ||
		resources.GetUnavailableReason() != "usage source is not readable; Guest FD source failed; Guest disk source failed" {
		t.Fatalf("unavailable reason = %q", resources.GetUnavailableReason())
	}
}
