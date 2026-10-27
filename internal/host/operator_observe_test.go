//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
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
	controller := newTestController(&Controller{store: database, manager: &vmm.Manager{}})
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	summary := (&operatorServer{controller: controller}).systemHealthSummary(context.Background())
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
	controller := newTestController(&Controller{store: database, manager: &vmm.Manager{}})
	summary, err := (&operatorServer{controller: controller}).vmImpactSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.GetComplete() || !summary.GetOverflow() || len(summary.GetKnownWorkloads()) != 100 {
		t.Fatalf("summary complete=%t overflow=%t known=%d issue=%q", summary.GetComplete(), summary.GetOverflow(), len(summary.GetKnownWorkloads()), summary.GetIssue())
	}
}

func TestVMImpactSummaryReportsStoreReadFailure(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database, manager: &vmm.Manager{}})
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := (&operatorServer{controller: controller}).vmImpactSummary(context.Background())
	if err != nil || summary.GetComplete() || summary.GetIssue() == "" {
		t.Fatalf("summary=%+v err=%v", summary, err)
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

func TestEnvironmentSnapshotUsesStoppedVMState(t *testing.T) {
	manager, err := vmm.NewManager(vmm.Config{BundleDirectory: t.TempDir(), InstanceDirectory: t.TempDir(), SerialLog: filepath.Join(t.TempDir(), "serial.log"), CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{manager: manager})
	snapshot := controller.services.EnvironmentSnapshot(context.Background(), store.Environment{ID: "environment", NamespaceID: "namespace"})
	if snapshot.GetInventoryState() != hostv1.InventoryState_INVENTORY_STATE_VM_STOPPED {
		t.Fatalf("stopped inventory = %v", snapshot.GetInventoryState())
	}
}
