//go:build darwin

package host

import (
	"google.golang.org/protobuf/proto"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func TestRemoteWorkloadCapacityUsesSharedVMCompute(t *testing.T) {
	capacity := remoteWorkloadCapacity(8, 12<<30, 64<<30)
	if capacity.CPUMaxMillis != 8000 || capacity.MemoryMaxBytes != 12<<30 || capacity.PIDMax != 2048 || capacity.WritableQuotaBytes != 51<<30 {
		t.Fatalf("remote workload capacity = %+v", capacity)
	}
	minimumCPU := remoteWorkloadCapacity(1, 1<<30, 16<<30)
	if minimumCPU.CPUMaxMillis != 1000 || minimumCPU.MemoryMaxBytes != 1<<30 || minimumCPU.WritableQuotaBytes != 3<<30 {
		t.Fatalf("minimum remote workload capacity = %+v", minimumCPU)
	}
}

func TestControllerSeparatesWorkloadAndSystemDiskCapacity(t *testing.T) {
	workload := environmentFromContract(remoteWorkloadCapacity(8, 12<<30, 64<<30))
	system := systemResourceCapacity(8, 12<<30)
	controller := &Controller{remoteCapacity: workload, systemCapacity: system}
	if controller.remoteCapacity.DiskBytes != 51<<30 {
		t.Fatalf("workload disk capacity=%d", controller.remoteCapacity.DiskBytes)
	}
	if got := controllerSystemIsolationCapacity(controller); got.DiskBytes != guestdisk.ControlSystemBytes {
		t.Fatalf("system disk capacity=%d", got.DiskBytes)
	}
	if system.CPUMillis != 8000 || system.MemoryBytes != 12<<30 || system.PIDs != 2048 {
		t.Fatalf("system capacity=%+v", system)
	}
	if workload.CPUMillis != system.CPUMillis || workload.MemoryBytes != system.MemoryBytes {
		t.Fatalf("shared compute ceilings diverged: workload=%+v system=%+v", workload, system)
	}
}

func TestApplianceGrantUsesItsOwnFilesystemCapacity(t *testing.T) {
	controller := &Controller{remoteCapacity: environmentFromContract(remoteWorkloadCapacity(4, 4<<30, 64<<30)), systemCapacity: systemResourceCapacity(4, 4<<30)}
	grant := &appliancev1.ApprovedGrant{SystemLimits: verifierLimits(1), Capacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: verifierLimits(1)}}
	grant.SystemLimits.DiskBytes = proto.Uint64(4 << 30)
	grant.Capacity.PerEnvironmentLimits.DiskBytes = proto.Uint64(16 << 30)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err != nil {
		t.Fatalf("16GiB workload on 64GiB state: %v", err)
	}
	grant.SystemLimits.DiskBytes = proto.Uint64(guestdisk.ControlSystemBytes + 1)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err == nil {
		t.Fatal("oversized control grant accepted")
	}
	grant.SystemLimits.DiskBytes = proto.Uint64(guestdisk.ControlSystemBytes)
	grant.Capacity.PerEnvironmentLimits.DiskBytes = proto.Uint64(controller.remoteCapacity.DiskBytes + 1)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err == nil {
		t.Fatal("oversized workload grant accepted")
	}
	for _, size := range []int64{0, 8 << 30, 13 << 30} {
		if got := remoteWorkloadCapacity(4, 4<<30, size).WritableQuotaBytes; got != 0 {
			t.Fatalf("state=%d capacity=%d", size, got)
		}
	}
}
