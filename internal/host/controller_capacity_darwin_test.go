//go:build darwin

package host

import (
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func TestRemoteWorkloadCapacityUsesSharedVMCompute(t *testing.T) {
	capacity := remoteWorkloadCapacity(8, 12<<30, 64<<30)
	if capacity.CPUMaxMillis != 8000 || capacity.MemoryMaxBytes != 12<<30 || capacity.PIDMax != 2048 || capacity.WritableQuotaBytes != 8<<30 {
		t.Fatalf("remote workload capacity = %+v", capacity)
	}
	minimumCPU := remoteWorkloadCapacity(1, 1<<30, 16<<30)
	if minimumCPU.CPUMaxMillis != 1000 || minimumCPU.MemoryMaxBytes != 1<<30 || minimumCPU.WritableQuotaBytes != 4<<30 {
		t.Fatalf("minimum remote workload capacity = %+v", minimumCPU)
	}
}

func TestControllerSeparatesWorkloadAndSystemDiskCapacity(t *testing.T) {
	workload := environmentFromContract(remoteWorkloadCapacity(8, 12<<30, 64<<30))
	system := systemResourceCapacity(8, 12<<30)
	controller := &Controller{remoteCapacity: workload, systemCapacity: system}
	if controller.remoteCapacity.DiskBytes != 8<<30 {
		t.Fatalf("workload disk capacity=%d", controller.remoteCapacity.DiskBytes)
	}
	if got := controllerIsolationCapacity(controller); got.DiskBytes != guestdisk.ControlSystemBytes {
		t.Fatalf("system disk capacity=%d", got.DiskBytes)
	}
	if system.CPUMillis != 8000 || system.MemoryBytes != 12<<30 || system.PIDs != 2048 {
		t.Fatalf("system capacity=%+v", system)
	}
	if workload.CPUMillis != system.CPUMillis || workload.MemoryBytes != system.MemoryBytes {
		t.Fatalf("shared compute ceilings diverged: workload=%+v system=%+v", workload, system)
	}
}
