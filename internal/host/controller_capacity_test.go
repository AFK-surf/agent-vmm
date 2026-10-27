//go:build darwin || linux

package host

import (
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func TestRemoteWorkloadCapacityPreservesStorageHeadroom(t *testing.T) {
	capacity := remoteWorkloadCapacity(64 << 30)
	if capacity.PIDMax != 2048 || capacity.WritableQuotaBytes != 51<<30 {
		t.Fatalf("remote workload capacity = %+v", capacity)
	}
	minimumDisk := remoteWorkloadCapacity(16 << 30)
	if minimumDisk.WritableQuotaBytes != 3<<30 {
		t.Fatalf("minimum remote workload capacity = %+v", minimumDisk)
	}
}

func TestControllerSeparatesWorkloadAndSystemDiskCapacity(t *testing.T) {
	workload := remoteWorkloadCapacity(64 << 30)
	system := systemResourceCapacity()
	if workload.WritableQuotaBytes != 51<<30 {
		t.Fatalf("workload disk capacity=%d", workload.WritableQuotaBytes)
	}
	if system.DiskBytes != guestdisk.ControlSystemBytes {
		t.Fatalf("system disk capacity=%d", system.DiskBytes)
	}

	if system.PIDs != 2048 {
		t.Fatalf("system capacity=%+v", system)
	}

}
