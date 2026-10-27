//go:build darwin || linux

package guestconn

import (
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

func TestContainerObservationRetainsSuccessfulStartInstanceAfterStop(t *testing.T) {
	guest := &vmmdv1.Container{Id: "worker", GenerationId: "generation", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, InstanceId: "current-instance", StartRequestInstanceId: "current-instance"}
	for _, state := range []vmmdv1.ContainerState{vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, vmmdv1.ContainerState_CONTAINER_STATE_STOPPED} {
		guest.State = state
		if state == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			guest.InstanceId = ""
		}
		observed := Container(guest)
		if observed.GetInstanceId() != guest.GetInstanceId() || observed.GetStartRequestInstanceId() != "current-instance" {
			t.Fatalf("current and successful Start instances diverged: %v", observed)
		}
	}
}

func TestUsageFromGuestPreservesUnavailableAsStale(t *testing.T) {
	usage := Usage(&vmmdv1.Usage{CpuNanos: 7, NetworkRxBytes: 13, NetworkTxBytes: 17, SampledAtUnixNano: 11, Unavailable: true})
	if usage.GetCpuNanos() != 7 || usage.GetNetworkRxBytes() != 13 || usage.GetNetworkTxBytes() != 17 || usage.GetSampledAt().AsTime().UnixNano() != 11 || !usage.GetStale() {
		t.Fatalf("Usage() = %+v", usage)
	}
}
