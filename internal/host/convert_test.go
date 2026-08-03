//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func TestUsageFromGuestPreservesUnavailableAsStale(t *testing.T) {
	usage := usageFromGuest(&vmmdv1.Usage{CpuNanos: 7, NetworkRxBytes: 13, NetworkTxBytes: 17, SampledAtUnixNano: 11, Unavailable: true})
	if usage.GetCpuNanos() != 7 || usage.GetNetworkRxBytes() != 13 || usage.GetNetworkTxBytes() != 17 || usage.GetSampledAt().AsTime().UnixNano() != 11 || !usage.GetStale() {
		t.Fatalf("usageFromGuest() = %+v", usage)
	}
}

func TestCloneContainersMarksIndependentCopiesUnknown(t *testing.T) {
	original := []*hostv1.Container{{Id: "container", State: hostv1.ContainerState_CONTAINER_STATE_RUNNING, Argv: []string{"true"}}}
	cloned := cloneContainers(original, true)
	if cloned[0].GetState() != hostv1.ContainerState_CONTAINER_STATE_UNKNOWN {
		t.Fatalf("clone state = %v", cloned[0].GetState())
	}
	cloned[0].Argv[0] = "false"
	if original[0].GetArgv()[0] != "true" || original[0].GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("clone mutated original: %+v", original[0])
	}
}
