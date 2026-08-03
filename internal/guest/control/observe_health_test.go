package control

import (
	"errors"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func TestComponentStatusReportsDependencyFailure(t *testing.T) {
	status := componentStatus("containerd/runsc", errors.New("version probe failed"), 42)
	if status.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_UNAVAILABLE || status.GetReason() != "version probe failed" || status.GetObservedAtUnixNano() != 42 {
		t.Fatalf("componentStatus() = %+v", status)
	}
}
