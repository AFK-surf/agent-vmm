//go:build darwin || linux

package workload

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

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
