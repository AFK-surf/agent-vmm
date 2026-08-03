//go:build linux

package control

import (
	"context"
	"errors"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func TestComponentHealthReportsContainerdVersionProbeFailure(t *testing.T) {
	called := false
	server := &Server{healthProbe: func(context.Context) healthProbeResult {
		called = true
		return healthProbeResult{containerd: errors.New("containerd version probe failed")}
	}}
	response, err := server.ComponentHealth(context.Background(), &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("ComponentHealth did not execute dependency probes")
	}
	for _, component := range response.GetComponents() {
		if component.GetComponent() == "containerd/runsc" {
			if component.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_UNAVAILABLE || component.GetReason() != "containerd version probe failed" {
				t.Fatalf("containerd/runsc health = %+v", component)
			}
			return
		}
	}
	t.Fatal("containerd/runsc health component is missing")
}
