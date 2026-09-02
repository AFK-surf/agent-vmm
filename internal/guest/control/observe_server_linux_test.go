//go:build linux

package control

import (
	"context"
	"errors"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
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

func TestPersistentOCIQuotaSettlementFailureMakesHealthActionableAndNotReady(t *testing.T) {
	runtime := &Runtime{diskBudget: new(guestdisk.Budget)}
	_ = runtime.diskBudget.Settle(func() error { return errors.New("persistent settlement failure") })
	server := &Server{runtime: runtime, healthProbe: func(context.Context) healthProbeResult { return healthProbeResult{} }}
	health, err := server.Health(context.Background(), &vmmdv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if health.GetReady() {
		t.Fatal("sealed OCI quota gate reported guest ready")
	}
	components, err := server.ComponentHealth(context.Background(), &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range components.GetComponents() {
		if component.GetComponent() == "quota" {
			if component.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_UNAVAILABLE || component.GetReason() != guestdisk.ErrGrowthSealed.Error() {
				t.Fatalf("sealed quota health=%+v", component)
			}
			return
		}
	}
	t.Fatal("quota health component is missing")
}
