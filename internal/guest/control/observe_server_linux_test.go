//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func TestGuestOpenFDsReadsAllocatedCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file-nr")
	if err := os.WriteFile(path, []byte("127 0 9223372036854775807\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	count, err := guestOpenFDs(path)
	if err != nil || count != 127 {
		t.Fatalf("Guest open FDs = %d, err=%v", count, err)
	}
}

func TestObservationFailureIsBoundedAndKeepsItsLayer(t *testing.T) {
	message := boundedObservationFailure("Guest disk", errors.New(strings.Repeat("x", 2048)))
	if len(message) != 1024 || !strings.HasPrefix(message, "Guest disk: ") {
		t.Fatalf("bounded observation failure has length %d: %q", len(message), message)
	}
}

func TestStateFilesystemHealthIncludesControlFilesystem(t *testing.T) {
	var paths []string
	err := stateFilesystemsHealth(func(path string) error {
		paths = append(paths, path)
		if path == controlRoot {
			return syscall.ENOSPC
		}
		return nil
	})
	if !errors.Is(err, syscall.ENOSPC) || len(paths) != 2 || paths[0] != stateRoot || paths[1] != controlRoot {
		t.Fatalf("filesystem probes=%v err=%v", paths, err)
	}
}

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

func TestPersistentOCIQuotaSettlementFailureKeepsControlReadyAndMakesPoolHealthActionable(t *testing.T) {
	runtime := &Runtime{diskBudget: new(guestdisk.Budget)}
	_ = runtime.diskBudget.Settle(func() error { return errors.New("persistent settlement failure") })
	server := &Server{runtime: runtime, healthProbe: func(context.Context) healthProbeResult { return healthProbeResult{} }}
	health, err := server.Health(context.Background(), &vmmdv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !health.GetReady() {
		t.Fatal("sealed workload-pool growth gate blocked Guest control readiness")
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
