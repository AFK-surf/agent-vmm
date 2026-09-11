//go:build darwin

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestNextApplianceReconcileDelayIsBounded(t *testing.T) {
	if got := nextApplianceReconcileDelay(100 * time.Millisecond); got != 200*time.Millisecond {
		t.Fatalf("first retry delay = %s, want 200ms", got)
	}
	if got := nextApplianceReconcileDelay(4 * time.Second); got != 5*time.Second {
		t.Fatalf("bounded retry delay = %s, want 5s", got)
	}
	if got := nextApplianceReconcileDelay(5 * time.Second); got != 5*time.Second {
		t.Fatalf("maximum retry delay = %s, want 5s", got)
	}
}

func TestReconcileObservedAppliancesSkipsApplianceDependenciesWhenEmpty(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	controller := &Controller{store: database}
	if err := controller.reconcileObservedAppliances(context.Background()); err != nil {
		t.Fatalf("empty appliance reconciliation = %v, want nil", err)
	}
}
