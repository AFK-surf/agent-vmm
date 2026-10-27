//go:build darwin || linux

package appliance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func TestNextApplianceReconcileDelayIsBounded(t *testing.T) {
	if got := nextApplianceReconcileDelay(100 * time.Millisecond); got != 5*time.Second {
		t.Fatalf("first retry delay = %s, want 5s", got)
	}
	if got := nextApplianceReconcileDelay(4 * time.Second); got != 8*time.Second {
		t.Fatalf("retry delay = %s, want 8s", got)
	}
	if got := nextApplianceReconcileDelay(time.Minute); got != time.Minute {
		t.Fatalf("maximum retry delay = %s, want 1m", got)
	}
}

func TestReconcileObservedAppliancesSkipsApplianceDependenciesWhenEmpty(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	controller := newTestOwner(&Owner{store: database})
	if err := controller.ReconcileAppliances(context.Background()); err != nil {
		t.Fatalf("empty appliance reconciliation = %v, want nil", err)
	}
}

func TestRepublishApplianceDiscardsSnapshotAfterStop(t *testing.T) {
	controller, snapshot := updateDrainFixture(t)
	before, err := controller.store.GetRemoteRegistration(context.Background(), snapshot.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	current := snapshot
	current.DesiredEnabled = false
	current.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	if _, err := controller.store.UpdateApplianceInstallation(context.Background(), current, current.Revision); err != nil {
		t.Fatal(err)
	}
	if err := controller.republishAppliance(context.Background(), nil, snapshot, nil, nil); err != nil {
		t.Fatalf("stale publication = %v", err)
	}
	registration, err := controller.store.GetRemoteRegistration(context.Background(), snapshot.RegistrationID)
	if err != nil || registration.State != before.State {
		t.Fatalf("stale publication enabled registration: %+v, %v", registration, err)
	}
}
