//go:build darwin

package host

import (
	"testing"
	"time"
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
