//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestValidateCleanupKindRejectsNonTerminalValues(t *testing.T) {
	for _, kind := range []hostv1.CleanupKind{hostv1.CleanupKind_CLEANUP_KIND_RESET, hostv1.CleanupKind_CLEANUP_KIND_DISCARD} {
		if err := validateCleanupKind(kind); err != nil {
			t.Fatalf("valid cleanup kind %v rejected: %v", kind, err)
		}
	}
	for _, kind := range []hostv1.CleanupKind{hostv1.CleanupKind_CLEANUP_KIND_UNSPECIFIED, hostv1.CleanupKind(99)} {
		if err := validateCleanupKind(kind); err == nil {
			t.Fatalf("invalid cleanup kind %v accepted", kind)
		}
	}
}
