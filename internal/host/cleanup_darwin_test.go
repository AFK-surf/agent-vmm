//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
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

func TestMissingNamespaceDiscardPlanIsStableAndHostOnly(t *testing.T) {
	environment := store.Environment{ID: "retained", PolicyRevision: 7}
	first := missingNamespaceCleanupPlan(environment)
	second := missingNamespaceCleanupPlan(environment)
	if first.GetRevision() == 0 || first.GetRevision() != second.GetRevision() {
		t.Fatalf("missing namespace revision is not stable: %d %d", first.GetRevision(), second.GetRevision())
	}
	if first.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_DISCARD || len(first.GetImpacts()) != 1 || first.GetImpacts()[0].GetResource().GetKind() != "environment" {
		t.Fatalf("missing namespace plan = %+v", first)
	}
}
