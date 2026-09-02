//go:build darwin

package host

import (
	"errors"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestDiscardTreatsNotFoundFromAnyGuestInventoryStageAsMissingNamespace(t *testing.T) {
	environment := store.Environment{ID: "retained", PolicyRevision: 7}
	plan, ok := missingNamespaceDiscardPlan(environment, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, status.Error(codes.NotFound, "resource was not found"))
	if !ok || plan.GetEnvironmentId() != environment.ID {
		t.Fatalf("missing namespace was not converted to a host-only discard plan: plan=%+v ok=%t", plan, ok)
	}
	for _, test := range []struct {
		kind hostv1.CleanupKind
		err  error
	}{
		{kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, err: status.Error(codes.NotFound, "missing")},
		{kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, err: errors.New("guest unavailable")},
	} {
		if _, ok := missingNamespaceDiscardPlan(environment, test.kind, test.err); ok {
			t.Fatalf("non-discard/not-found error was swallowed: kind=%s err=%v", test.kind, test.err)
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

func TestCleanupRevisionTracksResourceSetNotLifecycleState(t *testing.T) {
	grants := []store.Grant{{ID: "grant-a", Payload: "scope", SourceNamespaceID: "namespace-a"}}
	containers := []*vmmdv1.Container{
		{Id: "container-b", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING},
		{Id: "container-a", State: vmmdv1.ContainerState_CONTAINER_STATE_CREATED},
	}
	volumes := []*vmmdv1.Volume{{Id: "volume-a", MountedBy: []string{"container-b"}}}
	first := cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, containers, volumes, nil)

	containers[0].State = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	volumes[0].MountedBy = nil
	second := cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, []*vmmdv1.Container{containers[1], containers[0]}, volumes, nil)
	if first != second {
		t.Fatalf("lifecycle-only change invalidated cleanup plan: %d != %d", first, second)
	}

	withAdditionalContainer := append(containers, &vmmdv1.Container{Id: "container-c"})
	if first == cleanupInventoryRevision(hostv1.CleanupKind_CLEANUP_KIND_DISCARD, 7, "namespace-a", grants, withAdditionalContainer, volumes, nil) {
		t.Fatal("resource-set change did not invalidate cleanup plan")
	}
}

func TestCleanupChildRequestIDIsStableAndBounded(t *testing.T) {
	value := "operation-delete-workspace-remote-pi-019ffe950d107fa2995194027d5a4f9e"
	first := derivedRequestID(value)
	if len(first) > 63 || first != derivedRequestID(value) || first == derivedRequestID(value+"-different") {
		t.Fatalf("invalid derived request ID %q", first)
	}
}

func TestAllocationCleanupPlanPreservesKindAndImpacts(t *testing.T) {
	plan := allocationCleanupPlan("allocation", &hostv1.CleanupPlan{Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 9, Impacts: []*hostv1.CleanupImpact{
		{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "state"}, Effect: "retained"},
		{Resource: &hostv1.ResourceReference{Kind: "container", ResourceId: "worker"}, Effect: "deleted", Irreversible: true},
	}})
	if plan.GetAllocationId() != "allocation" || plan.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_RESET || plan.GetRevision() != 9 || len(plan.GetImpacts()) != 2 || len(plan.GetVolumeIds()) != 1 || plan.GetVolumeIds()[0] != "state" {
		t.Fatalf("allocation cleanup projection = %+v", plan)
	}
}
