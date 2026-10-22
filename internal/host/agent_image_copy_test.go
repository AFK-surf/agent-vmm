//go:build darwin || linux

package host

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestImageCopyRequiresCurrentOwnedAllocations(t *testing.T) {
	identity := auth.Identity{RegistrationID: "controller", NamespaceID: "task"}
	request := &hostv1.CopyImageRequest{Source: &hostv1.ImageSource{Source: &hostv1.ImageSource_Allocation{Allocation: &hostv1.AllocationImageSource{AllocationId: "preparation", AllocationGeneration: 3}}}, TargetAllocationId: "task", TargetAllocationGeneration: 7}
	source := store.Environment{OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "preparation", AllocationGeneration: 3, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)}
	target := store.Environment{OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "task", NamespaceID: "task", AllocationGeneration: 7, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)}
	if err := validateImageCopyAllocations(identity, request, source, target, true); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*store.Environment){
		func(e *store.Environment) { e.RegistrationID = "other-controller" },
		func(e *store.Environment) { e.AllocationGeneration++ },
		func(e *store.Environment) { e.AllocationID = "other-allocation" },
		func(e *store.Environment) {
			e.AllocationState = int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)
		},
	} {
		for _, sourceSide := range []bool{true, false} {
			a, b := source, target
			if sourceSide {
				mutate(&a)
			} else {
				mutate(&b)
			}
			if err := validateImageCopyAllocations(identity, request, a, b, true); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("invalid copy allocation accepted: %v", err)
			}
		}
	}
	target.NamespaceID = "other-namespace"
	if err := validateImageCopyAllocations(identity, request, source, target, true); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign target namespace: %v", err)
	}
}

func TestImageCopyObservationKeepsOwnerFenceAfterExecutionStops(t *testing.T) {
	identity := auth.Identity{RegistrationID: "controller", NamespaceID: "target"}
	request := &hostv1.CopyImageRequest{Source: &hostv1.ImageSource{Source: &hostv1.ImageSource_Allocation{Allocation: &hostv1.AllocationImageSource{AllocationId: "source", AllocationGeneration: 3}}}, TargetAllocationId: "target", TargetAllocationGeneration: 7}
	source := store.Environment{OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "source", AllocationGeneration: 3, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DISCARDED)}
	target := store.Environment{OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "target", AllocationGeneration: 7, NamespaceID: "target", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)}
	if err := validateImageCopyAllocations(identity, request, source, target, false); err != nil {
		t.Fatal(err)
	}
	source.AllocationGeneration++
	if err := validateImageCopyAllocations(identity, request, source, target, false); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale generation accepted: %v", err)
	}
}

func TestRetainedImageSourceHasExplicitControllerAuthority(t *testing.T) {
	identity := auth.Identity{RegistrationID: "controller", AllocationID: "target", AllocationGeneration: 7, NamespaceID: "target"}
	target := store.Environment{OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "target", AllocationGeneration: 7, NamespaceID: "target", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)}
	source := &hostv1.ImageSource{Source: &hostv1.ImageSource_Retained{Retained: &hostv1.RetainedImageSource{BuildRequestId: "build"}}}
	server := new(agentServer)
	namespace, scope, err := server.authorizeImageSource(context.Background(), identity, source, target, 7, true)
	if err != nil || namespace != "" || scope != "controller" {
		t.Fatalf("retained source authority: %s %s %v", namespace, scope, err)
	}
	if _, _, err := server.authorizeImageSource(context.Background(), identity, nil, target, 7, true); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing source became retained: %v", err)
	}
	target.RegistrationID = "other-controller"
	if _, _, err := server.authorizeImageSource(context.Background(), identity, source, target, 7, true); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign controller target: %v", err)
	}
	target.RegistrationID = "controller"
	target.AllocationGeneration++
	if _, _, err := server.authorizeImageSource(context.Background(), identity, source, target, 7, true); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale target generation: %v", err)
	}
}
