//go:build darwin || linux

package appliance

import (
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/host/remote"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func TestApplianceGrantUsesItsOwnFilesystemCapacity(t *testing.T) {
	controller := withTestRemoteCapacity(newTestOwner(&Owner{systemCapacity: store.Environment{PIDs: 2048, DiskBytes: guestdisk.ControlSystemBytes}}), store.Environment{PIDs: 2048, DiskBytes: 51 << 30})
	grant := &appliancev1.ApprovedGrant{SystemLimits: verifierLimits(1), Capacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: verifierLimits(1)}}
	grant.SystemLimits.DiskBytes = proto.Uint64(4 << 30)
	grant.Capacity.PerEnvironmentLimits.DiskBytes = proto.Uint64(16 << 30)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err != nil {
		t.Fatalf("16GiB workload on 64GiB state: %v", err)
	}
	grant.SystemLimits.DiskBytes = proto.Uint64(guestdisk.ControlSystemBytes + 1)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err == nil {
		t.Fatal("oversized control grant accepted")
	}
	grant.SystemLimits.DiskBytes = proto.Uint64(guestdisk.ControlSystemBytes)
	grant.Capacity.PerEnvironmentLimits.DiskBytes = proto.Uint64(controller.remote.(*remote.Owner).Capacity().DiskBytes + 1)
	if err := validateApplianceIsolationLimits(grant, controllerSystemIsolationCapacity(controller), controllerWorkloadIsolationCapacity(controller)); err == nil {
		t.Fatal("oversized workload grant accepted")
	}
}
