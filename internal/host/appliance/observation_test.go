//go:build darwin || linux

package appliance

import (
	"context"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type applianceObservationGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	response *vmmdv1.SystemApplianceResponse
	err      error
}

func (guest *applianceObservationGuest) GetSystemAppliance(context.Context, *vmmdv1.GetSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	return guest.response, guest.err
}

func TestGuestApplianceObservationDistinguishesAbsentStoppedAndUnknown(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *vmmdv1.SystemApplianceResponse
		err      error
		absent   bool
		state    vmmdv1.SystemApplianceState
		failed   bool
	}{
		{name: "absent", err: status.Error(codes.NotFound, "absent"), absent: true},
		{name: "unavailable", err: status.Error(codes.Unavailable, "offline"), failed: true},
		{name: "missing observation", response: &vmmdv1.SystemApplianceResponse{}, failed: true},
		{name: "running", response: &vmmdv1.SystemApplianceResponse{Appliance: &vmmdv1.SystemAppliance{State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}}, state: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING},
		{name: "stopped", response: &vmmdv1.SystemApplianceResponse{Appliance: &vmmdv1.SystemAppliance{State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED}}, state: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := workloadGuestClient(t, &applianceObservationGuest{response: test.response, err: test.err})
			observation, err := getSystemAppliance(context.Background(), client, "source")
			if (err != nil) != test.failed {
				t.Fatalf("observation error=%v", err)
			}
			if !test.failed && ((observation == nil) != test.absent || observation.GetState() != test.state) {
				t.Fatalf("observation=%v", observation)
			}
		})
	}
}
