//go:build darwin || linux

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestGrantConversionPreservesActualAndMaximumEgress(t *testing.T) {
	approved := verifierGrant()
	approved.EgressMode = hostv1.EgressMode_EGRESS_MODE_DENY_ALL
	approved.Capacity.MaxEgressMode = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
	stored := grantFromProto("installation", 4, approved)
	projected := grantToProto(stored)
	if stored.EgressMode != int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL) || stored.MaxEgressMode != int32(hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET) ||
		projected.GetEgressMode() != hostv1.EgressMode_EGRESS_MODE_DENY_ALL || projected.GetCapacity().GetMaxEgressMode() != hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET {
		t.Fatalf("actual/max egress collapsed: stored=%+v projected=%+v", stored, projected)
	}
}
