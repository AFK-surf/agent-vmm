//go:build darwin || linux

package workload

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestValidatePolicyRequiresBoundedPIDDiskAndEgress(t *testing.T) {
	valid := &hostv1.ResourceLimits{Pids: new(uint32(128)), DiskBytes: new(uint64(8 << 30))}
	if err := ValidatePolicy(valid, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		limits *hostv1.ResourceLimits
		egress hostv1.EgressMode
	}{
		{name: "missing field", limits: &hostv1.ResourceLimits{}, egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		{name: "zero PID", limits: &hostv1.ResourceLimits{Pids: new(uint32(0)), DiskBytes: new(uint64(8 << 30))}, egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		{name: "unknown egress", limits: valid, egress: hostv1.EgressMode(99)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidatePolicy(test.limits, test.egress); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
