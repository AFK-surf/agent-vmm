//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestValidatePolicyRequiresCompleteBoundedValues(t *testing.T) {
	valid := &hostv1.ResourceLimits{CpuMillis: optional(uint32(1000)), MemoryBytes: optional(uint64(1 << 30)), Pids: optional(uint32(128)), DiskBytes: optional(uint64(8 << 30))}
	if err := validatePolicy(valid, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		limits *hostv1.ResourceLimits
		egress hostv1.EgressMode
	}{
		{name: "missing field", limits: &hostv1.ResourceLimits{CpuMillis: optional(uint32(1000))}, egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		{name: "zero", limits: &hostv1.ResourceLimits{CpuMillis: optional(uint32(0)), MemoryBytes: optional(uint64(1 << 30)), Pids: optional(uint32(128)), DiskBytes: optional(uint64(8 << 30))}, egress: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		{name: "unknown egress", limits: valid, egress: hostv1.EgressMode(99)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validatePolicy(test.limits, test.egress); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
