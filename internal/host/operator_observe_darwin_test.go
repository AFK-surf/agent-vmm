//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestMarkUsageStale(t *testing.T) {
	tests := []struct {
		name     string
		response *hostv1.GetSystemSnapshotResponse
	}{
		{name: "missing usage", response: &hostv1.GetSystemSnapshotResponse{}},
		{name: "existing usage", response: &hostv1.GetSystemSnapshotResponse{Usage: &hostv1.Usage{CpuNanos: 7}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markUsageStale(test.response)
			if !test.response.GetUsage().GetStale() || test.response.GetUsage().GetSampledAt() == nil {
				t.Fatalf("markUsageStale() = %+v", test.response.GetUsage())
			}
		})
	}
}
