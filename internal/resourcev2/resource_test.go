package resourcev2

import (
	"testing"
)

func TestAdmissionReportsExactExhaustedDimension(t *testing.T) {
	limit := Contract{PIDMax: 4, WritableQuotaBytes: 5}
	for _, test := range []struct {
		name      string
		contract  Contract
		dimension CapacityDimension
	}{
		{name: "pids", contract: Contract{PIDMax: 5}, dimension: CapacityPIDs},
		{name: "writable", contract: Contract{WritableQuotaBytes: 6}, dimension: CapacityWritable},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ExhaustedDimension(test.contract.Fits(limit)); got != test.dimension {
				t.Fatalf("dimension = %q, want %q", got, test.dimension)
			}
		})
	}
}

func TestCgroupValuesShareCPUAndMemoryWithPIDBoundary(t *testing.T) {
	contract := Contract{PIDMax: 512}
	values := contract.CgroupValues()
	if values["cpu.max"] != "max 100000" || values["cpu.weight"] != "100" || values["memory.high"] != "max" || values["memory.max"] != "max" || values["pids.max"] != "512" {
		t.Fatalf("cgroup values = %#v", values)
	}
}
