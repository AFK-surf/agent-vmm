package resourcev2

import (
	"testing"
)

func TestWorkloadCPUAndMemoryDoNotReserveNodeCapacity(t *testing.T) {
	contract := Contract{CPUMaxMillis: 8001, MemoryMaxBytes: 64 << 30, PIDMax: 1, WritableQuotaBytes: 1}
	if err := contract.Validate(8000); err != nil {
		t.Fatal(err)
	}
	if err := contract.Fits(Contract{PIDMax: 1, WritableQuotaBytes: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionReportsExactExhaustedDimension(t *testing.T) {
	limit := Contract{CPUMaxMillis: 2, MemoryMaxBytes: 3, PIDMax: 4, WritableQuotaBytes: 5}
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
	contract := Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512}
	values := contract.CgroupValues()
	if values["cpu.max"] != "max 100000" || values["cpu.weight"] != "100" || values["memory.high"] != "max" || values["memory.max"] != "max" || values["pids.max"] != "512" {
		t.Fatalf("cgroup values = %#v", values)
	}
}
