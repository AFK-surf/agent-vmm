package resourcev2

import (
	"testing"
)

func TestContractRejectsCPUAboveNode(t *testing.T) {
	contract := Contract{CPUMaxMillis: 8001, MemoryMaxBytes: 16 << 20, PIDMax: 1, WritableQuotaBytes: 1}
	if err := contract.Validate(8000); err == nil {
		t.Fatal("cpu max above node capacity was accepted")
	}
}

func TestAdmissionReportsExactExhaustedDimension(t *testing.T) {
	limit := Contract{CPUMaxMillis: 2, MemoryMaxBytes: 3, PIDMax: 4, WritableQuotaBytes: 5}
	for _, test := range []struct {
		name      string
		contract  Contract
		dimension CapacityDimension
	}{
		{name: "cpu max", contract: Contract{CPUMaxMillis: 3}, dimension: CapacityCPUMax},
		{name: "memory", contract: Contract{MemoryMaxBytes: 4}, dimension: CapacityMemory},
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

func TestCgroupValuesUsePlatformSharingAndWorkloadCeilings(t *testing.T) {
	contract := Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512}
	values := contract.CgroupValues()
	if values["cpu.max"] != "200000 100000" || values["cpu.weight"] != "100" || values["memory.high"] != "max" || values["memory.max"] != "2147483648" || values["pids.max"] != "512" {
		t.Fatalf("cgroup values = %#v", values)
	}
}
