package resourcev2

import (
	"math"
	"testing"
)

func TestIndependentAdmissionInvariants(t *testing.T) {
	limit := NodeCapacity(8000, 9<<30, 2048, 8<<30)
	meeting := Contract{CPUGuaranteeMillis: 2000, CPUMaxMillis: 3000, CPUWeight: 8000, MemoryHighBytes: 7 << 29, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	codex := Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 2000, CPUWeight: 5000, MemoryHighBytes: 3 << 29, MemoryMaxBytes: 2 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	if err := meeting.Validate(8000); err != nil {
		t.Fatal(err)
	}
	used := Capacity{}.Add(meeting).Add(codex).Add(codex)
	if err := used.Admits(Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 5000, CPUWeight: 1, MemoryHighBytes: 1 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 513, WritableQuotaBytes: 2 << 30}, limit); err == nil {
		t.Fatal("PID overflow was admitted even though the other dimensions fit")
	}
	if limit.CPUMaxMillis != 12000 {
		t.Fatalf("cpu max budget = %d", limit.CPUMaxMillis)
	}
}

func TestSharedAdmissionReservesMemoryHighWithoutSummingHardMax(t *testing.T) {
	limit := NodeCapacity(8000, 17<<29, 2048, 8<<30)
	agent := Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 2000, MemoryHighBytes: 3 << 29, MemoryMaxBytes: 2 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	meeting := Contract{CPUGuaranteeMillis: 2000, CPUMaxMillis: 3000, MemoryHighBytes: 7 << 29, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 2 << 30}
	used := Capacity{}.Add(agent).Add(agent).Add(agent)
	if err := used.AdmitsShared(meeting, limit); err != nil {
		t.Fatalf("bounded memory overcommit was rejected: %v", err)
	}
	if err := (Capacity{}).Admits(meeting, Capacity{CPUGuaranteeMillis: 8000, CPUMaxMillis: 12000, MemoryMaxBytes: 3 << 30, PIDMax: 2048, WritableQuotaBytes: 8 << 30}); ExhaustedDimension(err) != CapacityMemory {
		t.Fatalf("per-workload hard max was not enforced: %v", err)
	}
}

func TestContractRejectsCPUAboveNode(t *testing.T) {
	contract := Contract{CPUGuaranteeMillis: 1000, CPUMaxMillis: 8001, CPUWeight: 100, MemoryHighBytes: 16 << 20, MemoryMaxBytes: 16 << 20, PIDMax: 1, WritableQuotaBytes: 1}
	if err := contract.Validate(8000); err == nil {
		t.Fatal("cpu max above node capacity was accepted")
	}
}

func TestAdmissionArithmeticOverflowFailsClosed(t *testing.T) {
	used := Capacity{MemoryMaxBytes: math.MaxUint64 - 1}
	if err := used.Admits(Contract{MemoryMaxBytes: 2}, Capacity{MemoryMaxBytes: math.MaxUint64}); err == nil {
		t.Fatal("overflowing memory commitment was admitted")
	}
}

func TestAdmissionReportsExactExhaustedDimension(t *testing.T) {
	limit := Capacity{CPUGuaranteeMillis: 1, CPUMaxMillis: 2, MemoryMaxBytes: 3, PIDMax: 4, WritableQuotaBytes: 5}
	for _, test := range []struct {
		name      string
		contract  Contract
		dimension CapacityDimension
	}{
		{name: "cpu guarantee", contract: Contract{CPUGuaranteeMillis: 2}, dimension: CapacityCPUGuarantee},
		{name: "cpu max", contract: Contract{CPUMaxMillis: 3}, dimension: CapacityCPUMax},
		{name: "memory", contract: Contract{MemoryMaxBytes: 4}, dimension: CapacityMemory},
		{name: "pids", contract: Contract{PIDMax: 5}, dimension: CapacityPIDs},
		{name: "writable", contract: Contract{WritableQuotaBytes: 6}, dimension: CapacityWritable},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ExhaustedDimension((Capacity{}).Admits(test.contract, limit)); got != test.dimension {
				t.Fatalf("dimension = %q, want %q", got, test.dimension)
			}
		})
	}
}

func TestCgroupValuesKeepPressureAndHardLimitsSeparate(t *testing.T) {
	contract := Contract{CPUMaxMillis: 2000, CPUWeight: 5000, MemoryHighBytes: 1536 << 20, MemoryMaxBytes: 2 << 30, PIDMax: 512}
	values := contract.CgroupValues()
	if values["cpu.max"] != "200000 100000" || values["cpu.weight"] != "5000" || values["memory.high"] == values["memory.max"] || values["pids.max"] != "512" {
		t.Fatalf("cgroup values = %#v", values)
	}
}
