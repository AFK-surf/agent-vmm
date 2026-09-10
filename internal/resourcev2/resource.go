package resourcev2

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

type CapacityDimension string

const (
	CapacityCPUMax          CapacityDimension = "cpu_max"
	CapacityMemory          CapacityDimension = "memory"
	CapacityPIDs            CapacityDimension = "pids"
	CapacityWritable        CapacityDimension = "writable"
	CapacityStorageHeadroom CapacityDimension = "storage_headroom"
	CapacityImportSlot      CapacityDimension = "import_slot"
	CapacityRunSlot         CapacityDimension = "run_slot"
	CapacityMemoryPressure  CapacityDimension = "memory_pressure"
)

type CapacityError struct {
	Dimension CapacityDimension
}

func (err *CapacityError) Error() string { return string(err.Dimension) + " capacity exhausted" }

func ExhaustedDimension(err error) CapacityDimension {
	var capacity *CapacityError
	if errors.As(err, &capacity) {
		return capacity.Dimension
	}
	return ""
}

const MaxStoredWorkloads = 256

type Contract struct {
	CPUMaxMillis       uint64
	MemoryMaxBytes     uint64
	PIDMax             uint64
	WritableQuotaBytes uint64
}

func (contract Contract) Validate(nodeCPUCapacity uint64) error {
	if contract.CPUMaxMillis == 0 || contract.MemoryMaxBytes < 16<<20 || contract.PIDMax == 0 || contract.WritableQuotaBytes == 0 {
		return errors.New("workload resource limits are outside the supported range")
	}
	if contract.CPUMaxMillis > nodeCPUCapacity {
		return errors.New("cpu max exceeds node capacity")
	}
	return nil
}

func (contract Contract) Fits(limit Contract) error {
	switch {
	case contract.CPUMaxMillis > limit.CPUMaxMillis:
		return &CapacityError{Dimension: CapacityCPUMax}
	case contract.MemoryMaxBytes > limit.MemoryMaxBytes:
		return &CapacityError{Dimension: CapacityMemory}
	case contract.PIDMax > limit.PIDMax:
		return &CapacityError{Dimension: CapacityPIDs}
	case contract.WritableQuotaBytes > limit.WritableQuotaBytes:
		return &CapacityError{Dimension: CapacityWritable}
	default:
		return nil
	}
}

func (contract Contract) CgroupValues() map[string]string {
	return map[string]string{
		"cpu.weight":  "100",
		"cpu.max":     strconv.FormatUint(contract.CPUMaxMillis*100, 10) + " 100000",
		"memory.high": "max",
		"memory.max":  strconv.FormatUint(contract.MemoryMaxBytes, 10),
		"pids.max":    strconv.FormatUint(contract.PIDMax, 10),
	}
}

// NamespaceCgroup separates appliance processes from ordinary workload costs.
func NamespaceCgroup(namespaceID string) string {
	group := "workload"
	if strings.HasPrefix(namespaceID, "system.") {
		group = "system"
	}
	return filepath.Join("/agent-vmm", group, namespaceID)
}
