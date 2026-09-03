package resourcev2

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
)

type CapacityDimension string

const (
	CapacityCPUGuarantee    CapacityDimension = "cpu_guarantee"
	CapacityCPUMax          CapacityDimension = "cpu_max"
	CapacityMemory          CapacityDimension = "memory"
	CapacityPIDs            CapacityDimension = "pids"
	CapacityWritable        CapacityDimension = "writable"
	CapacityStorageHeadroom CapacityDimension = "storage_headroom"
	CapacityImportSlot      CapacityDimension = "import_slot"
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

const MaxCPUOversubscriptionNumerator uint64 = 3
const MaxCPUOversubscriptionDenominator uint64 = 2

type Contract struct {
	CPUGuaranteeMillis uint64
	CPUMaxMillis       uint64
	CPUWeight          uint64
	MemoryHighBytes    uint64
	MemoryMaxBytes     uint64
	PIDMax             uint64
	WritableQuotaBytes uint64
}

type Capacity struct {
	CPUGuaranteeMillis uint64
	CPUMaxMillis       uint64
	MemoryHighBytes    uint64
	MemoryMaxBytes     uint64
	PIDMax             uint64
	WritableQuotaBytes uint64
}

func (contract Contract) Validate(nodeCPUCapacity uint64) error {
	if contract.CPUGuaranteeMillis == 0 || contract.CPUMaxMillis == 0 || contract.CPUWeight < 1 || contract.CPUWeight > 10000 ||
		contract.MemoryHighBytes < 16<<20 || contract.MemoryMaxBytes < contract.MemoryHighBytes || contract.PIDMax == 0 || contract.WritableQuotaBytes == 0 {
		return errors.New("resource-v2 values are outside the supported range")
	}
	if contract.CPUGuaranteeMillis > contract.CPUMaxMillis || contract.CPUMaxMillis > nodeCPUCapacity {
		return errors.New("cpu guarantee must not exceed max or node capacity")
	}
	return nil
}

func (capacity Capacity) Add(contract Contract) Capacity {
	return Capacity{
		CPUGuaranteeMillis: capacity.CPUGuaranteeMillis + contract.CPUGuaranteeMillis,
		CPUMaxMillis:       capacity.CPUMaxMillis + contract.CPUMaxMillis,
		MemoryHighBytes:    capacity.MemoryHighBytes + contract.MemoryHighBytes,
		MemoryMaxBytes:     capacity.MemoryMaxBytes + contract.MemoryMaxBytes,
		PIDMax:             capacity.PIDMax + contract.PIDMax,
		WritableQuotaBytes: capacity.WritableQuotaBytes + contract.WritableQuotaBytes,
	}
}

// AdmitsShared applies the bounded shared-capacity contract. MemoryHighBytes is
// the reclaimable admission entitlement; MemoryMaxBytes remains a per-workload
// cgroup safety ceiling and is intentionally not reserved across workloads.
func (used Capacity) AdmitsShared(contract Contract, limit Capacity) error {
	switch {
	case contract.CPUGuaranteeMillis > limit.CPUGuaranteeMillis || used.CPUGuaranteeMillis > limit.CPUGuaranteeMillis-contract.CPUGuaranteeMillis:
		return &CapacityError{Dimension: CapacityCPUGuarantee}
	case contract.CPUMaxMillis > limit.CPUMaxMillis || used.CPUMaxMillis > limit.CPUMaxMillis-contract.CPUMaxMillis:
		return &CapacityError{Dimension: CapacityCPUMax}
	case contract.MemoryHighBytes > limit.MemoryHighBytes || used.MemoryHighBytes > limit.MemoryHighBytes-contract.MemoryHighBytes:
		return &CapacityError{Dimension: CapacityMemory}
	case contract.PIDMax > limit.PIDMax || used.PIDMax > limit.PIDMax-contract.PIDMax:
		return &CapacityError{Dimension: CapacityPIDs}
	case contract.WritableQuotaBytes > limit.WritableQuotaBytes || used.WritableQuotaBytes > limit.WritableQuotaBytes-contract.WritableQuotaBytes:
		return &CapacityError{Dimension: CapacityWritable}
	default:
		return nil
	}
}

// Admits preserves each independent resource invariant. Writable quota is a
// logical Host commitment; GuestDiskBudget separately proves physical backing.
func (used Capacity) Admits(contract Contract, limit Capacity) error {
	switch {
	case contract.CPUGuaranteeMillis > limit.CPUGuaranteeMillis || used.CPUGuaranteeMillis > limit.CPUGuaranteeMillis-contract.CPUGuaranteeMillis:
		return &CapacityError{Dimension: CapacityCPUGuarantee}
	case contract.CPUMaxMillis > limit.CPUMaxMillis || used.CPUMaxMillis > limit.CPUMaxMillis-contract.CPUMaxMillis:
		return &CapacityError{Dimension: CapacityCPUMax}
	case contract.MemoryMaxBytes > limit.MemoryMaxBytes || used.MemoryMaxBytes > limit.MemoryMaxBytes-contract.MemoryMaxBytes:
		return &CapacityError{Dimension: CapacityMemory}
	case contract.PIDMax > limit.PIDMax || used.PIDMax > limit.PIDMax-contract.PIDMax:
		return &CapacityError{Dimension: CapacityPIDs}
	case contract.WritableQuotaBytes > limit.WritableQuotaBytes || used.WritableQuotaBytes > limit.WritableQuotaBytes-contract.WritableQuotaBytes:
		return &CapacityError{Dimension: CapacityWritable}
	default:
		return nil
	}
}

func NodeCapacity(cpuMillis, memoryBytes, pidMax, writableQuotaBytes uint64) Capacity {
	return Capacity{
		CPUGuaranteeMillis: cpuMillis,
		CPUMaxMillis:       cpuMillis * MaxCPUOversubscriptionNumerator / MaxCPUOversubscriptionDenominator,
		MemoryHighBytes:    memoryBytes,
		MemoryMaxBytes:     memoryBytes,
		PIDMax:             pidMax,
		WritableQuotaBytes: writableQuotaBytes,
	}
}

func (contract Contract) CgroupValues() map[string]string {
	return map[string]string{
		"cpu.weight":  strconv.FormatUint(contract.CPUWeight, 10),
		"cpu.max":     strconv.FormatUint(contract.CPUMaxMillis*100, 10) + " 100000",
		"memory.high": strconv.FormatUint(contract.MemoryHighBytes, 10),
		"memory.max":  strconv.FormatUint(contract.MemoryMaxBytes, 10),
		"pids.max":    strconv.FormatUint(contract.PIDMax, 10),
	}
}

func (contract Contract) Digest(allocationID string, generation uint64, namespaceID string) [32]byte {
	buffer := make([]byte, 0, len(allocationID)+len(namespaceID)+8*8+2)
	buffer = append(buffer, allocationID...)
	buffer = append(buffer, 0)
	buffer = binary.BigEndian.AppendUint64(buffer, generation)
	buffer = append(buffer, namespaceID...)
	buffer = append(buffer, 0)
	for _, value := range []uint64{contract.CPUGuaranteeMillis, contract.CPUMaxMillis, contract.CPUWeight,
		contract.MemoryHighBytes, contract.MemoryMaxBytes, contract.PIDMax, contract.WritableQuotaBytes} {
		buffer = binary.BigEndian.AppendUint64(buffer, value)
	}
	return sha256.Sum256(buffer)
}
