//go:build darwin

package main

import (
	"golang.org/x/sys/unix"
)

const (
	minimumVMCPUs        = uint(2)
	minimumVMMemoryBytes = uint64(2 << 30)
	maximumVMMemoryBytes = uint64(32 << 30)
)

// vmResources keeps explicit operator values and otherwise derives the shared
// VM size from the host. VZ vCPUs are host threads, not reserved cores, so
// three quarters of the cores keeps at least two cores for macOS when the
// guest saturates its CPUs (6 of 8 on base Macs). Guest memory becomes
// resident as Linux fills its page cache and is not reclaimed, so the guest
// gets half of physical memory: 4 GiB on an 8 GB Mac and 8 GiB on a 16 GB
// Mac keep the other half for macOS. The 2 GiB floor applies only below
// 4 GB of host memory; the 32 GiB ceiling keeps unreclaimed page cache from
// pinning most of a large workstation. Both values remain shared ceilings for
// all workloads and appliances, not per-workload reservations.
func vmResources(cpus uint, memoryBytes uint64, hostCPUs uint, hostMemoryBytes uint64) (uint, uint64) {
	if cpus == 0 {
		cpus = max(minimumVMCPUs, hostCPUs*3/4)
	}
	if memoryBytes == 0 {
		memoryBytes = min(max(minimumVMMemoryBytes, hostMemoryBytes/2), maximumVMMemoryBytes)
		memoryBytes -= memoryBytes % (1 << 20)
	}
	return cpus, memoryBytes
}

func hostResources() (uint, uint64, error) {
	cpus, err := unix.SysctlUint32("hw.ncpu")
	if err != nil {
		return 0, 0, err
	}
	memoryBytes, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, 0, err
	}
	return uint(cpus), memoryBytes, nil
}
