//go:build darwin || linux

package hostapp

import "testing"

func TestVMResourcesDeriveSharedHostSizeWithinBounds(t *testing.T) {
	for _, test := range []struct {
		name            string
		cpus            uint
		memoryBytes     uint64
		hostCPUs        uint
		hostMemoryBytes uint64
		wantCPUs        uint
		wantMemoryBytes uint64
	}{
		{name: "8 GB host", hostCPUs: 8, hostMemoryBytes: 8 << 30, wantCPUs: 6, wantMemoryBytes: 4 << 30},
		{name: "16 GB host", hostCPUs: 8, hostMemoryBytes: 16 << 30, wantCPUs: 6, wantMemoryBytes: 8 << 30},
		{name: "workstation ceiling", hostCPUs: 24, hostMemoryBytes: 192 << 30, wantCPUs: 18, wantMemoryBytes: 32 << 30},
		{name: "small host floor", hostCPUs: 2, hostMemoryBytes: 3 << 30, wantCPUs: 2, wantMemoryBytes: 2 << 30},
		{name: "unaligned memory", hostCPUs: 10, hostMemoryBytes: 24<<30 + 3<<19, wantCPUs: 7, wantMemoryBytes: 12 << 30},
		{name: "explicit flags", cpus: 3, memoryBytes: 5 << 30, hostCPUs: 8, hostMemoryBytes: 16 << 30, wantCPUs: 3, wantMemoryBytes: 5 << 30},
	} {
		cpus, memoryBytes := vmResources(test.cpus, test.memoryBytes, test.hostCPUs, test.hostMemoryBytes)
		if cpus != test.wantCPUs || memoryBytes != test.wantMemoryBytes {
			t.Errorf("%s: vmResources() = %d CPUs, %d bytes; want %d CPUs, %d bytes", test.name, cpus, memoryBytes, test.wantCPUs, test.wantMemoryBytes)
		}
	}
}

func TestHostResourcesReadsThisHost(t *testing.T) {
	cpus, memoryBytes, err := hostResources()
	if err != nil || cpus == 0 || memoryBytes == 0 {
		t.Fatalf("hostResources() = %d, %d, %v", cpus, memoryBytes, err)
	}
}
