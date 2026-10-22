package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"encoding/binary"
	"fmt"
)

// CPUIDEntry has the KVM CPUID2 ABI layout.
type CPUIDEntry struct {
	Function, Index, Flags, Eax, Ebx, Ecx, Edx uint32
	Padding                                    [3]uint32
}

// hyperVClockCPUID retains only interfaces implemented entirely by KVM. SynIC,
// synthetic timers, VMBus, and userspace hypercall services are not advertised.
func hyperVClockCPUID(native, supported []CPUIDEntry) ([]CPUIDEntry, error) {
	const clockMask = uint32(1<<1 | 1<<5 | 1<<6 | 1<<9)
	var features uint32
	for _, e := range supported {
		if e.Function == 0x40000003 {
			features = e.Eax & clockMask
		}
	}
	if features&(1<<1) == 0 {
		return nil, fmt.Errorf("KVM lacks Hyper-V reference counter support")
	}
	result := make([]CPUIDEntry, 0, len(native)+6)
	for _, e := range native {
		if e.Function < 0x40000000 || e.Function >= 0x50000000 {
			result = append(result, e)
		}
	}
	result = append(result,
		CPUIDEntry{Function: 0x40000000, Eax: 0x40000005, Ebx: 0x7263694d, Ecx: 0x666f736f, Edx: 0x76482074},
		CPUIDEntry{Function: 0x40000001, Eax: 0x31237648},
		CPUIDEntry{Function: 0x40000002},
		CPUIDEntry{Function: 0x40000003, Eax: features},
		CPUIDEntry{Function: 0x40000004},
		CPUIDEntry{Function: 0x40000005, Eax: 1, Ebx: 1},
	)
	return result, nil
}

func completeHyperVExit(data []byte) error {
	kind := binary.LittleEndian.Uint32(data)
	if kind != 2 {
		return fmt.Errorf("unsupported Hyper-V exit type %d", kind)
	}
	binary.LittleEndian.PutUint64(data[16:24], 2) // HV_STATUS_INVALID_HYPERCALL_CODE
	return nil
}
