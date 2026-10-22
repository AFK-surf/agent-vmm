//go:build linux && amd64

package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"fmt"
	"os"
	"unsafe"
)

// SupportedHyperVCPUID discovers accelerator interfaces without enabling them.
func (v *VM) SupportedHyperVCPUID() ([]CPUIDEntry, error) {
	m := v.machine
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, os.ErrClosed
	}
	return m.supportedHyperVCPUID()
}
func (m *machine) supportedHyperVCPUID() ([]CPUIDEntry, error) {
	supported, err := ioctl(uintptr(m.kvmfd), 0xae03, 191)
	if err != nil {
		return nil, err
	}
	if supported == 0 {
		return nil, fmt.Errorf("KVM lacks Hyper-V CPUID discovery")
	}
	table := struct {
		Count, Padding uint32
		Entries        [256]CPUIDEntry
	}{Count: 256}
	if _, err := ioctl(uintptr(m.kvmfd), 0xc008aec1, uintptr(unsafe.Pointer(&table))); err != nil {
		return nil, fmt.Errorf("KVM Hyper-V CPUID: %w", err)
	}
	if table.Count > uint32(len(table.Entries)) {
		return nil, fmt.Errorf("invalid KVM Hyper-V CPUID count %d", table.Count)
	}
	return append([]CPUIDEntry(nil), table.Entries[:table.Count]...), nil
}
