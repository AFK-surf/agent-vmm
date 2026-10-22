//go:build linux && amd64

package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const uartBase = 0x3f8
const uartIRQ = 4
const amd64MMIOBase = 0xc0000000

func (m *machine) memoryBase() uint64 { return 0 }

// createVM creates the in-kernel irqchip before any vCPU exists, as KVM requires.
func (m *machine) createVM() error {
	if m.cfg.CPUs != 1 {
		return fmt.Errorf("amd64 KVM supports one vCPU because the boot layout has no MP table, requested %d", m.cfg.CPUs)
	}
	if m.cfg.MemoryBytes > amd64MMIOBase {
		return fmt.Errorf("amd64 KVM guest RAM must end below the MMIO window at %#x, requested %d bytes", amd64MMIOBase, m.cfg.MemoryBytes)
	}
	fd, err := ioctl(uintptr(m.kvmfd), 0xae01, 0)
	if err != nil {
		return err
	}
	m.vmfd = int(fd)
	if _, err := ioctl(uintptr(m.vmfd), 0xae47, 0xfffbd000); err != nil {
		return fmt.Errorf("set KVM TSS address: %w", err)
	}
	if _, err := ioctl(uintptr(m.vmfd), 0xae60, 0); err != nil {
		return fmt.Errorf("create KVM irqchip: %w", err)
	}
	pit := [64]byte{}
	if _, err := ioctl(uintptr(m.vmfd), 0x4040ae77, uintptr(unsafe.Pointer(&pit[0]))); err != nil {
		return fmt.Errorf("create KVM PIT: %w", err)
	}
	return nil
}
func validatePlatformDevice(d MMIODevice) error {
	if d.IRQ < 5 || d.IRQ > 15 || d.IRQ == 8 || d.IRQ == 13 {
		return fmt.Errorf("amd64 virtio IRQ must be a free ISA IRQ (5..15 excluding 8 and 13)")
	}
	if d.Base < amd64MMIOBase || d.Base+d.Size > 0xf0000000 {
		return fmt.Errorf("amd64 virtio MMIO must lie between 3 GiB and 3.75 GiB")
	}
	return nil
}
func encodeIRQ(irq uint32) uint32 { return irq }

func (m *machine) prepareCPUs(fds []int) (func(), error) {
	fail := func(err error) (func(), error) { return nil, err }
	fd := fds[0]
	cpuid := struct {
		Count, Padding uint32
		Entries        [256]CPUIDEntry
	}{Count: 256}
	if _, err := ioctl(uintptr(m.kvmfd), 0xc008ae05, uintptr(unsafe.Pointer(&cpuid))); err != nil {
		return fail(fmt.Errorf("get supported KVM CPUID: %w", err))
	}
	if m.cfg.EnableHyperV {
		leaves, err := m.supportedHyperVCPUID()
		if err != nil {
			return fail(err)
		}
		merged, err := hyperVClockCPUID(cpuid.Entries[:cpuid.Count], leaves)
		if err != nil {
			return fail(err)
		}
		if len(merged) > len(cpuid.Entries) {
			return fail(fmt.Errorf("KVM CPUID table exceeds capacity"))
		}
		copy(cpuid.Entries[:], merged)
		cpuid.Count = uint32(len(merged))
	}
	if _, err := ioctl(uintptr(fd), 0x4008ae90, uintptr(unsafe.Pointer(&cpuid))); err != nil {
		return fail(fmt.Errorf("set KVM CPUID: %w", err))
	}
	boot, err := prepareAMD64Boot(m.mem, m.cfg, m.devices)
	if err != nil {
		return fail(err)
	}
	var sregs x86SystemRegisters
	if _, err := ioctl(uintptr(fd), 0x8138ae83, uintptr(unsafe.Pointer(&sregs))); err != nil {
		return fail(err)
	}
	sregs.Cs = x86Segment{Base: 0, Limit: 0xffffffff, Selector: 0x10, Type: 11, Present: 1, Db: 1, S: 1, G: 1}
	data := x86Segment{Base: 0, Limit: 0xffffffff, Selector: 0x18, Type: 3, Present: 1, Db: 1, S: 1, G: 1}
	sregs.Ds = data
	sregs.Es = data
	sregs.Fs = data
	sregs.Gs = data
	sregs.Ss = data
	sregs.Cr0 = 1
	sregs.Cr4 = 0
	sregs.Efer = 0
	if _, err := ioctl(uintptr(fd), 0x4138ae84, uintptr(unsafe.Pointer(&sregs))); err != nil {
		return fail(fmt.Errorf("set protected mode: %w", err))
	}
	regs := x86Registers{Rip: boot.entry, Rsi: 0x90000, Rsp: 0x8ff0, Rflags: 2}
	if _, err := ioctl(uintptr(fd), 0x4090ae82, uintptr(unsafe.Pointer(&regs))); err != nil {
		return fail(fmt.Errorf("set x86 boot registers: %w", err))
	}
	return func() {}, nil
}

func (m *machine) handleExit(reason uint32, run []byte) (bool, error) {
	switch reason {
	case 2:
		out := run[32] == 1
		size := uint64(run[33])
		port := binary.LittleEndian.Uint16(run[34:36])
		count := uint64(binary.LittleEndian.Uint32(run[36:40]))
		offset := binary.LittleEndian.Uint64(run[40:48])
		if size == 0 || size > 4 || offset > uint64(len(run)) || count > (uint64(len(run))-offset)/size {
			return false, fmt.Errorf("invalid KVM port IO exit")
		}
		for i := uint64(0); i < count; i++ {
			data := run[offset+i*size : offset+(i+1)*size]
			if port >= uartBase && port < uartBase+8 {
				if err := m.serial.access(uint64(port-uartBase), data, out); err != nil {
					return false, err
				}
				continue
			}
			if out && port == 0x604 && size == 2 && binary.LittleEndian.Uint16(data)&0x2000 != 0 {
				return true, nil
			}
			// Linux probes legacy PC ports even with ACPI and PCI disabled.
			if !out {
				for j := range data {
					data[j] = 0xff
				}
			}
		}
		return false, nil
	case 5:
		return false, nil // In-kernel irqchip wakes HLT when the next interrupt arrives.
	case 8:
		return false, fmt.Errorf("x86 guest triple fault")
	case 27:
		return false, completeHyperVExit(run[32:])
	case 17:
		return false, fmt.Errorf("KVM internal error %d", binary.LittleEndian.Uint32(run[32:36]))
	default:
		return false, fmt.Errorf("unhandled amd64 KVM exit %d", reason)
	}
}
