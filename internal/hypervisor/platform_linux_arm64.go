//go:build linux && arm64

package hypervisor

// SPDX-License-Identifier: Apache-2.0
// Derived from tinyrange/cc commit eed53ffb414a8bebcbb1d7934fab00558d78df00; modified for agent-vmm.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

const uartBase = arm64UARTBase
const uartIRQ = arm64UARTIRQ

func (m *machine) memoryBase() uint64 { return arm64RAMBase }
func (m *machine) createVM() error {
	if m.cfg.CPUs > arm64MaxCPUs {
		return fmt.Errorf("arm64 layout fits at most %d GICv3 redistributors, requested %d vCPUs", arm64MaxCPUs, m.cfg.CPUs)
	}
	// Zero selects the kernel's default when configurable IPA size is unavailable.
	ipa, err := ioctl(uintptr(m.kvmfd), kvmCheckExtension, 165)
	if err != nil {
		return fmt.Errorf("KVM ARM IPA size: %w", err)
	}
	fd, err := ioctl(uintptr(m.kvmfd), 0xae01, ipa)
	if err != nil {
		return err
	}
	m.vmfd = int(fd)
	return nil
}
func validatePlatformDevice(d MMIODevice) error {
	if d.IRQ < 32 || d.IRQ >= arm64UARTIRQ {
		return fmt.Errorf("device IRQ %d outside GIC SPI range 32..254 (255 is the UART)", d.IRQ)
	}
	if d.Base < 0x0a000000 || d.Base+d.Size > 0x10000000 {
		return fmt.Errorf("virtio MMIO range must lie in %#x..%#x", 0x0a000000, 0x10000000)
	}
	return nil
}
func encodeIRQ(irq uint32) uint32 { return 1<<24 | irq }

// prepareCPUs powers off secondary vCPUs until the guest starts them with PSCI CPU_ON.
// KVM sizes the GICv3 redistributor region from the vCPUs that exist at VGIC init.
func (m *machine) prepareCPUs(fds []int) (func(), error) {
	fail := func(err error) (func(), error) { return nil, err }
	var init kvmVcpuInit
	if _, err := ioctl(uintptr(m.vmfd), 0x8020aeaf, uintptr(unsafe.Pointer(&init))); err != nil {
		return fail(fmt.Errorf("KVM ARM preferred target: %w", err))
	}
	init.Features[0] |= 1 << 2 // KVM_ARM_VCPU_PSCI_0_2
	for i, fd := range fds {
		cpu := init
		if i > 0 {
			cpu.Features[0] |= 1 // KVM_ARM_VCPU_POWER_OFF
		}
		if _, err := ioctl(uintptr(fd), 0x4020aeae, uintptr(unsafe.Pointer(&cpu))); err != nil {
			return fail(fmt.Errorf("KVM ARM vCPU %d init: %w", i, err))
		}
	}
	dev := kvmCreateDeviceArgs{Type: 7}
	if _, err := ioctl(uintptr(m.vmfd), 0xc00caee0, uintptr(unsafe.Pointer(&dev))); err != nil {
		return fail(fmt.Errorf("KVM requires VGICv3: %w", err))
	}
	cleanup := func() { _ = unix.Close(int(dev.Fd)) }
	count := uint32(256)
	dist := uint64(arm64GICDistributor)
	redist := uint64(arm64GICRedistributor)
	for _, attr := range []struct {
		group uint32
		attr  uint64
		value unsafe.Pointer
	}{
		{3, 0, unsafe.Pointer(&count)}, {0, 2, unsafe.Pointer(&dist)}, {0, 3, unsafe.Pointer(&redist)}, {4, 0, nil},
	} {
		if err := setDeviceAttr(int(dev.Fd), attr.group, attr.attr, attr.value); err != nil {
			cleanup()
			return fail(fmt.Errorf("configure VGICv3: %w", err))
		}
	}
	boot, err := prepareARM64Boot(m.mem, m.cfg, m.devices)
	if err != nil {
		cleanup()
		return fail(err)
	}
	for _, r := range []struct{ id, value uint64 }{{regPC, boot.entry}, {regPState, 0x3c5}, {regX(0), boot.dtb}} {
		if err := setOneReg(fds[0], r.id, unsafe.Pointer(&r.value)); err != nil {
			cleanup()
			return fail(fmt.Errorf("initialize ARM64 register %#x: %w", r.id, err))
		}
	}
	return cleanup, nil
}

func (m *machine) handleExit(reason uint32, run []byte) error {
	switch reason {
	case 8:
		return fmt.Errorf("KVM guest shutdown without poweroff")
	case 17:
		return fmt.Errorf("KVM internal error %d", *(*uint32)(unsafe.Pointer(&run[32])))
	default:
		return fmt.Errorf("unhandled ARM64 KVM exit %d", reason)
	}
}
