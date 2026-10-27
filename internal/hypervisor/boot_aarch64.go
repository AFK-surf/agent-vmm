package hypervisor

// SPDX-License-Identifier: Apache-2.0
// Derived from tinyrange/cc commit eed53ffb414a8bebcbb1d7934fab00558d78df00; modified for agent-vmm.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"encoding/binary"
	"fmt"

	"github.com/AFK-surf/agent-vmm/internal/hypervisor/fdt"
)

const (
	arm64RAMBase          = 0x40000000
	arm64GICDistributor   = 0x08000000
	arm64GICRedistributor = 0x080a0000
	arm64UARTBase         = 0x09000000
	// Each GICv3 redistributor has RD and SGI frames; the region must end below the UART.
	arm64RedistributorStride = 0x20000
	arm64MaxCPUs             = (arm64UARTBase - arm64GICRedistributor) / arm64RedistributorStride
)

type arm64Boot struct{ entry, dtb uint64 }

func prepareARM64Boot(mem []byte, cfg Config, devices []MMIODevice) (arm64Boot, error) {
	image := cfg.Kernel
	if len(image) < 64 || binary.LittleEndian.Uint32(image[56:60]) != 0x644d5241 {
		return arm64Boot{}, fmt.Errorf("kernel is not an uncompressed ARM64 Image")
	}
	textOffset := binary.LittleEndian.Uint64(image[8:16])
	reserve := max(binary.LittleEndian.Uint64(image[16:24]), uint64(len(image)))
	if textOffset > uint64(len(mem)) || reserve > uint64(len(mem))-textOffset {
		return arm64Boot{}, fmt.Errorf("ARM64 Image does not fit in guest RAM")
	}
	copy(mem[textOffset:], image)
	initrdOffset := alignBoot(textOffset+reserve, 4096)
	if initrdOffset > uint64(len(mem)) || uint64(len(cfg.Initramfs)) > uint64(len(mem))-initrdOffset {
		return arm64Boot{}, fmt.Errorf("initramfs does not fit in guest RAM")
	}
	copy(mem[initrdOffset:], cfg.Initramfs)
	tree, err := arm64DeviceTree(cfg, devices, arm64RAMBase+initrdOffset)
	if err != nil {
		return arm64Boot{}, err
	}
	dtbOffset := alignBoot(initrdOffset+uint64(len(cfg.Initramfs)), 2<<20)
	if dtbOffset > uint64(len(mem)) || uint64(len(tree)) > uint64(len(mem))-dtbOffset {
		return arm64Boot{}, fmt.Errorf("device tree does not fit in guest RAM")
	}
	copy(mem[dtbOffset:], tree)
	return arm64Boot{entry: arm64RAMBase + textOffset, dtb: arm64RAMBase + dtbOffset}, nil
}

func alignBoot(n, alignment uint64) uint64 { return (n + alignment - 1) &^ (alignment - 1) }

// arm64MPIDR matches the affinity that KVM assigns to each vCPU index.
func arm64MPIDR(cpu int) uint64 {
	id := uint64(cpu)
	return id&0xf | (id>>4&0xff)<<8 | (id>>12&0xff)<<16
}

func arm64DeviceTree(cfg Config, devices []MMIODevice, initrd uint64) ([]byte, error) {
	root := fdt.Node{Name: "", Properties: map[string]fdt.Property{
		"#address-cells": {U32: []uint32{2}}, "#size-cells": {U32: []uint32{2}},
		"compatible": {Strings: []string{"agent-vmm,arm64"}}, "model": {Strings: []string{"agent-vmm"}},
		"interrupt-parent": {U32: []uint32{1}},
	}}
	chosen := fdt.Node{Name: "chosen", Properties: map[string]fdt.Property{
		"bootargs": {Strings: []string{cfg.CommandLine}}, "stdout-path": {Strings: []string{"/serial@9000000:115200n8"}},
	}}
	if len(cfg.Initramfs) > 0 {
		chosen.Properties["linux,initrd-start"] = fdt.Property{U64: []uint64{initrd}}
		chosen.Properties["linux,initrd-end"] = fdt.Property{U64: []uint64{initrd + uint64(len(cfg.Initramfs))}}
	}
	cpus := fdt.Node{Name: "cpus", Properties: map[string]fdt.Property{"#address-cells": {U32: []uint32{2}}, "#size-cells": {U32: []uint32{0}}}}
	for i := range cfg.CPUs {
		mpidr := arm64MPIDR(i)
		cpus.Children = append(cpus.Children, fdt.Node{Name: fmt.Sprintf("cpu@%x", mpidr), Properties: map[string]fdt.Property{
			"device_type": {Strings: []string{"cpu"}}, "compatible": {Strings: []string{"arm,armv8"}}, "reg": {U64: []uint64{mpidr}}, "enable-method": {Strings: []string{"psci"}},
		}})
	}
	root.Children = []fdt.Node{
		cpus,
		{Name: "memory@40000000", Properties: map[string]fdt.Property{"device_type": {Strings: []string{"memory"}}, "reg": {U64: []uint64{0x40000000, cfg.MemoryBytes}}}},
		chosen,
		{Name: "psci", Properties: map[string]fdt.Property{"compatible": {Strings: []string{"arm,psci-0.2"}}, "method": {Strings: []string{"hvc"}}}},
		{Name: "timer", Properties: map[string]fdt.Property{"compatible": {Strings: []string{"arm,armv8-timer"}}, "always-on": {Flag: true}, "interrupts": {U32: []uint32{1, 13, 4, 1, 14, 4, 1, 11, 4, 1, 10, 4}}}},
		{Name: "interrupt-controller@8000000", Properties: map[string]fdt.Property{
			"compatible": {Strings: []string{"arm,gic-v3"}}, "#interrupt-cells": {U32: []uint32{3}}, "#address-cells": {U32: []uint32{2}}, "#size-cells": {U32: []uint32{2}},
			"interrupt-controller": {Flag: true}, "phandle": {U32: []uint32{1}}, "ranges": {Flag: true},
			"reg": {U64: []uint64{arm64GICDistributor, 0x10000, arm64GICRedistributor, uint64(cfg.CPUs) * arm64RedistributorStride}},
		}},

		{Name: "apb-pclk", Properties: map[string]fdt.Property{
			"compatible": {Strings: []string{"fixed-clock"}}, "#clock-cells": {U32: []uint32{0}}, "clock-frequency": {U32: []uint32{24000000}}, "phandle": {U32: []uint32{2}},
		}},
		{Name: "rtc@9010000", Properties: map[string]fdt.Property{
			"compatible": {Strings: []string{"arm,pl031", "arm,primecell"}}, "reg": {U64: []uint64{arm64RTCBase, 0x1000}}, "clocks": {U32: []uint32{2}}, "clock-names": {Strings: []string{"apb_pclk"}},
		}},
		{Name: "serial@9000000", Properties: map[string]fdt.Property{
			"compatible": {Strings: []string{"ns16550a"}}, "interrupts": {U32: []uint32{0, arm64UARTIRQ - 32, 4}}, "reg": {U64: []uint64{arm64UARTBase, 8}}, "reg-io-width": {U32: []uint32{1}}, "clock-frequency": {U32: []uint32{1843200}},
		}},
	}
	for _, d := range devices {
		root.Children = append(root.Children, fdt.Node{Name: fmt.Sprintf("virtio_mmio@%x", d.Base), Properties: map[string]fdt.Property{
			"compatible": {Strings: []string{"virtio,mmio"}}, "reg": {U64: []uint64{d.Base, d.Size}}, "interrupts": {U32: []uint32{0, d.IRQ - 32, 4}}, "dma-coherent": {Flag: true},
		}})
	}
	return fdt.Build(root)
}
