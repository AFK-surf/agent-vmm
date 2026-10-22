package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"encoding/binary"
	"fmt"
	"strings"
)

type amd64Boot struct{ entry uint64 }

func prepareAMD64Boot(mem []byte, cfg Config, devices []MMIODevice) (amd64Boot, error) {
	kernel := cfg.Kernel
	if len(kernel) < 0x268 || binary.LittleEndian.Uint16(kernel[0x1fe:0x200]) != 0xaa55 || string(kernel[0x202:0x206]) != "HdrS" {
		return amd64Boot{}, fmt.Errorf("kernel is not a Linux bzImage")
	}
	if binary.LittleEndian.Uint16(kernel[0x206:0x208]) < 0x206 {
		return amd64Boot{}, fmt.Errorf("Linux bzImage boot protocol must be at least 2.06")
	}
	setup := int(kernel[0x1f1])
	if setup == 0 {
		setup = 4
	}
	payloadOffset := (setup + 1) * 512
	if payloadOffset >= len(kernel) {
		return amd64Boot{}, fmt.Errorf("bzImage setup exceeds kernel size")
	}
	load := uint64(0x100000)
	alignment := uint64(binary.LittleEndian.Uint32(kernel[0x230:0x234]))
	if kernel[0x234] != 0 && alignment > 0 {
		if alignment&(alignment-1) != 0 {
			return amd64Boot{}, fmt.Errorf("invalid bzImage kernel alignment")
		}
		load = (load + alignment - 1) &^ (alignment - 1)
	}
	payload := kernel[payloadOffset:]
	reserve := uint64(binary.LittleEndian.Uint32(kernel[0x260:0x264]))
	if reserve < uint64(len(payload)) {
		reserve = uint64(len(payload))
	}
	if load > uint64(len(mem)) || reserve > uint64(len(mem))-load {
		return amd64Boot{}, fmt.Errorf("bzImage does not fit in guest RAM")
	}
	copy(mem[load:], payload)
	const zeroPage = 0x90000
	const cmdlineGPA = 0x91000
	if len(mem) < cmdlineGPA+4096 {
		return amd64Boot{}, fmt.Errorf("guest RAM too small for Linux boot parameters")
	}
	copy(mem[zeroPage+0x1f1:zeroPage+0x268], kernel[0x1f1:0x268])
	zero := mem[zeroPage : zeroPage+4096]
	zero[0x210] = 0xff  // unregistered boot loader
	zero[0x211] |= 0x80 // loader owns heap
	binary.LittleEndian.PutUint32(zero[0x214:0x218], uint32(load))
	binary.LittleEndian.PutUint16(zero[0x224:0x226], 0xfe00)
	binary.LittleEndian.PutUint32(zero[0x228:0x22c], cmdlineGPA)
	binary.LittleEndian.PutUint16(zero[0x1fa:0x1fc], 0xffff) // no text framebuffer
	cmdline := cfg.CommandLine + " acpi=off noapic pci=off reboot=k"
	for _, d := range devices {
		cmdline += fmt.Sprintf(" virtio_mmio.device=%d@%#x:%d", d.Size, d.Base, d.IRQ)
	}
	cmdline = strings.TrimSpace(cmdline)
	maxCommand := binary.LittleEndian.Uint32(kernel[0x238:0x23c])
	if len(cmdline)+1 > 4096 || uint32(len(cmdline)) > maxCommand {
		return amd64Boot{}, fmt.Errorf("Linux kernel command line exceeds boot protocol limit")
	}
	copy(mem[cmdlineGPA:], cmdline)
	mem[cmdlineGPA+len(cmdline)] = 0
	if len(cfg.Initramfs) > 0 {
		top := uint64(len(mem))
		max := uint64(binary.LittleEndian.Uint32(kernel[0x22c:0x230])) + 1
		if max < top {
			top = max
		}
		if uint64(len(cfg.Initramfs)) > top {
			return amd64Boot{}, fmt.Errorf("initramfs does not fit below bzImage limit")
		}
		addr := (top - uint64(len(cfg.Initramfs))) &^ 4095
		if addr < load+reserve {
			return amd64Boot{}, fmt.Errorf("initramfs overlaps bzImage")
		}
		copy(mem[addr:], cfg.Initramfs)
		binary.LittleEndian.PutUint32(zero[0x218:0x21c], uint32(addr))
		binary.LittleEndian.PutUint32(zero[0x21c:0x220], uint32(len(cfg.Initramfs)))
	}
	entries := []struct {
		addr, size uint64
		typ        uint32
	}{{0, 0x9fc00, 1}, {0x9fc00, 0x60400, 2}, {0x100000, uint64(len(mem)) - 0x100000, 1}}
	zero[0x1e8] = byte(len(entries))
	for i, e := range entries {
		offset := 0x2d0 + i*20
		binary.LittleEndian.PutUint64(zero[offset:], e.addr)
		binary.LittleEndian.PutUint64(zero[offset+8:], e.size)
		binary.LittleEndian.PutUint32(zero[offset+16:], e.typ)
	}
	return amd64Boot{entry: load}, nil
}
