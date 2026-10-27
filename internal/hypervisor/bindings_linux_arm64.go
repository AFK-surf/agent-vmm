//go:build linux && arm64

package hypervisor

// SPDX-License-Identifier: Apache-2.0
// Derived from tinyrange/cc commit eed53ffb414a8bebcbb1d7934fab00558d78df00; modified for agent-vmm.
// Modified for agent-vmm; see LICENSE-cc.txt.

import "unsafe"

func setOneReg(fd int, id uint64, addr unsafe.Pointer) error {
	reg := kvmOneReg{id: id, addr: uint64(uintptr(addr))}
	_, err := ioctl(uintptr(fd), 0x4010aeac, uintptr(unsafe.Pointer(&reg)))
	return err
}

func setDeviceAttr(fd int, group uint32, attr uint64, value unsafe.Pointer) error {
	data := kvmDeviceAttr{Group: group, Attr: attr, Addr: uint64(uintptr(value))}
	_, err := ioctl(uintptr(fd), 0x4018aee1, uintptr(unsafe.Pointer(&data)))
	return err
}
