//go:build linux && arm64

package hypervisor

// SPDX-License-Identifier: Apache-2.0
// Derived from tinyrange/cc commit eed53ffb414a8bebcbb1d7934fab00558d78df00; modified for agent-vmm.
// Modified for agent-vmm; see LICENSE-cc.txt.

type kvmVcpuInit struct {
	Target   uint32
	Features [7]uint32
}
type kvmOneReg struct{ id, addr uint64 }
type kvmCreateDeviceArgs struct{ Type, Fd, Flags uint32 }
type kvmDeviceAttr struct {
	Flags, Group uint32
	Attr, Addr   uint64
}
