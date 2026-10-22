//go:build linux && arm64

package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

func arm64CoreRegister(offsetBytes uintptr) uint64 {
	const arm64 = uint64(0x6000000000000000)
	const u64 = uint64(0x0030000000000000)
	const core = uint64(0x0010 << 16)
	return arm64 | u64 | core | uint64(offsetBytes/4)
}

var (
	regPC     = arm64CoreRegister(32 * 8)
	regPState = arm64CoreRegister(33 * 8)
)

func regX(index int) uint64 { return arm64CoreRegister(uintptr(index * 8)) }
