//go:build linux && amd64

package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

type x86Registers struct {
	Rax, Rbx, Rcx, Rdx, Rsi, Rdi, Rsp, Rbp uint64
	R8, R9, R10, R11, R12, R13, R14, R15   uint64
	Rip, Rflags                            uint64
}
type x86Segment struct {
	Base                                                    uint64
	Limit                                                   uint32
	Selector                                                uint16
	Type, Present, Dpl, Db, S, L, G, Avl, Unusable, Padding uint8
}
type x86DescriptorTable struct {
	Base    uint64
	Limit   uint16
	Padding [3]uint16
}
type x86SystemRegisters struct {
	Cs, Ds, Es, Fs, Gs, Ss, Tr, Ldt         x86Segment
	Gdt, Idt                                x86DescriptorTable
	Cr0, Cr2, Cr3, Cr4, Cr8, Efer, ApicBase uint64
	InterruptBitmap                         [4]uint64
}
