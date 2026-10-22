//go:build windows && amd64

package whp

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see ../LICENSE-cc.txt.

import (
	"golang.org/x/sys/windows"
	"unsafe"
)

func configureInterruptController(part windows.Handle) error {
	xapic := uint32(1)
	return call(setProperty, uintptr(part), 0x1005, uintptr(unsafe.Pointer(&xapic)), 4)
}
func injectInterrupt(part windows.Handle, vector uint32, asserted bool) error {
	if !asserted {
		return nil
	}
	control := struct {
		Control             uint64
		Destination, Vector uint32
	}{Vector: vector}
	return call(requestInterrupt, uintptr(part), uintptr(unsafe.Pointer(&control)), unsafe.Sizeof(control))
}
