//go:build windows && arm64

package whp

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see ../LICENSE-cc.txt.

import (
	"golang.org/x/sys/windows"
	"unsafe"
)

func configureInterruptController(part windows.Handle) error {
	params := struct {
		Mode, Reserved                               uint32
		Distributor, ITSTranslator                   uint64
		Reserved2, LPIBits, TimerPPI, PerformancePPI uint32
		Reserved3                                    [6]uint32
	}{Mode: 1, Distributor: 0x08000000, ITSTranslator: 0x08080000, TimerPPI: 20, PerformancePPI: 23}
	return call(setProperty, uintptr(part), 0x1012, uintptr(unsafe.Pointer(&params)), unsafe.Sizeof(params))
}
func injectInterrupt(part windows.Handle, vector uint32, asserted bool) error {
	control := struct {
		Target, Control, Destination uint64
		Vector                       uint32
		VTL, Reserved                uint8
		Reserved2                    uint16
	}{Vector: vector}
	if asserted {
		control.Control = 1 << 34
	}
	return call(requestInterrupt, uintptr(part), uintptr(unsafe.Pointer(&control)), unsafe.Sizeof(control))
}
