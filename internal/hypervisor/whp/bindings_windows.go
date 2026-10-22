//go:build windows && (amd64 || arm64)

// Package whp retains the Windows Hypervisor Platform primitives. Boot and
// device emulation are not yet connected to the product VM manager on Windows.
package whp

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see ../LICENSE-cc.txt.

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

var platform = windows.NewLazySystemDLL("WinHvPlatform.dll")
var (
	getCapability    = platform.NewProc("WHvGetCapability")
	createPartition  = platform.NewProc("WHvCreatePartition")
	setupPartition   = platform.NewProc("WHvSetupPartition")
	deletePartition  = platform.NewProc("WHvDeletePartition")
	setProperty      = platform.NewProc("WHvSetPartitionProperty")
	mapGPA           = platform.NewProc("WHvMapGpaRange")
	createVP         = platform.NewProc("WHvCreateVirtualProcessor")
	deleteVP         = platform.NewProc("WHvDeleteVirtualProcessor")
	runVP            = platform.NewProc("WHvRunVirtualProcessor")
	cancelVP         = platform.NewProc("WHvCancelRunVirtualProcessor")
	getRegisters     = platform.NewProc("WHvGetVirtualProcessorRegisters")
	setRegisters     = platform.NewProc("WHvSetVirtualProcessorRegisters")
	requestInterrupt = platform.NewProc("WHvRequestInterrupt")
)

func call(proc *windows.LazyProc, args ...uintptr) error {
	if err := proc.Find(); err != nil {
		return err
	}
	value, _, _ := proc.Call(args...)
	if int32(value) < 0 {
		return fmt.Errorf("%s: HRESULT %#08x", proc.Name, uint32(value))
	}
	return nil
}

func Present() (bool, error) {
	var value, written uint32
	err := call(getCapability, 0, uintptr(unsafe.Pointer(&value)), 4, uintptr(unsafe.Pointer(&written)))
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

type RegisterValue [16]byte

func Uint64Register(value uint64) RegisterValue {
	var v RegisterValue
	binary.LittleEndian.PutUint64(v[:], value)
	return v
}

func registers(proc *windows.LazyProc, partition windows.Handle, names []uint32, values []RegisterValue) error {
	if len(names) != len(values) {
		return fmt.Errorf("register names and values have different lengths")
	}
	if len(names) == 0 {
		return nil
	}
	// WHP requires 16-byte alignment; Go only promises byte alignment for this type.
	backing := make([]byte, len(values)*16+15)
	start := unsafe.Pointer(&backing[0])
	aligned := unsafe.Slice((*RegisterValue)(unsafe.Add(start, -uintptr(start)&15)), len(values))
	if proc == setRegisters {
		copy(aligned, values)
	}
	err := call(proc, uintptr(partition), 0, uintptr(unsafe.Pointer(&names[0])), uintptr(len(names)), uintptr(unsafe.Pointer(&aligned[0])))
	if err == nil && proc == getRegisters {
		copy(values, aligned)
	}
	runtime.KeepAlive(names)
	runtime.KeepAlive(values)
	runtime.KeepAlive(backing)
	return err
}
