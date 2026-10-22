// Portions derived from tinyrange/cc, commit eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Licensed under the Apache License, Version 2.0; see ../LICENSE-cc.txt.
package virtio

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

type GuestMemory interface {
	ReadIPA(addr uint64, size int) ([]byte, error)
	WriteIPA(addr uint64, data []byte) error
}

type Memory struct {
	Base uint64
	Data []byte
}

func (m Memory) ReadIPA(addr uint64, size int) ([]byte, error) {
	if size < 0 || addr < m.Base || addr-m.Base > uint64(len(m.Data)) || uint64(size) > uint64(len(m.Data))-(addr-m.Base) {
		return nil, fmt.Errorf("guest memory range %#x+%d outside RAM", addr, size)
	}
	return m.Data[addr-m.Base : addr-m.Base+uint64(size)], nil
}

func (m Memory) WriteIPA(addr uint64, data []byte) error {
	out, err := m.ReadIPA(addr, len(data))
	if err == nil {
		copy(out, data)
	}
	return err
}

// Queue index accesses publish descriptor contents across the vCPU and device threads.
func (m Memory) readQueueIndex(addr uint64) (uint16, error) {
	raw, err := m.ReadIPA(addr&^3, 4)
	if err != nil {
		return 0, err
	}
	word := atomic.LoadUint32((*uint32)(unsafe.Pointer(&raw[0])))
	return uint16(word >> ((addr & 2) * 8)), nil
}
func (m Memory) writeQueueIndex(addr uint64, value uint16) error {
	raw, err := m.ReadIPA(addr&^3, 4)
	if err != nil {
		return err
	}
	ptr := (*uint32)(unsafe.Pointer(&raw[0]))
	shift := (addr & 2) * 8
	for {
		old := atomic.LoadUint32(ptr)
		next := (old &^ (uint32(0xffff) << shift)) | uint32(value)<<shift
		if atomic.CompareAndSwapUint32(ptr, old, next) {
			return nil
		}
	}
}
