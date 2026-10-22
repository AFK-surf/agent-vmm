// Portions derived from tinyrange/cc, commit eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Licensed under the Apache License, Version 2.0; see ../LICENSE-cc.txt.
package virtio

import (
	"encoding/binary"
	"fmt"
)

const (
	maxQueueSize    = 256
	maxRequestBytes = 1 << 20
	descFNext       = 1
	descFWrite      = 2
)

type queue struct {
	size               uint16
	ready              bool
	desc, avail, used  uint64
	lastAvail, usedIdx uint16
}

type buffer struct {
	addr   uint64
	length uint32
	write  bool
}

type chain struct {
	generation uint64
	head       uint16
	request    []byte
	buffers    []buffer
}

func (q *queue) validate(mem GuestMemory) error {
	if q.size == 0 || q.size > maxQueueSize || q.size&(q.size-1) != 0 {
		return fmt.Errorf("invalid queue size %d", q.size)
	}
	for _, r := range []struct {
		addr  uint64
		size  int
		align uint64
	}{{q.desc, int(q.size) * 16, 16}, {q.avail, 6 + int(q.size)*2, 2}, {q.used, 6 + int(q.size)*8, 4}} {
		if r.addr%r.align != 0 {
			return fmt.Errorf("misaligned virtqueue address %#x", r.addr)
		}
		if _, err := mem.ReadIPA(r.addr, r.size); err != nil {
			return err
		}
	}
	return nil
}

func (q *queue) next(mem GuestMemory) (*chain, error) {
	available, err := readQueueIndex(mem, q.avail+2)
	if err != nil {
		return nil, err
	}
	if available == q.lastAvail {
		return nil, nil
	}
	if uint16(available-q.lastAvail) > q.size {
		return nil, fmt.Errorf("virtqueue available ring overrun")
	}
	a, err := mem.ReadIPA(q.avail+4+uint64(q.lastAvail%q.size)*2, 2)
	if err != nil {
		return nil, err
	}
	head := binary.LittleEndian.Uint16(a)
	c := &chain{head: head}
	index := head
	var total uint64
	writable := false
	for count := uint16(0); count < q.size; count++ {
		if index >= q.size {
			return nil, fmt.Errorf("descriptor %d outside queue", index)
		}
		raw, err := mem.ReadIPA(q.desc+uint64(index)*16, 16)
		if err != nil {
			return nil, err
		}
		b := buffer{addr: binary.LittleEndian.Uint64(raw), length: binary.LittleEndian.Uint32(raw[8:]), write: binary.LittleEndian.Uint16(raw[12:])&descFWrite != 0}
		flags := binary.LittleEndian.Uint16(raw[12:])
		next := binary.LittleEndian.Uint16(raw[14:])
		if flags & ^uint16(descFNext|descFWrite) != 0 {
			return nil, fmt.Errorf("unsupported descriptor flags %#x", flags)
		}
		total += uint64(b.length)
		if total > maxRequestBytes {
			return nil, fmt.Errorf("descriptor chain exceeds %d bytes", maxRequestBytes)
		}
		if writable && !b.write {
			return nil, fmt.Errorf("readable descriptor after writable descriptor")
		}
		writable = writable || b.write
		if _, err := mem.ReadIPA(b.addr, int(b.length)); err != nil {
			return nil, err
		}
		c.buffers = append(c.buffers, b)
		if flags&descFNext == 0 {
			return c, nil
		}
		index = next
	}
	return nil, fmt.Errorf("descriptor chain loop")
}

func (q *queue) complete(mem GuestMemory, c *chain, n uint32) error {
	var elem [8]byte
	binary.LittleEndian.PutUint32(elem[:], uint32(c.head))
	binary.LittleEndian.PutUint32(elem[4:], n)
	if err := mem.WriteIPA(q.used+4+uint64(q.usedIdx%q.size)*8, elem[:]); err != nil {
		return err
	}
	q.usedIdx++
	q.lastAvail++
	return writeQueueIndex(mem, q.used+2, q.usedIdx)
}

func (c *chain) readable(mem GuestMemory) ([]byte, error) {
	var out []byte
	for _, b := range c.buffers {
		if b.write {
			break
		}
		raw, err := mem.ReadIPA(b.addr, int(b.length))
		if err != nil {
			return nil, err
		}
		out = append(out, raw...)
	}
	return out, nil
}

func (c *chain) writableBytes() int {
	n := 0
	for _, b := range c.buffers {
		if b.write {
			n += int(b.length)
		}
	}
	return n
}

func (c *chain) write(mem GuestMemory, data []byte) error {
	if len(data) > c.writableBytes() {
		return fmt.Errorf("response exceeds writable descriptors")
	}
	for _, b := range c.buffers {
		if !b.write {
			continue
		}
		n := min(len(data), int(b.length))
		if err := mem.WriteIPA(b.addr, data[:n]); err != nil {
			return err
		}
		data = data[n:]
		if len(data) == 0 {
			return nil
		}
	}
	return nil
}

type queueIndexMemory interface {
	readQueueIndex(uint64) (uint16, error)
	writeQueueIndex(uint64, uint16) error
}

func readQueueIndex(mem GuestMemory, addr uint64) (uint16, error) {
	if shared, ok := mem.(queueIndexMemory); ok {
		return shared.readQueueIndex(addr)
	}
	raw, err := mem.ReadIPA(addr, 2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(raw), nil
}
func writeQueueIndex(mem GuestMemory, addr uint64, value uint16) error {
	if shared, ok := mem.(queueIndexMemory); ok {
		return shared.writeQueueIndex(addr, value)
	}
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], value)
	return mem.WriteIPA(addr, raw[:])
}
