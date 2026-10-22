package hypervisor

import (
	"io"
	"sync"
)

const arm64UARTIRQ = 255
const serialBufferBytes = 64 << 10

// uart16550 provides a transmit-only serial console, including the IRQ used by the Linux TTY driver.
type uart16550 struct {
	mu          sync.Mutex
	registers   [8]byte
	divisorHigh byte
	threPending bool
	irqHigh     bool
	irq         func(bool) error
	out         io.ByteWriter
}

// serialOutput keeps host writes off vCPU threads. A full buffer drops output.
type serialOutput struct {
	mu              sync.Mutex
	pending         []byte
	dropped, failed uint64
	closed          bool
	wake            chan struct{}
	done            chan struct{}
}

func newSerialOutput(w io.Writer) *serialOutput {
	s := &serialOutput{wake: make(chan struct{}, 1), done: make(chan struct{})}
	go s.drain(w)
	return s
}

func (s *serialOutput) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *serialOutput) WriteByte(b byte) error {
	s.mu.Lock()
	if len(s.pending) < serialBufferBytes {
		s.pending = append(s.pending, b)
	} else {
		s.dropped++
	}
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *serialOutput) drain(w io.Writer) {
	defer close(s.done)
	var batch []byte
	for {
		s.mu.Lock()
		batch, s.pending = s.pending, batch[:0]
		closed := s.closed
		s.mu.Unlock()
		if len(batch) > 0 {
			if _, err := w.Write(batch); err != nil {
				s.mu.Lock()
				s.failed += uint64(len(batch))
				s.mu.Unlock()
			}
			continue
		}
		if closed {
			return
		}
		<-s.wake
	}
}

func (s *serialOutput) loss() (dropped, failed uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped, s.failed
}

// close flushes buffered output after every vCPU has stopped.
func (s *serialOutput) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signal()
	<-s.done
}

func (u *uart16550) updateIRQ() error {
	high := u.registers[1]&2 != 0 && u.threPending
	if high == u.irqHigh {
		return nil
	}
	if u.irq != nil {
		if err := u.irq(high); err != nil {
			return err
		}
	}
	u.irqHigh = high
	return nil
}

func (u *uart16550) access(offset uint64, data []byte, write bool) error {
	if len(data) != 1 {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	dlab := u.registers[3]&0x80 != 0
	if write {
		switch offset {
		case 0:
			if dlab {
				u.registers[0] = data[0]
				return nil
			}
			_ = u.out.WriteByte(data[0])
			u.threPending = true
		case 1:
			if dlab {
				u.divisorHigh = data[0]
				return nil
			}
			if u.registers[1]&2 == 0 && data[0]&2 != 0 {
				u.threPending = true
			}
			u.registers[1] = data[0] & 0x0f
		case 2:
			u.registers[2] = data[0] & 1
			if data[0]&4 != 0 {
				u.threPending = true
			}
		case 3, 4, 7:
			u.registers[offset] = data[0]
		}
		return u.updateIRQ()
	}
	switch offset {
	case 0:
		if dlab {
			data[0] = u.registers[0]
		} else {
			data[0] = 0
		}
	case 1:
		if dlab {
			data[0] = u.divisorHigh
		} else {
			data[0] = u.registers[1]
		}
	case 2:
		data[0] = 1
		if u.registers[1]&2 != 0 && u.threPending {
			data[0] = 2
			u.threPending = false
		}
		if u.registers[2]&1 != 0 {
			data[0] |= 0xc0
		}
		return u.updateIRQ()
	case 5:
		data[0] = 0x60
	case 6:
		data[0] = 0xb0
	default:
		data[0] = u.registers[offset]
	}
	return nil
}
