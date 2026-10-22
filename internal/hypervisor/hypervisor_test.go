package hypervisor

import (
	"bytes"
	"errors"
	"testing"
)

type testMMIO struct{}

func (testMMIO) Read(uint64, []byte) error  { return nil }
func (testMMIO) Write(uint64, []byte) error { return nil }

func TestDeviceRanges(t *testing.T) {
	good := []MMIODevice{{Base: 0xa000000, Size: 0x200, IRQ: 48, Device: testMMIO{}}, {Base: 0xa000200, Size: 0x200, IRQ: 49, Device: testMMIO{}}}
	if err := validateDevices(good); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		device MMIODevice
	}{
		{"overlap", MMIODevice{Base: 0xa000100, Size: 0x200, IRQ: 49, Device: testMMIO{}}},
		{"same IRQ", MMIODevice{Base: 0xa000200, Size: 0x200, IRQ: 48, Device: testMMIO{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateDevices([]MMIODevice{good[0], test.device}); err == nil {
				t.Fatal("accepted invalid device range")
			}
		})
	}
}

func TestUARTConsole(t *testing.T) {
	var output bytes.Buffer
	serial := uart16550{out: &output}
	write := func(offset uint64, value byte) {
		t.Helper()
		if err := serial.access(offset, []byte{value}, true); err != nil {
			t.Fatal(err)
		}
	}
	write(3, 0x80)
	write(0, 1)
	write(1, 0)
	write(3, 3)
	write(0, 'x')
	data := []byte{0}
	_ = serial.access(5, data, false)
	if output.String() != "x" || data[0] != 0x60 {
		t.Fatalf("output=%q status=%x", output.String(), data[0])
	}

	t.Run("transmit-interrupts", func(t *testing.T) {
		var output bytes.Buffer
		serial := uart16550{out: &output}
		var levels []bool
		serial.irq = func(level bool) error { levels = append(levels, level); return nil }
		access := func(offset uint64, value byte, write bool) byte {
			t.Helper()
			data := []byte{value}
			if err := serial.access(offset, data, write); err != nil {
				t.Fatal(err)
			}
			return data[0]
		}
		access(2, 1, true)
		access(1, 2, true)
		if len(levels) != 1 || !levels[0] {
			t.Fatal("empty transmitter did not assert enabled IRQ")
		}
		if iir := access(2, 0, false); iir != 0xc2 {
			t.Fatalf("IIR=%#x", iir)
		}
		if levels[len(levels)-1] {
			t.Fatal("IIR read did not acknowledge transmit IRQ")
		}
		for round := 0; round < 4; round++ {
			for range 16 {
				access(0, 'a', true)
			}
			if !levels[len(levels)-1] {
				t.Fatal("new transmit did not reassert IRQ")
			}
			if iir := access(2, 0, false); iir != 0xc2 {
				t.Fatalf("transmit IIR=%#x", iir)
			}
		}
		if output.Len() != 64 {
			t.Fatalf("userspace output truncated: %d", output.Len())
		}
		access(0, 'b', true)
		access(1, 0, true)
		if levels[len(levels)-1] || access(2, 0, false) != 0xc1 {
			t.Fatal("disabled transmitter retained IRQ")
		}
		access(3, 0x80, true)
		access(0, 0x12, true)
		access(1, 0x34, true)
		if access(0, 0, false) != 0x12 || access(1, 0, false) != 0x34 {
			t.Fatal("divisor readback changed")
		}
		access(3, 3, true)
		if access(1, 0, false) != 0 {
			t.Fatal("divisor write changed interrupt enable")
		}
	})

	t.Run("host-output", func(t *testing.T) {
		blocked := &blockingWriter{release: make(chan struct{}), started: make(chan struct{})}
		out := newSerialOutput(blocked)
		serial := uart16550{out: out}
		_ = serial.access(0, []byte{'a'}, true)
		<-blocked.started
		for range serialBufferBytes + 3 {
			_ = serial.access(0, []byte{'b'}, true)
		}
		close(blocked.release)
		out.close()
		if dropped, failed := out.loss(); dropped != 3 || failed != 0 || blocked.n != serialBufferBytes+1 {
			t.Fatalf("dropped=%d failed=%d written=%d", dropped, failed, blocked.n)
		}

		out = newSerialOutput(failingWriter{})
		serial = uart16550{out: out}
		if err := serial.access(0, []byte{'c'}, true); err != nil {
			t.Fatal(err)
		}
		out.close()
		if _, failed := out.loss(); failed != 1 {
			t.Fatalf("failed=%d", failed)
		}
	})
}

type blockingWriter struct {
	release, started chan struct{}
	n                int
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	if w.n == 0 {
		close(w.started)
		<-w.release
	}
	w.n += len(p)
	return len(p), nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("host write failed") }
