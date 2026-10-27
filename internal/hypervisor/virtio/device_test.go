package virtio

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"testing"
	"time"
)

type testMemory struct {
	mu   sync.Mutex
	data []byte
}

func newTestMemory() *testMemory { return &testMemory{data: make([]byte, 2<<20)} }
func (m *testMemory) ReadIPA(addr uint64, size int) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, err := (Memory{Data: m.data}).ReadIPA(addr, size)
	return append([]byte(nil), b...), err
}
func (m *testMemory) WriteIPA(addr uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return (Memory{Data: m.data}).WriteIPA(addr, data)
}
func put16(mem GuestMemory, addr uint64, value uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], value)
	_ = mem.WriteIPA(addr, b[:])
}
func get16(mem GuestMemory, addr uint64) uint16 {
	b, _ := mem.ReadIPA(addr, 2)
	return binary.LittleEndian.Uint16(b)
}
func writeReg(t *testing.T, d *transport, offset uint64, value uint32) {
	t.Helper()
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], value)
	if err := d.Write(offset, b[:]); err != nil {
		t.Fatal(err)
	}
}
func readReg(t *testing.T, d *transport, offset uint64) uint32 {
	t.Helper()
	var b [4]byte
	if err := d.Read(offset, b[:]); err != nil {
		t.Fatal(err)
	}
	return binary.LittleEndian.Uint32(b[:])
}
func negotiate(t *testing.T, d *transport) {
	t.Helper()
	writeReg(t, d, regDriverFeatSel, 1)
	writeReg(t, d, regDriverFeatures, 1)
	writeReg(t, d, regStatus, 1|2|statusFeaturesOK|statusDriverOK)
}

type testQueue struct {
	t                    *testing.T
	device               *transport
	mem                  GuestMemory
	index                uint32
	desc, avail, used    uint64
	available, completed uint16
}

func setupQueue(t *testing.T, d *transport, mem GuestMemory, index uint32) *testQueue {
	t.Helper()
	base := uint64(index) * 0x4000
	q := &testQueue{t: t, device: d, mem: mem, index: index, desc: base + 0x1000, avail: base + 0x2000, used: base + 0x3000}
	writeReg(t, d, regQueueSel, index)
	writeReg(t, d, regQueueNum, 8)
	for _, p := range []struct {
		reg  uint64
		addr uint64
	}{{regQueueDescLow, q.desc}, {regQueueAvailLow, q.avail}, {regQueueUsedLow, q.used}} {
		writeReg(t, d, p.reg, uint32(p.addr))
	}
	writeReg(t, d, regQueueReady, 1)
	return q
}
func (q *testQueue) descriptor(index uint16, addr uint64, size uint32, flags, next uint16) {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:], addr)
	binary.LittleEndian.PutUint32(b[8:], size)
	binary.LittleEndian.PutUint16(b[12:], flags)
	binary.LittleEndian.PutUint16(b[14:], next)
	if err := q.mem.WriteIPA(q.desc+uint64(index)*16, b[:]); err != nil {
		q.t.Fatal(err)
	}
}
func (q *testQueue) submit() {
	put16(q.mem, q.avail+4+uint64(q.available%8)*2, 0)
	q.available++
	put16(q.mem, q.avail+2, q.available)
	writeReg(q.t, q.device, regQueueNotify, q.index)
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("device did not make progress")
		}
		time.Sleep(time.Millisecond)
	}
}
func (q *testQueue) wait() {
	q.t.Helper()
	q.completed++
	eventually(q.t, func() bool { return get16(q.mem, q.used+2) == q.completed })
}

func blockRequest(t *testing.T, q *testQueue, typ uint32, sector uint64, data []byte, readSize int) (byte, []byte) {
	t.Helper()
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header, typ)
	binary.LittleEndian.PutUint64(header[8:], sector)
	_ = q.mem.WriteIPA(0x10000, header)
	_ = q.mem.WriteIPA(0x11000, data)
	_ = q.mem.WriteIPA(0x15000, []byte{0xff})
	q.descriptor(0, 0x10000, 16, descFNext, 1)
	if readSize > 0 {
		q.descriptor(1, 0x11000, uint32(readSize), descFWrite|descFNext, 2)
		q.descriptor(2, 0x15000, 1, descFWrite, 0)
	} else if len(data) > 0 {
		q.descriptor(1, 0x11000, uint32(len(data)), descFNext, 2)
		q.descriptor(2, 0x15000, 1, descFWrite, 0)
	} else {
		q.descriptor(1, 0x15000, 1, descFWrite, 0)
	}
	q.submit()
	q.wait()
	status, _ := q.mem.ReadIPA(0x15000, 1)
	out, _ := q.mem.ReadIPA(0x11000, readSize)
	return status[0], out
}

func TestBlockRequests(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "writable", true: "readonly"}[readOnly], func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "disk")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if err := file.Truncate(4096); err != nil {
				t.Fatal(err)
			}
			original := bytes.Repeat([]byte{0x19}, 512)
			if _, err := file.WriteAt(original, 512); err != nil {
				t.Fatal(err)
			}
			mem := newTestMemory()
			b, err := NewBlock(mem, nil, file, readOnly, "AGENT_STATE")
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			q := setupQueue(t, b.transport, mem, 0)
			negotiate(t, b.transport)
			writeReg(t, b.transport, regDeviceFeatSel, 0)
			features := readReg(t, b.transport, regDeviceFeatures)
			if (features&(1<<5) != 0) != readOnly || features&(1<<9) == 0 {
				t.Fatalf("wrong block features %#x", features)
			}
			if status, id := blockRequest(t, q, blockGetID, 0, nil, 20); status != blockOK || string(bytes.TrimRight(id, "\x00")) != "AGENT_STATE" {
				t.Fatalf("get ID: %d %q", status, id)
			}
			if status, data := blockRequest(t, q, blockRead, 1, nil, 512); status != blockOK || !bytes.Equal(data, original) {
				t.Fatalf("read: %d", status)
			}
			changed := bytes.Repeat([]byte{0x42}, 512)
			status, _ := blockRequest(t, q, blockWrite, 1, changed, 0)
			expected := byte(blockOK)
			if readOnly {
				expected = blockIOError
			}
			if status != expected {
				t.Fatalf("write: %d, want %d", status, expected)
			}
			if status, _ := blockRequest(t, q, blockFlush, 0, nil, 0); status != blockOK {
				t.Fatalf("flush: %d", status)
			}
			want := changed
			if readOnly {
				want = original
			}
			if status, data := blockRequest(t, q, blockRead, 1, nil, 512); status != blockOK || !bytes.Equal(data, want) {
				t.Fatalf("read after write: %d", status)
			}
			if status, _ := blockRequest(t, q, blockRead, ^uint64(0), nil, 512); status != blockIOError {
				t.Fatalf("out of range: %d", status)
			}
			if status, _ := blockRequest(t, q, 11, 0, nil, 0); status != blockUnsupported {
				t.Fatalf("unsupported: %d", status)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if status, _ := blockRequest(t, q, blockFlush, 0, nil, 0); status != blockIOError {
				t.Fatalf("failed flush returned %d", status)
			}
		})
	}
}

func TestMalformedQueueCannotWriteDisk(t *testing.T) {
	for _, kind := range []string{"cycle", "outside-ram", "indirect", "ring-overrun", "address-wrap"} {
		t.Run(kind, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "disk")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			_ = file.Truncate(4096)
			mem := newTestMemory()
			b, err := NewBlock(mem, nil, file, false, "STATE")
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			q := setupQueue(t, b.transport, mem, 0)
			negotiate(t, b.transport)
			header := make([]byte, 16)
			binary.LittleEndian.PutUint32(header, blockWrite)
			_ = mem.WriteIPA(0x10000, header)
			_ = mem.WriteIPA(0x11000, bytes.Repeat([]byte{1}, 512))
			q.descriptor(0, 0x10000, 16, descFNext, 1)
			q.descriptor(1, 0x11000, 512, descFNext, 2)
			q.descriptor(2, 0x15000, 1, descFWrite, 0)
			switch kind {
			case "cycle":
				q.descriptor(1, 0x11000, 512, descFNext, 1)
			case "outside-ram":
				q.descriptor(2, 1<<30, 1, descFWrite, 0)
			case "indirect":
				q.descriptor(0, 0x10000, 16, 4, 0)
			case "address-wrap":
				q.descriptor(2, ^uint64(0)-4, 64, descFWrite, 0)
			case "ring-overrun":
				q.available = 8
			}
			q.submit()
			eventually(t, func() bool { return readReg(t, b.transport, regStatus)&statusNeedsReset != 0 })
			data := make([]byte, 4096)
			_, _ = file.ReadAt(data, 0)
			if !bytes.Equal(data, make([]byte, 4096)) {
				t.Fatal("malformed request wrote disk")
			}
		})
	}
}

func TestRNGAndQueueReset(t *testing.T) {
	mem := newTestMemory()
	r := NewRNG(mem, nil)
	defer r.Close()
	q := setupQueue(t, r.transport, mem, 0)
	negotiate(t, r.transport)
	q.descriptor(0, 0x10000, 512, descFWrite, 0)
	q.submit()
	q.wait()
	data, _ := mem.ReadIPA(0x10000, 512)
	if bytes.Equal(data, make([]byte, 512)) {
		t.Fatal("rng returned no entropy")
	}
	writeReg(t, r.transport, regStatus, 0)
	if readReg(t, r.transport, regQueueReady) != 0 || readReg(t, r.transport, regInterruptStatus) != 0 {
		t.Fatal("reset retained queue or IRQ")
	}
}
