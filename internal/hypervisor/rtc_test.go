package hypervisor

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestPL031Clock(t *testing.T) {
	now := time.Date(2026, time.October, 22, 10, 0, 0, 0, time.UTC)
	r := pl031{now: func() time.Time { return now }}
	access := func(offset uint64, value uint32, write bool) uint32 {
		t.Helper()
		var data [4]byte
		binary.LittleEndian.PutUint32(data[:], value)
		if err := r.access(offset, data[:], write); err != nil {
			t.Fatal(err)
		}
		return binary.LittleEndian.Uint32(data[:])
	}
	if got := access(0, 0, false); got != uint32(now.Unix()) {
		t.Fatalf("boot clock=%d want %d", got, now.Unix())
	}
	now = now.Add(2 * time.Second)
	if got := access(0, 0, false); got != uint32(now.Unix()) {
		t.Fatal("clock did not advance")
	}
	access(8, 12345, true)
	now = now.Add(3 * time.Second)
	if got := access(0, 0, false); got != 12348 {
		t.Fatalf("loaded clock=%d", got)
	}
	access(0xc, 0, true)
	now = now.Add(time.Minute)
	if got := access(0, 0, false); got != 12348 || access(0xc, 0, false) != 0 {
		t.Fatal("disabled clock did not stop")
	}
	access(0xc, 1, true)
	now = now.Add(2 * time.Second)
	if got := access(0, 0, false); got != 12350 || access(0xc, 0, false) != 1 {
		t.Fatal("clock did not resume")
	}
	var peripheral, cell uint32
	for i := uint64(0); i < 4; i++ {
		peripheral |= access(0xfe0+i*4, 0, false) << (i * 8)
		cell |= access(0xff0+i*4, 0, false) << (i * 8)
	}
	if peripheral != 0x00041031 || cell != 0xb105f00d {
		t.Fatalf("PrimeCell identity %#x/%#x", peripheral, cell)
	}
	access(0x10, 1, true)
	if access(0x10, 0, false) != 0 || access(0x14, 0, false) != 0 || access(0x18, 0, false) != 0 {
		t.Fatal("time-only RTC exposed an alarm interrupt")
	}
}
