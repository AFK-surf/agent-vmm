package vmm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxStateAllocationPreservesDataAndBacksHoles(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "state.raw"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const size = int64(8 << 20)
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, size - 1} {
		if _, err := file.WriteAt([]byte{0x7e}, offset); err != nil {
			t.Fatal(err)
		}
	}
	if err := allocateState(file, size); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := allocatedBytes(info)
	if err != nil || allocated < uint64(size) {
		t.Fatalf("state backing = %d: %v", allocated, err)
	}
	for _, offset := range []int64{0, size / 2, size - 1} {
		value := make([]byte, 1)
		if _, err := file.ReadAt(value, offset); err != nil {
			t.Fatal(err)
		}
		want := byte(0x7e)
		if offset == size/2 {
			want = 0
		}
		if value[0] != want {
			t.Fatalf("offset %d: got %x, want %x", offset, value[0], want)
		}
	}
}
