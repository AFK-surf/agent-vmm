package hypervisor

import (
	"encoding/binary"
	"strings"
	"testing"
)

func testBZImage() []byte {
	image := make([]byte, 4096)
	image[0x1f1] = 4
	binary.LittleEndian.PutUint16(image[0x1fe:], 0xaa55)
	copy(image[0x202:], "HdrS")
	binary.LittleEndian.PutUint16(image[0x206:], 0x20c)
	binary.LittleEndian.PutUint32(image[0x22c:], 0x37ffffff)
	binary.LittleEndian.PutUint32(image[0x238:], 4095)
	binary.LittleEndian.PutUint32(image[0x260:], 2<<20)
	return image
}

func TestAMD64Boot(t *testing.T) {
	mem := make([]byte, 8<<20)
	cfg := Config{Kernel: testBZImage(), Initramfs: []byte("initrd"), MemoryBytes: uint64(len(mem)), CommandLine: "console=ttyS0"}
	devices := []MMIODevice{{Base: 0xd0000000, Size: 512, IRQ: 5}}
	boot, err := prepareAMD64Boot(mem, cfg, devices)
	if err != nil {
		t.Fatal(err)
	}
	if boot.entry != 0x100000 {
		t.Fatalf("entry=%#x", boot.entry)
	}
	zero := mem[0x90000:0x91000]
	initrd := binary.LittleEndian.Uint32(zero[0x218:])
	if string(mem[initrd:initrd+6]) != "initrd" {
		t.Fatal("initrd copied incorrectly")
	}
	cmdline := strings.SplitN(string(mem[0x91000:0x92000]), "\x00", 2)[0]
	if !strings.Contains(cmdline, "virtio_mmio.device=512@0xd0000000:5") {
		t.Fatalf("cmdline=%q", cmdline)
	}
	if zero[0x1e8] != 3 {
		t.Fatal("E820 map missing")
	}
	t.Run("overlap", func(t *testing.T) {
		cfg.Initramfs = make([]byte, 6<<20)
		if _, err := prepareAMD64Boot(mem, cfg, devices); err == nil {
			t.Fatal("accepted initrd overlapping kernel")
		}
	})
}
