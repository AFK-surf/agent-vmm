package hypervisor

import (
	"bytes"
	"encoding/binary"
	"testing"
)

var (
	arm64WFILoop  = []uint32{0xd503207f, 0x17ffffff}             // WFI; B back to WFI
	arm64PowerOff = []uint32{0x52800100, 0x72b08000, 0xd4000002} // W0 = PSCI SYSTEM_OFF; HVC #0
)

func testARM64Image(code []uint32) []byte {
	image := make([]byte, 64+4*len(code))
	binary.LittleEndian.PutUint32(image[0:4], 0x14000010) // B to the code after the header
	binary.LittleEndian.PutUint64(image[8:16], 0x80000)
	binary.LittleEndian.PutUint64(image[16:24], 2<<20)
	binary.LittleEndian.PutUint32(image[56:60], 0x644d5241)
	for i, instruction := range code {
		binary.LittleEndian.PutUint32(image[64+4*i:], instruction)
	}
	return image
}

func TestARM64Boot(t *testing.T) {
	image := testARM64Image(arm64WFILoop)
	mem := make([]byte, 8<<20)
	cfg := Config{MemoryBytes: uint64(len(mem)), CPUs: 18, Kernel: image, Initramfs: []byte("initrd"), CommandLine: "console=ttyS0"}
	boot, err := prepareARM64Boot(mem, cfg, []MMIODevice{{Base: 0xa000000, Size: 0x200, IRQ: 48}})
	if err != nil {
		t.Fatal(err)
	}
	if boot.entry != 0x40080000 || boot.dtb%8 != 0 {
		t.Fatalf("boot=%+v", boot)
	}
	if !bytes.Equal(mem[0x80000:0x80000+len(image)], image) {
		t.Fatal("kernel copied incorrectly")
	}
	if !bytes.Equal(mem[0x280000:0x280006], cfg.Initramfs) {
		t.Fatal("initrd copied incorrectly")
	}
	dtb := mem[boot.dtb-0x40000000:]
	if binary.BigEndian.Uint32(dtb[:4]) != 0xd00dfeed {
		t.Fatal("invalid FDT")
	}
	reg := func(values ...uint64) []byte {
		var b []byte
		for _, v := range values {
			b = binary.BigEndian.AppendUint64(b, v)
		}
		return b
	}
	for _, want := range [][]byte{
		[]byte("virtio_mmio@a000000"), []byte("cpu@0\x00"), []byte("cpu@f\x00"), []byte("cpu@101\x00"),
		reg(0x08000000, 0x10000, 0x080a0000, 18*0x20000), reg(0x09000000, 8),
	} {
		if !bytes.Contains(dtb, want) {
			t.Fatalf("FDT lacks %q", want)
		}
	}
	if bytes.Contains(dtb, []byte("cpu@12\x00")) || arm64MPIDR(17) != 0x101 {
		t.Fatal("cpu nodes do not follow KVM MPIDR affinity")
	}
	if arm64GICRedistributor+arm64MaxCPUs*arm64RedistributorStride > arm64UARTBase {
		t.Fatal("redistributor limit overlaps the UART")
	}
	for name, cfg := range map[string]Config{
		"outside RAM":  {Kernel: image, MemoryBytes: 1 << 20, CPUs: 1},
		"not an Image": {Kernel: make([]byte, 64), MemoryBytes: 8 << 20, CPUs: 1},
	} {
		if _, err := prepareARM64Boot(make([]byte, cfg.MemoryBytes), cfg, nil); err == nil {
			t.Fatalf("accepted kernel %s", name)
		}
	}
}
