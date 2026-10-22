package hypervisor

import (
	"encoding/binary"
	"testing"
)

func TestHyperVClockInterfaces(t *testing.T) {
	native := []CPUIDEntry{{Function: 1, Eax: 42}, {Function: 0x40000001, Eax: 1}}
	entries, err := hyperVClockCPUID(native, []CPUIDEntry{{Function: 0x40000003, Eax: ^uint32(0), Ebx: ^uint32(0), Edx: ^uint32(0)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Function == 1 && e.Eax != 42 {
			t.Fatal("native leaf changed")
		}
		if e.Function == 0x40000003 && (e.Eax != 1<<1|1<<5|1<<6|1<<9 || e.Ebx != 0 || e.Edx != 0) {
			t.Fatalf("advertised unsupported features: %+v", e)
		}
	}
	if _, err := hyperVClockCPUID(native, nil); err == nil {
		t.Fatal("enabled without accelerator support")
	}
	data := make([]byte, 40)
	binary.LittleEndian.PutUint32(data, 2)
	if err := completeHyperVExit(data); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(data[16:]) != 2 {
		t.Fatal("unknown hypercall was not rejected")
	}
	binary.LittleEndian.PutUint32(data, 1)
	if err := completeHyperVExit(data); err == nil {
		t.Fatal("accepted unsupported SynIC exit")
	}
}
