//go:build linux && arm64

package hypervisor

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestKVMRunStopsEveryCPU(t *testing.T) {
	kvm, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("/dev/kvm is unavailable: %v", err)
	}
	_ = kvm.Close()
	// WFI exercises a sleeping boot vCPU and the secondaries wait powered off;
	// each restart must release every CPU, interrupt controller, mapping, and
	// KVM descriptor before creating another VM.
	for i := 0; i < 2; i++ {
		vm, err := New(Config{MemoryBytes: 64 << 20, CPUs: 4, Kernel: testARM64Image(arm64WFILoop)})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		done := make(chan error, 1)
		go func() { done <- vm.Run(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("run %d: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation failed to wake KVM_RUN")
		}
		cancel()
		if err := vm.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// One vCPU's PSCI SYSTEM_OFF ends the VM while the secondaries remain powered off.
	vm, err := New(Config{MemoryBytes: 64 << 20, CPUs: 4, Kernel: testARM64Image(arm64PowerOff)})
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := vm.Run(ctx); err != nil {
		t.Fatalf("poweroff: %v", err)
	}
}
