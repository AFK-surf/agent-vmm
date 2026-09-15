//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"testing"
	"time"
)

func TestGuestStartupConsoleFailure(t *testing.T) {
	harness := startHostWithState(t, 1<<30)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result := make(chan error, 1)
	summary, err := harness.client.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		t.Fatalf("VM start impact unavailable: summary=%+v err=%v", summary, err)
	}
	fence := &hostv1.VMMutationFence{State: summary.GetVm().GetState(), BootId: summary.GetVm().GetBootId(), ObservedWorkloads: summary.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	go func() {
		_, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{RequestId: "console-capacity-failure", Fence: fence})
		result <- err
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		output := readFileTail(harness.serial, 64<<10)
		if bytes.Contains(output, []byte("guest startup stage=oci_storage error=storage capacity unavailable")) {
			if bytes.Contains(output, []byte("guest startup stage=listener")) {
				t.Fatal("failed storage startup reached listener")
			}
			cancel()
			if err := <-result; err == nil {
				t.Fatal("undersized state disk started successfully")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pre-listener failure did not reach Host log: %s", output)
		case <-ticker.C:
		}
	}
}
