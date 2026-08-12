package host

import (
	"testing"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRouteBudgetTracksConnectionConcurrencyAndBytesAcrossStreams(t *testing.T) {
	now := time.Now()
	capability := &trustv1.RouteCapability{CapabilityId: "cap", RouteGeneration: 2, ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		Budget: &trustv1.RouteBudget{ConnectionLimit: 2, ConcurrencyLimit: 1, ByteLimit: 10}}
	tracker := new(routeBudgetTracker)
	first, err := tracker.acquire(capability, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.acquire(capability, now); err == nil {
		t.Fatal("concurrent stream exceeded capability budget")
	}
	if !first.consume(6) || first.consume(5) {
		t.Fatal("byte budget was not enforced across directions")
	}
	first.release()
	second, err := tracker.acquire(capability, now)
	if err != nil {
		t.Fatal(err)
	}
	second.release()
	if _, err := tracker.acquire(capability, now); err == nil {
		t.Fatal("connection budget was not durable across stream close")
	}
}
