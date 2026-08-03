package control

import (
	"context"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEventBufferIsBoundedAndReplaysWithoutBlocking(t *testing.T) {
	buffer := newEventBuffer()
	for range eventCapacity + 100 {
		buffer.publish(&vmmdv1.RuntimeEvent{})
	}
	if len(buffer.events) != eventCapacity {
		t.Fatalf("event buffer size = %d", len(buffer.events))
	}
	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	err := buffer.watch(ctx, 0, func(event *vmmdv1.RuntimeEvent) error {
		count++
		if count == eventCapacity {
			cancel()
		}
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("watch error = %v", err)
	}
	if count != eventCapacity {
		t.Fatalf("replayed %d events", count)
	}
	if buffer.events[0].GetSequence() != 101 || buffer.events[eventCapacity-1].GetSequence() != eventCapacity+100 {
		t.Fatalf("unexpected retained sequence range")
	}
}

func TestEventBufferRejectsCursorOutsideRetainedWindow(t *testing.T) {
	buffer := newEventBuffer()
	for range eventCapacity + 2 {
		buffer.publish(&vmmdv1.RuntimeEvent{})
	}
	err := buffer.watch(context.Background(), 1, func(*vmmdv1.RuntimeEvent) error { return nil })
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("watch error = %v", err)
	}
}
