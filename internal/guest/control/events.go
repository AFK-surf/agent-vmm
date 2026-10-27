package control

import (
	"context"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const eventCapacity = 1024

type eventBuffer struct {
	mu          sync.Mutex
	sequence    uint64
	events      []*vmmdv1.RuntimeEvent
	subscribers map[chan *vmmdv1.RuntimeEvent]struct{}
}

func newEventBuffer() eventBuffer {
	return eventBuffer{subscribers: make(map[chan *vmmdv1.RuntimeEvent]struct{})}
}

func (buffer *eventBuffer) publish(event *vmmdv1.RuntimeEvent) {
	buffer.mu.Lock()
	buffer.sequence++
	event.Sequence = buffer.sequence
	buffer.events = append(buffer.events, event)
	if len(buffer.events) > eventCapacity {
		buffer.events = buffer.events[len(buffer.events)-eventCapacity:]
	}
	for subscriber := range buffer.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
	buffer.mu.Unlock()
}

func (buffer *eventBuffer) watch(ctx context.Context, after uint64, send func(*vmmdv1.RuntimeEvent) error) error {
	channel := make(chan *vmmdv1.RuntimeEvent, 64)
	buffer.mu.Lock()
	var replay []*vmmdv1.RuntimeEvent
	if after != 0 && len(buffer.events) != 0 && after+1 < buffer.events[0].GetSequence() {
		buffer.mu.Unlock()
		return status.Error(codes.OutOfRange, "event cursor is older than the retained event window; take a new snapshot")
	}
	for _, event := range buffer.events {
		if event.GetSequence() > after {
			replay = append(replay, event)
		}
	}
	buffer.subscribers[channel] = struct{}{}
	buffer.mu.Unlock()
	defer func() {
		buffer.mu.Lock()
		delete(buffer.subscribers, channel)
		buffer.mu.Unlock()
	}()
	for _, event := range replay {
		if err := send(event); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-channel:
			if err := send(event); err != nil {
				return err
			}
		}
	}
}
