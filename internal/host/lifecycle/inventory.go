//go:build darwin || linux

package lifecycle

import (
	"context"
	"sync"

	"github.com/AFK-surf/agent-vmm/internal/host/locks"
)

type InventoryFence struct {
	lock        locks.RWMutex
	mu          sync.Mutex
	readers     map[uint64]context.CancelFunc
	next        uint64
	transitions int
	changed     chan struct{}
}

func (fence *InventoryFence) Enter(ctx context.Context) (context.Context, func(), error) {
	for {
		if err := fence.lock.RLock(ctx); err != nil {
			return nil, nil, err
		}
		fence.mu.Lock()
		if fence.transitions != 0 {
			changed := fence.changed
			fence.mu.Unlock()
			fence.lock.RUnlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-changed:
			}
			continue
		}
		if fence.readers == nil {
			fence.readers = make(map[uint64]context.CancelFunc)
		}
		fence.next++
		id := fence.next
		reader, cancel := context.WithCancel(ctx)
		fence.readers[id] = cancel
		fence.mu.Unlock()
		return reader, func() {
			fence.mu.Lock()
			delete(fence.readers, id)
			fence.mu.Unlock()
			cancel()
			fence.lock.RUnlock()
		}, nil
	}
}

func (fence *InventoryFence) CancelReaders() {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	for _, cancel := range fence.readers {
		cancel()
	}
}

func (fence *InventoryFence) Transition(ctx context.Context) (func(), error) {
	fence.mu.Lock()
	if fence.transitions == 0 {
		fence.changed = make(chan struct{})
	}
	fence.transitions++
	for _, cancel := range fence.readers {
		cancel()
	}
	fence.mu.Unlock()
	finish := func() {
		fence.mu.Lock()
		fence.transitions--
		if fence.transitions == 0 {
			close(fence.changed)
		}
		fence.mu.Unlock()
	}
	if err := fence.lock.Lock(ctx); err != nil {
		finish()
		return nil, err
	}
	return func() { fence.lock.Unlock(); finish() }, nil
}
