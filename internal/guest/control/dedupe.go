package control

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	dedupeLimit = 1024
	dedupeTTL   = 5 * time.Minute
)

type completion struct {
	fingerprint string
	value       any
	err         error
	completed   time.Time
}

type pendingCall struct {
	fingerprint string
	done        chan struct{}
}

type deduper struct {
	mu      sync.Mutex
	pending map[string]*pendingCall
	done    map[string]completion
}

func (d *deduper) do(ctx context.Context, key, fingerprint string, operation func() (any, error)) (any, error) {
	d.mu.Lock()
	if item, ok := d.done[key]; ok && time.Since(item.completed) < dedupeTTL {
		d.mu.Unlock()
		if item.fingerprint != fingerprint {
			return nil, errors.New("request_id was reused with different input")
		}
		return item.value, item.err
	}
	if call, ok := d.pending[key]; ok {
		d.mu.Unlock()
		if call.fingerprint != fingerprint {
			return nil, errors.New("request_id is in flight with different input")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return d.do(ctx, key, fingerprint, operation)
		}
	}
	if d.pending == nil {
		d.pending = make(map[string]*pendingCall)
	}
	if len(d.pending) >= 64 {
		d.mu.Unlock()
		return nil, resourceExhausted("too many concurrent mutations")
	}
	call := &pendingCall{fingerprint: fingerprint, done: make(chan struct{})}
	d.pending[key] = call
	d.mu.Unlock()

	value, err := operation()
	d.mu.Lock()
	delete(d.pending, key)
	if d.done == nil {
		d.done = make(map[string]completion)
	}
	if len(d.done) >= dedupeLimit {
		dropOldest(d.done)
	}
	d.done[key] = completion{fingerprint: fingerprint, value: value, err: err, completed: time.Now()}
	close(call.done)
	d.mu.Unlock()
	return value, err
}

func dropOldest(items map[string]completion) {
	var oldestKey string
	oldest := time.Now()
	for key, item := range items {
		if item.completed.Before(oldest) {
			oldestKey, oldest = key, item.completed
		}
	}
	delete(items, oldestKey)
}
