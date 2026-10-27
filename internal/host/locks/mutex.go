//go:build darwin || linux

package locks

import (
	"context"
	"sync"
)

type RWMutex struct {
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	changed        chan struct{}
}

func (lock *RWMutex) notifyLocked() {
	if lock.changed != nil {
		close(lock.changed)
	}
	lock.changed = make(chan struct{})
}

func (lock *RWMutex) Lock(ctx context.Context) error {
	lock.mu.Lock()
	lock.waitingWriters++
	defer func() { lock.waitingWriters--; lock.notifyLocked(); lock.mu.Unlock() }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !lock.writer && lock.readers == 0 {
			lock.writer = true
			return nil
		}
		if lock.changed == nil {
			lock.changed = make(chan struct{})
		}
		changed := lock.changed
		lock.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		lock.mu.Lock()
	}
}

func (lock *RWMutex) RLock(ctx context.Context) error {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !lock.writer && lock.waitingWriters == 0 {
			lock.readers++
			return nil
		}
		if lock.changed == nil {
			lock.changed = make(chan struct{})
		}
		changed := lock.changed
		lock.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		lock.mu.Lock()
	}
}

func (lock *RWMutex) Unlock() {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if !lock.writer {
		panic("unlock of unlocked RWMutex")
	}
	lock.writer = false
	lock.notifyLocked()
}

func (lock *RWMutex) RUnlock() {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.readers == 0 {
		panic("read unlock of unlocked RWMutex")
	}
	lock.readers--
	lock.notifyLocked()
}

func (lock *RWMutex) TryLock() bool {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.writer || lock.readers != 0 || lock.waitingWriters != 0 {
		return false
	}
	lock.writer = true
	return true
}
