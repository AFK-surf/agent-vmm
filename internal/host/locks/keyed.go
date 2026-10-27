//go:build darwin || linux

package locks

import (
	"context"
	"sync"
)

// Keyed keeps one mutex for every held or queued key. An idle entry is
// removed only after all holders and waiters have left.
type Keyed[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*keyedLockEntry
}

type keyedLockEntry struct {
	lock  RWMutex
	users int
}

func (table *Keyed[K]) Lock(ctx context.Context, id K) (func(), error) {
	return table.acquire(ctx, id, false)
}

func (table *Keyed[K]) RLock(ctx context.Context, id K) (func(), error) {
	return table.acquire(ctx, id, true)
}

func (table *Keyed[K]) acquire(ctx context.Context, id K, read bool) (func(), error) {
	table.mu.Lock()
	if table.entries == nil {
		table.entries = make(map[K]*keyedLockEntry)
	}
	entry := table.entries[id]
	if entry == nil {
		entry = &keyedLockEntry{}
		table.entries[id] = entry
	}
	entry.users++
	table.mu.Unlock()
	leave := func() {
		table.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(table.entries, id)
		}
		table.mu.Unlock()
	}
	acquire, release := entry.lock.Lock, entry.lock.Unlock
	if read {
		acquire, release = entry.lock.RLock, entry.lock.RUnlock
	}
	if err := acquire(ctx); err != nil {
		leave()
		return nil, err
	}
	return func() { release(); leave() }, nil
}
