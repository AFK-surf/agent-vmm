package ocimaterializer

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Manager is the single Guest-global materialization owner. It serializes the
// physical import/unpack path per immutable digest and admits exactly one
// Guest-global import peak at a time. Waiters are served in FIFO order.
type Manager struct {
	mu     sync.Mutex
	keys   map[string]*keyLock
	global *semaphore.Weighted
}

type keyLock struct {
	owner *semaphore.Weighted
	refs  int
}

func (manager *Manager) Lock(digest string) func() {
	unlock, _ := manager.LockImport(context.Background(), digest)
	return unlock
}

// LockImport waits behind an existing owner of the same immutable digest and
// then behind unrelated physical materialization until ctx ends. The caller
// maps an expired request deadline to the import-slot capacity boundary.
func (manager *Manager) LockImport(ctx context.Context, digest string) (func(), error) {
	item := manager.retain(digest)
	if err := item.owner.Acquire(ctx, 1); err != nil {
		manager.release(digest, item)
		return nil, err
	}
	if err := manager.slot().Acquire(ctx, 1); err != nil {
		item.owner.Release(1)
		manager.release(digest, item)
		return nil, err
	}
	return manager.unlock(digest, item), nil
}

// Admitted build workers wait within their request deadline. Their existing
// two-worker semaphore remains held through output materialization.
func (manager *Manager) TryLockBuild() (func(), bool) {
	slot := manager.slot()
	if !slot.TryAcquire(1) {
		return nil, false
	}
	return func() { slot.Release(1) }, true
}

func (manager *Manager) LockBuild(ctx context.Context) (func(), error) {
	slot := manager.slot()
	if err := slot.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { slot.Release(1) }, nil
}

func (manager *Manager) slot() *semaphore.Weighted {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.global == nil {
		manager.global = semaphore.NewWeighted(1)
	}
	return manager.global
}

func (manager *Manager) retain(digest string) *keyLock {
	manager.mu.Lock()
	if manager.keys == nil {
		manager.keys = make(map[string]*keyLock)
	}
	item := manager.keys[digest]
	if item == nil {
		item = &keyLock{owner: semaphore.NewWeighted(1)}
		manager.keys[digest] = item
	}
	item.refs++
	manager.mu.Unlock()
	return item
}

func (manager *Manager) unlock(digest string, item *keyLock) func() {
	return func() {
		manager.slot().Release(1)
		item.owner.Release(1)
		manager.release(digest, item)
	}
}

func (manager *Manager) release(digest string, item *keyLock) {
	manager.mu.Lock()
	item.refs--
	if item.refs == 0 {
		delete(manager.keys, digest)
	}
	manager.mu.Unlock()
}
