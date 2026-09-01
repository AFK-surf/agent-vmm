package ocimaterializer

import "sync"

// Manager is the single Guest-global materialization owner. It serializes the
// physical import/unpack path per immutable digest and admits exactly one
// Guest-global import peak at a time.
type Manager struct {
	mu     sync.Mutex
	keys   map[string]*keyLock
	global sync.Mutex
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

func (manager *Manager) Lock(digest string) func() {
	item := manager.retain(digest)
	item.mu.Lock()
	manager.global.Lock()
	return manager.unlock(digest, item)
}

// LockImport preserves digest singleflight by waiting for an existing owner of
// the same immutable digest, but it does not wait behind unrelated physical
// materialization. A false result is the unique global import-slot capacity
// boundary and is surfaced to the controller as such.
func (manager *Manager) LockImport(digest string) (func(), bool) {
	item := manager.retain(digest)
	item.mu.Lock()
	if !manager.global.TryLock() {
		item.mu.Unlock()
		manager.release(digest, item)
		return nil, false
	}
	return manager.unlock(digest, item), true
}

func (manager *Manager) retain(digest string) *keyLock {
	manager.mu.Lock()
	if manager.keys == nil {
		manager.keys = make(map[string]*keyLock)
	}
	item := manager.keys[digest]
	if item == nil {
		item = new(keyLock)
		manager.keys[digest] = item
	}
	item.refs++
	manager.mu.Unlock()
	return item
}

func (manager *Manager) unlock(digest string, item *keyLock) func() {
	return func() {
		manager.global.Unlock()
		item.mu.Unlock()
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
