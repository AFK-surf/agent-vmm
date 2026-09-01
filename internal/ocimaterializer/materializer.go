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
	item.mu.Lock()
	manager.global.Lock()
	return func() {
		manager.global.Unlock()
		item.mu.Unlock()
		manager.mu.Lock()
		item.refs--
		if item.refs == 0 {
			delete(manager.keys, digest)
		}
		manager.mu.Unlock()
	}
}
