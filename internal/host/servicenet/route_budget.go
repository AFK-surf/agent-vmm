package servicenet

import (
	"errors"
	"sync"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

type routeBudgetState struct {
	generation  uint64
	expiresAt   time.Time
	connections uint32
	active      uint32
	bytes       uint64
}

type RouteBudgetTracker struct {
	mu     sync.Mutex
	states map[string]*routeBudgetState
}

type RouteBudgetLease struct {
	tracker      *RouteBudgetTracker
	capabilityID string
	generation   uint64
	budget       *trustv1.RouteBudget
	released     bool
}

func (tracker *RouteBudgetTracker) Acquire(capability *trustv1.RouteCapability, now time.Time) (*RouteBudgetLease, error) {
	budget := capability.GetBudget()
	if capability.GetCapabilityId() == "" || capability.GetRouteGeneration() == 0 || capability.GetExpiresAt() == nil ||
		budget.GetConnectionLimit() == 0 || budget.GetConcurrencyLimit() == 0 || budget.GetByteLimit() == 0 {
		return nil, errors.New("complete route budget is required")
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.states == nil {
		tracker.states = make(map[string]*routeBudgetState)
	}
	for id, state := range tracker.states {
		if !state.expiresAt.After(now) {
			delete(tracker.states, id)
		}
	}
	state := tracker.states[capability.GetCapabilityId()]
	if state == nil {
		state = &routeBudgetState{generation: capability.GetRouteGeneration(), expiresAt: capability.GetExpiresAt().AsTime()}
		tracker.states[capability.GetCapabilityId()] = state
	}
	if state.generation != capability.GetRouteGeneration() || state.expiresAt != capability.GetExpiresAt().AsTime() ||
		state.connections >= budget.GetConnectionLimit() || state.active >= budget.GetConcurrencyLimit() || state.bytes >= budget.GetByteLimit() {
		return nil, errors.New("route budget is exhausted or stale")
	}
	state.connections++
	state.active++
	return &RouteBudgetLease{tracker: tracker, capabilityID: capability.GetCapabilityId(), generation: capability.GetRouteGeneration(), budget: budget}, nil
}

func (lease *RouteBudgetLease) Consume(count uint64) bool {
	lease.tracker.mu.Lock()
	defer lease.tracker.mu.Unlock()
	state := lease.tracker.states[lease.capabilityID]
	if state == nil || state.generation != lease.generation || count > lease.budget.GetByteLimit()-min(state.bytes, lease.budget.GetByteLimit()) {
		return false
	}
	state.bytes += count
	return true
}

func (lease *RouteBudgetLease) Release() {
	lease.tracker.mu.Lock()
	defer lease.tracker.mu.Unlock()
	if lease.released {
		return
	}
	lease.released = true
	if state := lease.tracker.states[lease.capabilityID]; state != nil && state.generation == lease.generation && state.active > 0 {
		state.active--
	}
}
