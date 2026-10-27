//go:build darwin || linux

package session

import (
	"context"
	"errors"
	"os"
)

func (owner *Owner) RevokeEnvironmentSessions(ctx context.Context, environmentID string) error {
	return owner.revokeWhere(ctx, func(session *server) bool { return session.environmentID == environmentID })
}

func (owner *Owner) revokeWhere(ctx context.Context, match func(*server) bool) error {
	owner.mu.Lock()
	var ids []string
	for id, session := range owner.servers {
		if match(session) {
			session.revoked = true
			ids = append(ids, id)
		}
	}
	owner.mu.Unlock()
	var result error
	for _, id := range ids {
		result = errors.Join(result, owner.Revoke(ctx, id))
	}
	return result
}

func (owner *Owner) AllocationSessionIDs(registrationID, allocationID string, generation uint64) []string {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	ids := make([]string, 0)
	for id, session := range owner.servers {
		if session.registrationID == registrationID && session.allocationID == allocationID && session.allocationGeneration == generation {
			ids = append(ids, id)
		}
	}
	return ids
}

func (owner *Owner) RevokeRegistration(ctx context.Context, registrationID string) error {
	return owner.revokeWhere(ctx, func(session *server) bool { return session.registrationID == registrationID })
}

func (owner *Owner) RevokeAllSessions(ctx context.Context) {
	_ = owner.revokeWhere(ctx, func(*server) bool { return true })
}
func (owner *Owner) ActiveSessions(registrationID string) uint32 {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	var count uint32
	for _, session := range owner.servers {
		if session.registrationID == registrationID {
			count++
		}
	}
	return count
}
func (owner *Owner) MatchesAllocation(id, registrationID, allocationID string, generation uint64) bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	session := owner.servers[id]
	return session != nil && session.registrationID == registrationID && session.allocationID == allocationID && session.allocationGeneration == generation
}
func (owner *Owner) CloseDisposed(id string) {
	owner.mu.Lock()
	session := owner.servers[id]
	delete(owner.servers, id)
	owner.mu.Unlock()
	if session != nil {
		session.server.Stop()
		_ = session.listener.Close()
		_ = os.Remove(session.path)
	}
}
func (owner *Owner) Close() {
	owner.mu.Lock()
	servers := owner.servers
	owner.servers = make(map[string]*server)
	owner.mu.Unlock()
	for _, session := range servers {
		session.server.Stop()
		session.listener.Close()
		_ = os.Remove(session.path)
	}
}
