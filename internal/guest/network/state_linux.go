//go:build linux

package network

import (
	"errors"
	"maps"
	"net/netip"
	"reflect"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

type revokedTuple struct {
	source, destination netip.Addr
	port                uint16
}

type state struct {
	namespaces map[string]Namespace
	endpoints  map[string]map[string]Endpoint
	links      map[string]*vmmdv1.NetworkLink
	services   map[string]*vmmdv1.ServiceProjection
	revoking   map[revokedTuple]struct{}
}

func (m *Manager) stateLocked() state {
	endpoints := make(map[string]map[string]Endpoint, len(m.endpoints))
	for id, values := range m.endpoints {
		endpoints[id] = maps.Clone(values)
	}
	return state{maps.Clone(m.namespaces), endpoints, maps.Clone(m.links), maps.Clone(m.services), maps.Clone(m.revoking)}
}

func (m *Manager) mutateLocked(mutate func(*state) error) error {

	if m.closed {
		return errors.New("network manager is closed")
	}
	current := m.stateLocked()
	next := m.stateLocked()
	if next.revoking == nil {
		next.revoking = make(map[revokedTuple]struct{})
	}
	if err := mutate(&next); err != nil {
		return err
	}
	if !reflect.DeepEqual(current.namespaces, next.namespaces) || !reflect.DeepEqual(current.endpoints, next.endpoints) || !reflect.DeepEqual(current.links, next.links) || !reflect.DeepEqual(current.services, next.services) {
		candidate := &Manager{logger: m.logger, namespaces: next.namespaces, endpoints: next.endpoints, links: next.links, services: next.services}
		if err := candidate.reconcileLocked(); err != nil {
			return err
		}
		m.namespaces, m.endpoints, m.links, m.services = next.namespaces, next.endpoints, next.links, next.services
		m.publishDNSLocked()
	}
	m.revoking = next.revoking
	m.lastError = deleteRevokedConntrack(m.revoking)
	if m.lastError == nil {
		clear(m.revoking)
	}
	m.scheduleExpiryLocked()
	return nil
}

func (next *state) revoke(sources, destinations []Endpoint, ports []uint32) {
	for _, source := range sources {
		for _, destination := range destinations {
			for _, port := range ports {
				next.revoking[revokedTuple{source.Address, destination.Address, uint16(port)}] = struct{}{}
			}
		}
	}
}

func (m *Manager) scheduleExpiryLocked() {
	if m.expiryTimer != nil {
		m.expiryTimer.Stop()
	}
	if m.closed {
		return
	}
	var earliest time.Time
	for _, service := range m.services {
		expiry := time.UnixMilli(service.ExpiresUnixMillis)
		if earliest.IsZero() || expiry.Before(earliest) {
			earliest = expiry
		}
	}
	if m.lastError != nil {
		retry := time.Now().Add(5 * time.Second)
		if earliest.IsZero() || retry.Before(earliest) {
			earliest = retry
		}
	}
	if earliest.IsZero() {
		return
	}
	delay := max(time.Until(earliest), 0)
	if m.lastError != nil && delay == 0 {
		delay = 5 * time.Second
	}
	if m.expiryTimer == nil {
		m.expiryTimer = time.AfterFunc(delay, m.expireServices)
	} else {
		m.expiryTimer.Reset(delay)
	}
}

func (m *Manager) expireServices() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	err := m.mutateLocked(func(next *state) error {
		for id, projection := range m.services {
			if projection.ExpiresUnixMillis <= time.Now().UnixMilli() {
				m.revokeProjectionLocked(next, projection)
				delete(next.services, id)
			}
		}
		return nil
	})
	if err != nil {
		m.lastError = err
		m.scheduleExpiryLocked()
	}
}

func (m *Manager) revokeProjectionLocked(next *state, projection *vmmdv1.ServiceProjection) {
	if !projection.Remote && projection.SourceNamespaceId == projection.DestinationNamespaceId {
		return
	}
	next.revoke(m.selectedEndpoints(projection.SourceNamespaceId, &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: []string{projection.SourceContainerId}}}}), m.serviceDestinationEndpoints(projection), []uint32{projection.DestinationPort})
}
