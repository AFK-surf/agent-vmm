//go:build linux

package network

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
)

const (
	publicResolver = netprotocol.VirtualDNS
	netnsRoot      = "/run/netns"
	resolvRoot     = "/state/network/resolv"
)

type Namespace struct {
	ID         string
	Subnet     netip.Prefix
	Gateway    netip.Addr
	Bridge     string
	EgressMode vmmdv1.EgressMode
}

type Endpoint struct {
	NamespaceID   string
	ContainerID   string
	InstanceID    string
	HostInterface string
	Address       netip.Addr
	NetNSPath     string
}

type Counters struct {
	RXBytes uint64
	TXBytes uint64
}

func (m *Manager) NamespaceCounters(namespaceID string) (Counters, error) {
	m.mu.RLock()
	_, ok := m.namespaces[namespaceID]
	if !ok {
		m.mu.RUnlock()
		return Counters{}, errors.New("network namespace does not exist")
	}
	result := m.settled[namespaceID]
	if ok, tracked := m.settledOK[namespaceID]; tracked && !ok {
		m.mu.RUnlock()
		return Counters{}, errors.New("network counters became unavailable")
	}
	for _, endpoint := range m.endpoints[namespaceID] {
		counters, err := interfaceCounters(endpoint.HostInterface, true)
		if err != nil {
			m.mu.RUnlock()
			return Counters{}, err
		}
		result.RXBytes += counters.RXBytes
		result.TXBytes += counters.TXBytes
	}
	m.mu.RUnlock()
	return result, nil
}

func (m *Manager) ContainerCounters(namespaceID, containerID string) (Counters, error) {
	m.mu.RLock()
	endpoint, ok := m.endpoints[namespaceID][containerID]
	m.mu.RUnlock()
	if !ok || endpoint.HostInterface == "" {
		return Counters{}, errors.New("container network endpoint is unavailable")
	}
	return interfaceCounters(endpoint.HostInterface, true)
}

func interfaceCounters(name string, swapDirection bool) (Counters, error) {
	read := func(field string) (uint64, error) {
		data, err := os.ReadFile(filepath.Join("/sys/class/net", name, "statistics", field))
		if err != nil {
			return 0, err
		}
		return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	}
	rx, err := read("rx_bytes")
	if err != nil {
		return Counters{}, err
	}
	tx, err := read("tx_bytes")
	if err != nil {
		return Counters{}, err
	}
	return orientCounters(Counters{RXBytes: rx, TXBytes: tx}, swapDirection), nil
}

func orientCounters(counters Counters, swapDirection bool) Counters {
	if swapDirection {
		counters.RXBytes, counters.TXBytes = counters.TXBytes, counters.RXBytes
	}
	return counters
}

type Manager struct {
	mu         sync.RWMutex
	namespaces map[string]Namespace
	endpoints  map[string]map[string]Endpoint
	settled    map[string]Counters
	settledOK  map[string]bool
	links      map[string]*vmmdv1.NetworkLink
	dns        map[string]*dnsServer
	forwards   map[string]map[net.Conn]struct{}
	closed     bool
}

func NewManager() (*Manager, error) {
	manager := &Manager{
		namespaces: make(map[string]Namespace),
		endpoints:  make(map[string]map[string]Endpoint),
		settled:    make(map[string]Counters),
		settledOK:  make(map[string]bool),
		links:      make(map[string]*vmmdv1.NetworkLink),
		dns:        make(map[string]*dnsServer),
		forwards:   make(map[string]map[net.Conn]struct{}),
	}
	if err := manager.reconcileLocked(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for id, server := range m.dns {
		server.close()
		delete(m.dns, id)
	}
	for _, connections := range m.forwards {
		for connection := range connections {
			if tracked, ok := connection.(*trackedConn); ok {
				_ = tracked.Conn.Close()
			} else {
				_ = connection.Close()
			}
		}
	}
	return nil
}

func (m *Manager) Health() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("network manager is closed")
	}
	return m.reconcileLocked()
}

func (m *Manager) EnsureNamespace(id, subnet string, egress vmmdv1.EgressMode) error {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() != 24 {
		return errors.New("namespace subnet must be an IPv4 /24")
	}
	namespace := Namespace{
		ID: id, Subnet: prefix.Masked(), Gateway: prefix.Addr().Next(),
		Bridge: bridgeName(id), EgressMode: egress,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("network manager is closed")
	}
	if current, ok := m.namespaces[id]; ok && (current.ID != namespace.ID || current.Subnet != namespace.Subnet || current.Gateway != namespace.Gateway || current.Bridge != namespace.Bridge) {
		return errors.New("namespace network allocation changed")
	}
	m.namespaces[id] = namespace
	if m.endpoints[id] == nil {
		m.endpoints[id] = make(map[string]Endpoint)
	}
	if _, ok := m.settledOK[id]; !ok {
		m.settledOK[id] = true
	}
	if err := writeResolvConf(id, namespace.Gateway); err != nil {
		return err
	}
	return m.reconcileLocked()
}

func (m *Manager) DeleteNamespace(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.endpoints[id]) != 0 {
		return errors.New("namespace still has network endpoints")
	}
	for _, link := range m.links {
		if link.GetSourceNamespaceId() == id || link.GetDestinationNamespaceId() == id {
			return errors.New("namespace is referenced by a network link")
		}
	}
	if server := m.dns[id]; server != nil {
		server.close()
		delete(m.dns, id)
	}
	delete(m.endpoints, id)
	delete(m.settled, id)
	delete(m.settledOK, id)
	delete(m.namespaces, id)
	if err := os.Remove(filepath.Join(resolvRoot, id+".conf")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return m.reconcileLocked()
}

func (m *Manager) CanDeleteNamespace(id string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.endpoints[id]) != 0 {
		return errors.New("namespace still has network endpoints")
	}
	for _, link := range m.links {
		if link.GetSourceNamespaceId() == id || link.GetDestinationNamespaceId() == id {
			return errors.New("namespace is referenced by a network link")
		}
	}
	return nil
}

func (m *Manager) ResolvPath(namespaceID string) string {
	return filepath.Join(resolvRoot, namespaceID+".conf")
}

func writeResolvConf(id string, gateway netip.Addr) error {
	if err := os.MkdirAll(resolvRoot, 0o700); err != nil {
		return err
	}
	content := "nameserver " + gateway.String() + "\nsearch " + id + ".agent\noptions ndots:1\n"
	return os.WriteFile(filepath.Join(resolvRoot, id+".conf"), []byte(content), 0o644)
}
