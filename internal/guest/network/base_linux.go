//go:build linux

package network

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

const (
	publicResolver = "192.168.88.2:53"
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
	NamespaceID string
	ContainerID string
	Address     netip.Addr
	NetNSPath   string
}

type Manager struct {
	mu         sync.RWMutex
	namespaces map[string]Namespace
	endpoints  map[string]map[string]Endpoint
	links      map[string]*vmmdv1.NetworkLink
	dns        map[string]*dnsServer
	forwards   map[string]map[net.Conn]struct{}
	closed     bool
}

func NewManager() (*Manager, error) {
	manager := &Manager{
		namespaces: make(map[string]Namespace),
		endpoints:  make(map[string]map[string]Endpoint),
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
	if current, ok := m.namespaces[id]; ok && current != namespace {
		return errors.New("namespace network configuration changed")
	}
	m.namespaces[id] = namespace
	if m.endpoints[id] == nil {
		m.endpoints[id] = make(map[string]Endpoint)
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
