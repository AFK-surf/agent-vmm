//go:build linux

package network

import (
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
)

const (
	publicResolver     = egresslink.VirtualDNS
	netnsRoot          = "/run/netns"
	workloadResolvRoot = "/state/network/resolv"
	systemResolvRoot   = "/control/network/resolv"
)

type Namespace struct {
	ID         string
	Subnet     netip.Prefix
	Gateway    netip.Addr
	Bridge     string
	EgressMode vmmdv1.EgressMode
	LANEgress  bool
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
	logger      *slog.Logger
	mu          sync.RWMutex
	namespaces  map[string]Namespace
	endpoints   map[string]map[string]Endpoint
	settled     map[string]Counters
	settledOK   map[string]bool
	links       map[string]*vmmdv1.NetworkLink
	services    map[string]*vmmdv1.ServiceProjection
	dns         map[string]*dnsServer
	forwards    map[string]map[net.Conn]struct{}
	closed      bool
	revoking    map[revokedTuple]struct{}
	expiryTimer *time.Timer
	lastError   error
	dnsView     atomic.Pointer[dnsView]
}

func NewManager(logger *slog.Logger) (*Manager, error) {
	manager := &Manager{logger: logger.With("component", "guest-network"),
		namespaces: make(map[string]Namespace),
		endpoints:  make(map[string]map[string]Endpoint),
		settled:    make(map[string]Counters),
		settledOK:  make(map[string]bool),
		links:      make(map[string]*vmmdv1.NetworkLink),
		services:   make(map[string]*vmmdv1.ServiceProjection),
		dns:        make(map[string]*dnsServer),
		forwards:   make(map[string]map[net.Conn]struct{}),
	}
	manager.publishDNSLocked()
	if err := manager.reconcileLocked(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.expiryTimer != nil {
		m.expiryTimer.Stop()
	}
	for id, server := range m.dns {
		server.close(id)
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return errors.New("network manager is closed")
	}
	for id := range m.namespaces {
		if len(m.endpoints[id]) != 0 && m.dns[id] == nil {
			return errors.New("namespace DNS listener is unavailable")
		}
	}
	return m.lastError
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
	if current, ok := m.namespaces[id]; ok {
		if current.ID != namespace.ID || current.Subnet != namespace.Subnet || current.Gateway != namespace.Gateway || current.Bridge != namespace.Bridge {
			return errors.New("namespace network allocation changed")
		}
		namespace.LANEgress = current.LANEgress
	}
	if err := writeResolvConf(id, namespace.Gateway); err != nil {
		return err
	}
	err = m.mutateLocked(func(next *state) error {
		next.namespaces[id] = namespace
		if next.endpoints[id] == nil {
			next.endpoints[id] = make(map[string]Endpoint)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if _, ok := m.settledOK[id]; !ok {
		m.settledOK[id] = true
	}
	return nil
}

func (m *Manager) DeleteNamespace(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.canDeleteNamespaceLocked(id); err != nil {
		return err
	}
	if err := m.mutateLocked(func(next *state) error { delete(next.endpoints, id); delete(next.namespaces, id); return nil }); err != nil {
		return err
	}
	if server := m.dns[id]; server != nil {
		server.close(id)
		delete(m.dns, id)
	}
	delete(m.settled, id)
	delete(m.settledOK, id)
	if err := os.Remove(resolvPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (m *Manager) CanDeleteNamespace(id string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.canDeleteNamespaceLocked(id)
}

func (m *Manager) canDeleteNamespaceLocked(id string) error {
	if len(m.endpoints[id]) != 0 {
		return errors.New("namespace still has network endpoints")
	}
	for _, link := range m.links {
		if link.GetSourceNamespaceId() == id || link.GetDestinationNamespaceId() == id {
			return errors.New("namespace is referenced by a network link")
		}
	}
	for _, service := range m.services {
		if service.GetSourceNamespaceId() == id || service.GetDestinationNamespaceId() == id {
			return errors.New("namespace is referenced by a service projection")
		}
	}
	return nil
}

func (m *Manager) ResolvPath(namespaceID string) string {
	return resolvPath(namespaceID)
}

func writeResolvConf(id string, gateway netip.Addr) error {
	path := resolvPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	content := "nameserver " + gateway.String() + "\nsearch " + id + ".agent\noptions ndots:1\n"
	if current, err := os.ReadFile(path); err == nil && string(current) == content {
		return nil
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

func resolvPath(namespaceID string) string {
	root := workloadResolvRoot
	if strings.HasPrefix(namespaceID, "system.") {
		root = systemResolvRoot
	}
	return filepath.Join(root, namespaceID+".conf")
}

func (m *Manager) SetSystemLAN(id string, allowed bool) error {
	if !strings.HasPrefix(id, "system.") {
		return errors.New("LAN egress requires a system namespace")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	namespace, ok := m.namespaces[id]
	if !ok {
		return errors.New("system namespace does not exist")
	}
	namespace.LANEgress = allowed
	return m.mutateLocked(func(next *state) error { next.namespaces[id] = namespace; return nil })
}
