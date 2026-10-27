//go:build linux

package network

import (
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"google.golang.org/protobuf/proto"
)

var blockedEgressPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

var remoteServicePrefix = netip.MustParsePrefix(egresslink.ServicePrefix)

func blockedEgress(address netip.Addr) bool {
	for _, prefix := range blockedEgressPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func (m *Manager) ApplyLink(link *vmmdv1.NetworkLink) error {
	if err := validateLink(link); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.namespaces[link.GetSourceNamespaceId()]; !ok {
		return errors.New("source namespace does not exist")
	}
	if _, ok := m.namespaces[link.GetDestinationNamespaceId()]; !ok {
		return errors.New("destination namespace does not exist")
	}
	if current := m.links[link.GetId()]; current != nil && !proto.Equal(current, link) {
		return errors.New("network link already exists with different policy")
	}
	for id, current := range m.links {
		if id != link.GetId() && current.GetSourceNamespaceId() == link.GetSourceNamespaceId() && current.GetDnsAlias() == link.GetDnsAlias() {
			return errors.New("dns alias already exists in source namespace")
		}
	}
	return m.mutateLocked(func(next *state) error { next.links[link.Id] = proto.Clone(link).(*vmmdv1.NetworkLink); return nil })
}

func (m *Manager) DeleteLink(linkID string) error {
	if linkID == "" {
		return errors.New("link_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mutateLocked(func(next *state) error {
		link := m.links[linkID]
		if link != nil {
			next.revoke(m.selectedEndpoints(link.SourceNamespaceId, link.Source), m.selectedEndpoints(link.DestinationNamespaceId, link.Destination), link.TcpPorts)
			delete(next.links, linkID)
		}
		return nil
	})
}

// ApplyServiceProjection is the namespace-scoped service projection boundary.
// The host supplies exact current container instances; workloads never choose
// destination addresses or transport endpoints.
func (m *Manager) ApplyServiceProjection(projection *vmmdv1.ServiceProjection) error {
	if err := validateServiceProjection(projection); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	source := m.endpoints[projection.GetSourceNamespaceId()][projection.GetSourceContainerId()]
	if source.InstanceID != projection.GetSourceContainerInstanceId() {
		return errors.New("service projection container instance is stale")
	}
	if !projection.GetRemote() {
		destination := m.endpoints[projection.GetDestinationNamespaceId()][projection.GetDestinationContainerId()]
		if destination.InstanceID != projection.GetDestinationContainerInstanceId() {
			return errors.New("service projection container instance is stale")
		}
	}
	if current := m.services[projection.GetId()]; current != nil && !proto.Equal(current, projection) {
		return errors.New("service projection already exists with different policy")
	}
	for id, current := range m.services {
		if id != projection.GetId() && current.GetSourceNamespaceId() == projection.GetSourceNamespaceId() && current.GetDnsAlias() == projection.GetDnsAlias() {
			return errors.New("service projection DNS alias already exists")
		}
	}
	return m.mutateLocked(func(next *state) error {
		next.services[projection.Id] = proto.Clone(projection).(*vmmdv1.ServiceProjection)
		return nil
	})
}

func (m *Manager) ServiceProjectionSourceAddress(id string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	projection := m.services[id]
	if projection == nil {
		return "", errors.New("service projection does not exist")
	}
	endpoint := m.endpoints[projection.GetSourceNamespaceId()][projection.GetSourceContainerId()]
	if endpoint.InstanceID != projection.GetSourceContainerInstanceId() {
		return "", errors.New("service projection source instance is stale")
	}
	return endpoint.Address.String(), nil
}

func (m *Manager) DeleteServiceProjection(id string) error {
	if id == "" {
		return errors.New("service projection id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mutateLocked(func(next *state) error {
		projection := m.services[id]
		if projection != nil {
			m.revokeProjectionLocked(next, projection)
			delete(next.services, id)
		}
		return nil
	})
}

func validateServiceProjection(value *vmmdv1.ServiceProjection) error {
	if value == nil || value.GetId() == "" || value.GetSourceNamespaceId() == "" || value.GetSourceContainerId() == "" ||
		value.GetSourceContainerInstanceId() == "" || value.GetDestinationPort() == 0 || value.GetDestinationPort() > 65535 {
		return errors.New("complete service projection identity, source, and port are required")
	}
	if value.GetRemote() {
		address, err := netip.ParseAddr(value.GetVirtualAddress())
		if err != nil || !remoteServicePrefix.Contains(address) || value.GetDestinationNamespaceId() != "" ||
			value.GetDestinationContainerId() != "" || value.GetDestinationContainerInstanceId() != "" {
			return errors.New("remote service projection requires only a reserved virtual destination")
		}
	} else if value.GetDestinationNamespaceId() == "" || value.GetDestinationContainerId() == "" ||
		value.GetDestinationContainerInstanceId() == "" || value.GetVirtualAddress() != "" {
		return errors.New("local service projection requires an exact destination container")
	}
	if value.GetExpiresUnixMillis() <= time.Now().UnixMilli() {
		return errors.New("service projection expiry must be in the future")
	}
	alias := value.GetDnsAlias()
	if !validDNSLabel(alias) {
		return errors.New("service projection DNS alias must be a lowercase DNS label")
	}
	return nil
}

func (m *Manager) serviceDestinationEndpoints(projection *vmmdv1.ServiceProjection) []Endpoint {
	if projection.GetRemote() {
		address, err := netip.ParseAddr(projection.GetVirtualAddress())
		if err != nil {
			return nil
		}
		return []Endpoint{{Address: address}}
	}
	return m.selectedEndpoints(projection.GetDestinationNamespaceId(), &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: []string{projection.GetDestinationContainerId()}}}})
}

func validateLink(link *vmmdv1.NetworkLink) error {
	if link == nil || link.GetId() == "" || link.GetSourceNamespaceId() == "" || link.GetDestinationNamespaceId() == "" {
		return errors.New("link id and namespaces are required")
	}
	if link.GetSourceNamespaceId() == link.GetDestinationNamespaceId() {
		return errors.New("network link must cross namespaces")
	}
	if err := validateSelector(link.GetSource()); err != nil {
		return fmt.Errorf("source selector: %w", err)
	}
	if err := validateSelector(link.GetDestination()); err != nil {
		return fmt.Errorf("destination selector: %w", err)
	}
	if len(link.GetTcpPorts()) == 0 || len(link.GetTcpPorts()) > 256 {
		return errors.New("at least one TCP port is required")
	}
	seen := make(map[uint32]bool)
	for _, port := range link.GetTcpPorts() {
		if port == 0 || port > 65535 || seen[port] {
			return errors.New("TCP ports must be unique and between 1 and 65535")
		}
		seen[port] = true
	}
	alias := link.GetDnsAlias()
	if !validDNSLabel(alias) {
		return errors.New("dns_alias must be a lowercase DNS label")
	}
	return nil
}

func validateSelector(selector *vmmdv1.ContainerSelector) error {
	if selector == nil {
		return errors.New("selector is required")
	}
	switch selection := selector.GetSelection().(type) {
	case *vmmdv1.ContainerSelector_AllContainers:
		if !selection.AllContainers {
			return errors.New("all_containers must be true")
		}
	case *vmmdv1.ContainerSelector_ContainerIds:
		if selection.ContainerIds == nil || len(selection.ContainerIds.GetIds()) == 0 {
			return errors.New("container_ids must be non-empty")
		}
		seen := make(map[string]bool)
		for _, id := range selection.ContainerIds.GetIds() {
			if id == "" || seen[id] {
				return errors.New("container ids must be non-empty and unique")
			}
			seen[id] = true
		}
	default:
		return errors.New("selector choice is required")
	}
	return nil
}

func (m *Manager) selectedEndpoints(namespaceID string, selector *vmmdv1.ContainerSelector) []Endpoint {
	return selectEndpoints(m.endpoints[namespaceID], selector)
}

func selectEndpoints(all map[string]Endpoint, selector *vmmdv1.ContainerSelector) []Endpoint {
	result := make([]Endpoint, 0, len(all))
	if selector.GetAllContainers() {
		for _, endpoint := range all {
			result = append(result, endpoint)
		}
	} else {
		for _, id := range selector.GetContainerIds().GetIds() {
			if endpoint, ok := all[id]; ok {
				result = append(result, endpoint)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ContainerID < result[j].ContainerID })
	return result
}

func validDNSLabel(alias string) bool {
	return alias != "" && len(alias) <= 63 && strings.Trim(alias, "abcdefghijklmnopqrstuvwxyz0123456789-") == "" && alias[0] != '-' && alias[len(alias)-1] != '-'
}
