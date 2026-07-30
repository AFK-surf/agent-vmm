//go:build linux

package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/mdlayher/netlink"
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
	m.links[link.GetId()] = proto.Clone(link).(*vmmdv1.NetworkLink)
	return m.reconcileLocked()
}

func (m *Manager) DeleteLink(linkID string) error {
	if linkID == "" {
		return errors.New("link_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	link := m.links[linkID]
	if link == nil {
		return nil
	}
	delete(m.links, linkID)
	if err := m.reconcileLocked(); err != nil {
		m.links[linkID] = link
		return err
	}
	return deleteLinkConntrack(m.selectedEndpoints(link.GetSourceNamespaceId(), link.GetSource()), m.selectedEndpoints(link.GetDestinationNamespaceId(), link.GetDestination()), link.GetTcpPorts())
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
	if alias == "" || len(alias) > 63 || strings.Trim(alias, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" || alias[0] == '-' || alias[len(alias)-1] == '-' {
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
	all := m.endpoints[namespaceID]
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

func (m *Manager) reconcileLocked() error {
	deadline := time.Now().Add(5 * time.Second)
	connection, err := nftables.New(nftables.AsLasting(), nftables.WithSockOptions(func(socket *netlink.Conn) error {
		if err := socket.SetReadDeadline(deadline); err != nil {
			return err
		}
		return socket.SetWriteDeadline(deadline)
	}))
	if err != nil {
		return err
	}
	defer connection.CloseLasting()
	tables, err := connection.ListTables()
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table.Name == "agent-vmm" && (table.Family == nftables.TableFamilyINet || table.Family == nftables.TableFamilyIPv4) {
			connection.DelTable(table)
		}
	}
	filterTable := connection.AddTable(&nftables.Table{Name: "agent-vmm", Family: nftables.TableFamilyINet})
	policy := nftables.ChainPolicyDrop
	forward := connection.AddChain(&nftables.Chain{Name: "forward", Table: filterTable, Type: nftables.ChainTypeFilter, Hooknum: nftables.ChainHookForward, Priority: nftables.ChainPriorityFilter, Policy: &policy})
	connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: establishedRule()})

	namespaceIDs := make([]string, 0, len(m.namespaces))
	sets := make(map[string]*nftables.Set)
	for id := range m.namespaces {
		namespaceIDs = append(namespaceIDs, id)
	}
	sort.Strings(namespaceIDs)
	for index, id := range namespaceIDs {
		namespace := m.namespaces[id]
		set := &nftables.Set{Table: filterTable, Name: fmt.Sprintf("ns%d", index), KeyType: nftables.TypeIPAddr}
		elements := endpointElements(m.endpoints[id])
		if err := connection.AddSet(set, elements); err != nil {
			return err
		}
		sets[id] = set
		connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: interfacePairRule(namespace.Bridge, namespace.Bridge, expr.VerdictAccept)})
		if namespace.EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET {
			for _, blocked := range blockedEgressPrefixes {
				connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: append(sourceSetRule(set), prefixDestinationRule(blocked, expr.VerdictDrop)...)})
			}
			connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: append(sourceSetRule(set), &expr.Verdict{Kind: expr.VerdictAccept})})
		}
	}

	linkIDs := make([]string, 0, len(m.links))
	for id := range m.links {
		linkIDs = append(linkIDs, id)
	}
	sort.Strings(linkIDs)
	for _, id := range linkIDs {
		link := m.links[id]
		sources := selectedElements(m.selectedEndpoints(link.GetSourceNamespaceId(), link.GetSource()))
		destinations := selectedElements(m.selectedEndpoints(link.GetDestinationNamespaceId(), link.GetDestination()))
		if len(sources) == 0 || len(destinations) == 0 {
			continue
		}
		sourceSet := &nftables.Set{Table: filterTable, Name: "ls" + shortSetID(id), KeyType: nftables.TypeIPAddr}
		destinationSet := &nftables.Set{Table: filterTable, Name: "ld" + shortSetID(id), KeyType: nftables.TypeIPAddr}
		portSet := &nftables.Set{Table: filterTable, Name: "lp" + shortSetID(id), KeyType: nftables.TypeInetService, KeyByteOrder: binaryutil.BigEndian}
		if err := connection.AddSet(sourceSet, sources); err != nil {
			return err
		}
		if err := connection.AddSet(destinationSet, destinations); err != nil {
			return err
		}
		if err := connection.AddSet(portSet, portElements(link.GetTcpPorts())); err != nil {
			return err
		}
		connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: linkRule(sourceSet, destinationSet, portSet)})
	}

	natTable := connection.AddTable(&nftables.Table{Name: "agent-vmm", Family: nftables.TableFamilyIPv4})
	postrouting := connection.AddChain(&nftables.Chain{Name: "postrouting", Table: natTable, Type: nftables.ChainTypeNAT, Hooknum: nftables.ChainHookPostrouting, Priority: nftables.ChainPriorityNATSource})
	for _, id := range namespaceIDs {
		if m.namespaces[id].EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET {
			connection.AddRule(&nftables.Rule{Table: natTable, Chain: postrouting, Exprs: append(prefixSourceRule(m.namespaces[id].Subnet), &expr.Masq{})})
		}
	}
	return connection.Flush()
}

func endpointElements(endpoints map[string]Endpoint) []nftables.SetElement {
	values := make([]Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		values = append(values, endpoint)
	}
	return selectedElements(values)
}

func selectedElements(endpoints []Endpoint) []nftables.SetElement {
	elements := make([]nftables.SetElement, 0, len(endpoints))
	for _, endpoint := range endpoints {
		elements = append(elements, nftables.SetElement{Key: endpoint.Address.AsSlice()})
	}
	return elements
}

func portElements(ports []uint32) []nftables.SetElement {
	elements := make([]nftables.SetElement, 0, len(ports))
	for _, port := range ports {
		value := make([]byte, 2)
		binary.BigEndian.PutUint16(value, uint16(port))
		elements = append(elements, nftables.SetElement{Key: value})
	}
	return elements
}

func shortSetID(id string) string {
	var value uint32 = 2166136261
	for i := range len(id) {
		value = (value ^ uint32(id[i])) * 16777619
	}
	return fmt.Sprintf("%08x", value)
}

func establishedRule() []expr.Any {
	state := expr.CtStateBitESTABLISHED | expr.CtStateBitRELATED
	return []expr.Any{
		&expr.Ct{Register: 1, Key: expr.CtKeySTATE},
		&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: 4, Mask: binaryutil.NativeEndian.PutUint32(state), Xor: binaryutil.NativeEndian.PutUint32(0)},
		&expr.Cmp{Op: expr.CmpOpNeq, Register: 1, Data: []byte{0, 0, 0, 0}},
		&expr.Verdict{Kind: expr.VerdictAccept},
	}
}

func interfacePairRule(input, output string, verdict expr.VerdictKind) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: interfaceName(input)},
		&expr.Meta{Key: expr.MetaKeyOIFNAME, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: interfaceName(output)},
		&expr.Verdict{Kind: verdict},
	}
}

func interfaceName(name string) []byte { value := make([]byte, 16); copy(value, name); return value }

func sourceSetRule(set *nftables.Set) []expr.Any {
	return []expr.Any{&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4}, &expr.Lookup{SourceRegister: 1, SetName: set.Name, SetID: set.ID}}
}

func prefixDestinationRule(prefix netip.Prefix, verdict expr.VerdictKind) []expr.Any {
	mask := net.CIDRMask(prefix.Bits(), 32)
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4},
		&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: 4, Mask: mask, Xor: []byte{0, 0, 0, 0}},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: prefix.Masked().Addr().AsSlice()}, &expr.Verdict{Kind: verdict},
	}
}

func prefixSourceRule(prefix netip.Prefix) []expr.Any {
	mask := net.CIDRMask(prefix.Bits(), 32)
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4},
		&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: 4, Mask: mask, Xor: []byte{0, 0, 0, 0}},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: prefix.Masked().Addr().AsSlice()},
	}
}

func linkRule(source, destination, ports *nftables.Set) []expr.Any {
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4}, &expr.Lookup{SourceRegister: 1, SetName: source.Name, SetID: source.ID},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4}, &expr.Lookup{SourceRegister: 1, SetName: destination.Name, SetID: destination.ID},
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{6}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2}, &expr.Lookup{SourceRegister: 1, SetName: ports.Name, SetID: ports.ID},
		&expr.Verdict{Kind: expr.VerdictAccept},
	}
}
