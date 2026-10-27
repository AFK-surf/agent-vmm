//go:build linux

package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/mdlayher/netlink"
)

func (m *Manager) reconcileLocked() error {
	deadline := time.Now().Add(5 * time.Second)
	connection, err := nftables.New(nftables.AsLasting(), nftables.WithSockOptions(func(socket *netlink.Conn) error {
		// Rule creation returns ACKs and full rule echoes before Flush reads them.
		if err := socket.SetReadBuffer(4 << 20); err != nil {
			return err
		}
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
	inputPolicy := nftables.ChainPolicyAccept
	input := connection.AddChain(&nftables.Chain{Name: "input", Table: filterTable, Type: nftables.ChainTypeFilter, Hooknum: nftables.ChainHookInput, Priority: nftables.ChainPriorityFilter, Policy: &inputPolicy})
	connection.AddRule(&nftables.Rule{Table: filterTable, Chain: input, Exprs: establishedRule()})

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
		for _, protocol := range []byte{6, 17} {
			connection.AddRule(&nftables.Rule{Table: filterTable, Chain: input, Exprs: []expr.Any{
				&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: interfaceName(namespace.Bridge)},
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: namespace.Gateway.AsSlice()},
				&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{protocol}},
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{0, 53}},
				&expr.Verdict{Kind: expr.VerdictAccept},
			}})
		}
		connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: interfacePairRule(namespace.Bridge, namespace.Bridge, expr.VerdictAccept)})

	}

	connection.AddRule(&nftables.Rule{Table: filterTable, Chain: input, Exprs: []expr.Any{
		&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte("avm")}, &expr.Verdict{Kind: expr.VerdictDrop},
	}})
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
	serviceIDs := make([]string, 0, len(m.services))
	for id := range m.services {
		serviceIDs = append(serviceIDs, id)
	}
	sort.Strings(serviceIDs)
	for _, id := range serviceIDs {
		service := m.services[id]
		if service.GetExpiresUnixMillis() <= time.Now().UnixMilli() {
			continue
		}
		if !service.GetRemote() && service.GetSourceNamespaceId() == service.GetDestinationNamespaceId() {
			continue
		}
		sources := selectedElements(m.selectedEndpoints(service.GetSourceNamespaceId(), &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: []string{service.GetSourceContainerId()}}}}))
		destinations := selectedElements(m.serviceDestinationEndpoints(service))
		if len(sources) == 0 || len(destinations) == 0 {
			continue
		}
		sourceSet := &nftables.Set{Table: filterTable, Name: "ss" + shortSetID(id), KeyType: nftables.TypeIPAddr}
		destinationSet := &nftables.Set{Table: filterTable, Name: "sd" + shortSetID(id), KeyType: nftables.TypeIPAddr}
		portSet := &nftables.Set{Table: filterTable, Name: "sp" + shortSetID(id), KeyType: nftables.TypeInetService, KeyByteOrder: binaryutil.BigEndian}
		if err := connection.AddSet(sourceSet, sources); err != nil {
			return err
		}
		if err := connection.AddSet(destinationSet, destinations); err != nil {
			return err
		}
		if err := connection.AddSet(portSet, portElements([]uint32{service.GetDestinationPort()})); err != nil {
			return err
		}
		connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: linkRule(sourceSet, destinationSet, portSet)})
	}

	for _, id := range namespaceIDs {
		namespace := m.namespaces[id]
		set := sets[id]
		if namespace.EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET || namespace.LANEgress {
			for _, blocked := range blockedEgressPrefixes {
				if namespace.LANEgress && blocked.Addr().IsPrivate() {
					continue
				}
				connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: append(sourceSetRule(set), prefixDestinationRule(blocked, expr.VerdictDrop)...)})
			}
			connection.AddRule(&nftables.Rule{Table: filterTable, Chain: forward, Exprs: append(sourceSetRule(set), outputInterfaceRule(egresslink.TunName, expr.VerdictAccept)...)})
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

func outputInterfaceRule(output string, verdict expr.VerdictKind) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyOIFNAME, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: interfaceName(output)},
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

func linkRule(source, destination, ports *nftables.Set) []expr.Any {
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4}, &expr.Lookup{SourceRegister: 1, SetName: source.Name, SetID: source.ID},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4}, &expr.Lookup{SourceRegister: 1, SetName: destination.Name, SetID: destination.ID},
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1}, &expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{6}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2}, &expr.Lookup{SourceRegister: 1, SetName: ports.Name, SetID: ports.ID},
		&expr.Verdict{Kind: expr.VerdictAccept},
	}
}
