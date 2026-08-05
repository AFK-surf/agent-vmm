//go:build linux

package network

import (
	"net/netip"
	"reflect"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func TestNetworkLinkPolicy(t *testing.T) {
	all := &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_AllContainers{AllContainers: true}}
	ids := &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: []string{"web"}}}}
	base := &vmmdv1.NetworkLink{Id: "link", SourceNamespaceId: "source", DestinationNamespaceId: "destination", Source: all, Destination: ids, TcpPorts: []uint32{443}, DnsAlias: "service"}
	tests := []struct {
		name   string
		mutate func(*vmmdv1.NetworkLink)
		valid  bool
	}{
		{name: "valid", mutate: func(*vmmdv1.NetworkLink) {}, valid: true},
		{name: "same namespace", mutate: func(link *vmmdv1.NetworkLink) { link.DestinationNamespaceId = "source" }},
		{name: "implicit all", mutate: func(link *vmmdv1.NetworkLink) { link.Source = nil }},
		{name: "empty ids", mutate: func(link *vmmdv1.NetworkLink) {
			link.Destination = &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{}}}
		}},
		{name: "empty ports", mutate: func(link *vmmdv1.NetworkLink) { link.TcpPorts = nil }},
		{name: "duplicate ports", mutate: func(link *vmmdv1.NetworkLink) { link.TcpPorts = []uint32{80, 80} }},
		{name: "invalid alias", mutate: func(link *vmmdv1.NetworkLink) { link.DnsAlias = "Service.Name" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			link := cloneLink(base)
			test.mutate(link)
			if err := validateLink(link); (err == nil) != test.valid {
				t.Fatalf("validateLink() = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestDNSViewTracksEndpointProjection(t *testing.T) {
	manager := &Manager{
		namespaces: map[string]Namespace{"source": {ID: "source"}, "destination": {ID: "destination"}},
		endpoints: map[string]map[string]Endpoint{
			"source":      {"client": {ContainerID: "client", Address: netip.MustParseAddr("10.64.1.2")}},
			"destination": {"web": {ContainerID: "web", Address: netip.MustParseAddr("10.64.2.2")}, "db": {ContainerID: "db", Address: netip.MustParseAddr("10.64.2.3")}},
		},
		links: map[string]*vmmdv1.NetworkLink{
			"link": {Id: "link", SourceNamespaceId: "source", DestinationNamespaceId: "destination", Source: allSelector(), Destination: idsSelector("web"), TcpPorts: []uint32{80}, DnsAlias: "service"},
		},
	}
	if got := manager.localDNSAddresses("source", "client.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.1.2")}) {
		t.Fatalf("local address = %v", got)
	}
	if got := manager.localDNSAddresses("source", "service.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.2")}) {
		t.Fatalf("linked address = %v", got)
	}
	if got := manager.localDNSAddresses("destination", "service.source.agent."); len(got) != 0 {
		t.Fatalf("destination saw source alias: %v", got)
	}
	manager.endpoints["destination"]["web"] = Endpoint{ContainerID: "web", Address: netip.MustParseAddr("10.64.2.8")}
	if got := manager.localDNSAddresses("source", "service.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.8")}) {
		t.Fatalf("address after churn = %v", got)
	}
}

func TestEgressDestinationClassification(t *testing.T) {
	tests := []struct {
		address string
		blocked bool
	}{
		{address: "10.64.1.2", blocked: true},
		{address: "169.254.169.254", blocked: true},
		{address: "192.168.1.1", blocked: true},
		{address: "198.18.0.1", blocked: false},
		{address: "1.1.1.1", blocked: false},
		{address: "203.0.113.1", blocked: true},
	}
	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			if got := blockedEgress(netip.MustParseAddr(test.address)); got != test.blocked {
				t.Fatalf("blockedEgress() = %v", got)
			}
		})
	}
}

func allSelector() *vmmdv1.ContainerSelector {
	return &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_AllContainers{AllContainers: true}}
}

func idsSelector(ids ...string) *vmmdv1.ContainerSelector {
	return &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: ids}}}
}

func cloneLink(link *vmmdv1.NetworkLink) *vmmdv1.NetworkLink {
	return &vmmdv1.NetworkLink{Id: link.Id, SourceNamespaceId: link.SourceNamespaceId, DestinationNamespaceId: link.DestinationNamespaceId, Source: link.Source, Destination: link.Destination, TcpPorts: append([]uint32(nil), link.TcpPorts...), DnsAlias: link.DnsAlias}
}
