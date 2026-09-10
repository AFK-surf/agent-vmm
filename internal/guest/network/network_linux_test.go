//go:build linux

package network

import (
	"context"
	"io"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

func TestResolverStateFollowsNamespaceStorageClass(t *testing.T) {
	if got := resolvPath("workload-a"); got != "/state/network/resolv/workload-a.conf" {
		t.Fatalf("workload resolver path = %q", got)
	}
	if got := resolvPath("system.appliance"); got != "/control/network/resolv/system.appliance.conf" {
		t.Fatalf("system resolver path = %q", got)
	}
}

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

func TestDialContainerFencesInstanceAndDetachClosesForward(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	manager := &Manager{
		endpoints: map[string]map[string]Endpoint{"env": {"web": {NamespaceID: "env", ContainerID: "web", InstanceID: "current", Address: netip.MustParseAddr("127.0.0.1")}}},
		forwards:  make(map[string]map[net.Conn]struct{}),
	}
	if _, err := manager.DialContainer(context.Background(), "env", "web", "old", port); err == nil {
		t.Fatal("DialContainer accepted an old instance")
	}
	accepted := make(chan net.Conn, 1)
	go func() { connection, _ := listener.Accept(); accepted <- connection }()
	forward, err := manager.DialContainer(context.Background(), "env", "web", "current", port)
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	defer server.Close()
	manager.mu.Lock()
	manager.closeForwardsLocked("env/web")
	manager.mu.Unlock()
	if _, err := forward.Write([]byte("closed")); err == nil {
		_ = server.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 1)
		if _, readErr := server.Read(buffer); readErr != io.EOF {
			t.Fatalf("forward remained active after detach: %v", readErr)
		}
	}
}

func TestEndpointGenerationFencesReplacementAndStaleDetach(t *testing.T) {
	endpoint := Endpoint{NamespaceID: "env", ContainerID: "web", InstanceID: "current", HostInterface: "veth0"}
	manager := &Manager{
		namespaces: map[string]Namespace{"env": {ID: "env"}},
		endpoints:  map[string]map[string]Endpoint{"env": {"web": endpoint}},
		settled:    make(map[string]Counters),
		settledOK:  map[string]bool{"env": true},
		forwards:   make(map[string]map[net.Conn]struct{}),
	}
	if got, err := manager.Attach(context.Background(), "env", "web", "current"); err != nil || got.InstanceID != "current" {
		t.Fatalf("idempotent Attach() = %+v, %v", got, err)
	}
	if _, err := manager.Attach(context.Background(), "env", "web", "replacement"); err == nil {
		t.Fatal("replacement Attach() succeeded while the previous instance is attached")
	}
	if err := manager.Detach(context.Background(), "env", "web", "stale"); err != nil {
		t.Fatal(err)
	}
	if got := manager.endpoints["env"]["web"].InstanceID; got != "current" {
		t.Fatalf("stale Detach removed instance %q", got)
	}
}

func TestCounterDirectionAndSettledFailure(t *testing.T) {
	if got := orientCounters(Counters{RXBytes: 10, TXBytes: 20}, true); got != (Counters{RXBytes: 20, TXBytes: 10}) {
		t.Fatalf("orientCounters() = %+v", got)
	}
	manager := &Manager{
		namespaces: map[string]Namespace{"env": {ID: "env"}},
		endpoints:  map[string]map[string]Endpoint{"env": {}},
		settled:    make(map[string]Counters),
		settledOK:  map[string]bool{"env": false},
	}
	if _, err := manager.NamespaceCounters("env"); err == nil {
		t.Fatal("NamespaceCounters() hid a failed settled sample")
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

func TestServiceProjectionIsSourceScopedAndInstanceFenced(t *testing.T) {
	projection := &vmmdv1.ServiceProjection{Id: "route", SourceNamespaceId: "source", SourceContainerId: "client",
		SourceContainerInstanceId: "client-v1", DestinationNamespaceId: "destination", DestinationContainerId: "server",
		DestinationContainerInstanceId: "server-v1", DestinationPort: 8080, DnsAlias: "database", ExpiresUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
	if err := validateServiceProjection(projection); err != nil {
		t.Fatal(err)
	}
	invalid := proto.Clone(projection).(*vmmdv1.ServiceProjection)
	invalid.DnsAlias = "database.example"
	if err := validateServiceProjection(invalid); err == nil {
		t.Fatal("service projection accepted a non-label DNS alias")
	}
	manager := &Manager{
		namespaces: map[string]Namespace{"source": {ID: "source"}, "destination": {ID: "destination"}},
		endpoints: map[string]map[string]Endpoint{
			"source":      {"client": {ContainerID: "client", InstanceID: "client-v1", Address: netip.MustParseAddr("10.64.1.2")}},
			"destination": {"server": {ContainerID: "server", InstanceID: "server-v1", Address: netip.MustParseAddr("10.64.2.2")}},
		},
		services: map[string]*vmmdv1.ServiceProjection{"route": projection},
	}
	if got := manager.localDNSAddresses("source", "database.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.2")}) {
		t.Fatalf("source projection = %v", got)
	}
	if got := manager.localDNSAddresses("destination", "database.source.agent."); len(got) != 0 {
		t.Fatalf("destination namespace saw source projection: %v", got)
	}
	manager.endpoints["destination"]["server"] = Endpoint{ContainerID: "server", InstanceID: "server-v2", Address: netip.MustParseAddr("10.64.2.3")}
	if got := manager.localDNSAddresses("source", "database.source.agent."); len(got) != 0 {
		t.Fatalf("stale destination instance remained projected: %v", got)
	}
	remote := &vmmdv1.ServiceProjection{Id: "remote-route", SourceNamespaceId: "source", SourceContainerId: "client",
		SourceContainerInstanceId: "client-v1", DestinationPort: 8443, DnsAlias: "remote-api",
		ExpiresUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Remote: true, VirtualAddress: "198.19.4.9"}
	if err := validateServiceProjection(remote); err != nil {
		t.Fatal(err)
	}
	manager.services[remote.GetId()] = remote
	if got := manager.localDNSAddresses("source", "remote-api.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("198.19.4.9")}) {
		t.Fatalf("remote source projection = %v", got)
	}
	for name, mutate := range map[string]func(*vmmdv1.ServiceProjection){
		"outside reserved range":  func(value *vmmdv1.ServiceProjection) { value.VirtualAddress = "203.0.113.5" },
		"local destination mixed": func(value *vmmdv1.ServiceProjection) { value.DestinationNamespaceId = "destination" },
	} {
		invalidRemote := proto.Clone(remote).(*vmmdv1.ServiceProjection)
		mutate(invalidRemote)
		if err := validateServiceProjection(invalidRemote); err == nil {
			t.Fatalf("%s remote projection was accepted", name)
		}
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
