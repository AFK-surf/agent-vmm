//go:build linux

package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/sys/unix"
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
		forwards:  make(map[string]map[net.Conn]struct{}), logger: slog.Default().With("component", "guest-network"),
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
		forwards:   make(map[string]map[net.Conn]struct{}), logger: slog.Default().With("component", "guest-network"),
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
		settledOK:  map[string]bool{"env": false}, logger: slog.Default().With("component", "guest-network"),
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
		}, logger: slog.Default().With("component", "guest-network"),
	}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "client.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.1.2")}) {
		t.Fatalf("local address = %v", got)
	}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "service.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.2")}) {
		t.Fatalf("linked address = %v", got)
	}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("destination", "service.source.agent."); len(got) != 0 {
		t.Fatalf("destination saw source alias: %v", got)
	}
	manager.endpoints["destination"]["web"] = Endpoint{ContainerID: "web", Address: netip.MustParseAddr("10.64.2.8")}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "service.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.8")}) {
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
		services: map[string]*vmmdv1.ServiceProjection{"route": projection}, logger: slog.Default().With("component", "guest-network"),
	}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "database.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("10.64.2.2")}) {
		t.Fatalf("source projection = %v", got)
	}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("destination", "database.source.agent."); len(got) != 0 {
		t.Fatalf("destination namespace saw source projection: %v", got)
	}
	for _, test := range []struct {
		namespace, name string
		want            bool
	}{
		{"source", "database.services.agent.", true},
		{"destination", "database.services.agent.", false},
		{"source", "neighbor.services.agent.", false},
		{"source", "client.services.agent.", false},
	} {
		if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses(test.namespace, test.name); (len(got) > 0) != test.want {
			t.Fatalf("source-scoped %s/%s = %v", test.namespace, test.name, got)
		}
	}
	delete(manager.services, "route")
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "database.services.agent."); len(got) != 0 {
		t.Fatalf("revoked import still resolved: %v", got)
	}
	manager.services["route"] = projection
	manager.endpoints["destination"]["server"] = Endpoint{ContainerID: "server", InstanceID: "server-v2", Address: netip.MustParseAddr("10.64.2.3")}
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "database.source.agent."); len(got) != 0 {
		t.Fatalf("stale destination instance remained projected: %v", got)
	}
	remote := &vmmdv1.ServiceProjection{Id: "remote-route", SourceNamespaceId: "source", SourceContainerId: "client",
		SourceContainerInstanceId: "client-v1", DestinationPort: 8443, DnsAlias: "remote-api",
		ExpiresUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Remote: true, VirtualAddress: "100.126.4.9"}
	if err := validateServiceProjection(remote); err != nil {
		t.Fatal(err)
	}
	manager.services[remote.GetId()] = remote
	if got := (&dnsView{state: manager.stateLocked()}).localDNSAddresses("source", "remote-api.source.agent."); !reflect.DeepEqual(got, []netip.Addr{netip.MustParseAddr("100.126.4.9")}) {
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

// Requires CAP_SYS_ADMIN and CAP_NET_ADMIN to create an isolated network namespace.
func TestKernelPolicyReconcilePublicNamespaces(t *testing.T) {
	if os.Getenv("AGENT_VMM_NFTABLES_TEST") != "1" {
		t.Skip("requires an isolated kernel network namespace")
	}
	if os.Getenv("AGENT_VMM_NFTABLES_HELPER") != "1" {
		runtime.LockOSThread()
		original, err := unix.Open("/proc/thread-self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			runtime.UnlockOSThread()
			t.Fatal(err)
		}
		if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
			_ = unix.Close(original)
			runtime.UnlockOSThread()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			defer unix.Close(original)
			if err := unix.Setns(original, unix.CLONE_NEWNET); err != nil {
				t.Errorf("restore network namespace: %v", err)
				return
			}
			runtime.UnlockOSThread()
		})
		command := exec.Command(os.Args[0], "-test.run=^TestKernelPolicyReconcilePublicNamespaces$", "-test.v")
		command.Env = append(os.Environ(), "AGENT_VMM_NFTABLES_HELPER=1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("private network helper: %v\n%s", err, output)
		}
		t.Logf("%s", output)
		return
	}
	manager := &Manager{
		namespaces: make(map[string]Namespace),
		endpoints:  make(map[string]map[string]Endpoint),
		dns:        make(map[string]*dnsServer), logger: slog.Default().With("component", "guest-network"),
	}
	t.Cleanup(func() {
		connection := &nftables.Conn{}
		connection.DelTable(&nftables.Table{Name: "agent-vmm", Family: nftables.TableFamilyINet})
		if err := connection.Flush(); err != nil {
			t.Errorf("remove test policy: %v", err)
		}
	})
	t.Cleanup(func() { manager.Close() })
	for index := range 16 {
		id := fmt.Sprintf("public-%d", index)
		manager.namespaces[id] = Namespace{ID: id, Gateway: netip.MustParseAddr(fmt.Sprintf("127.64.%d.1", index)), Bridge: fmt.Sprintf("bridge%d", index), EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}
		manager.endpoints[id] = map[string]Endpoint{"worker": {Address: netip.MustParseAddr(fmt.Sprintf("10.64.%d.2", index))}}
		manager.publishDNSLocked()
		if err := manager.startDNSLocked(id); err != nil {
			t.Fatal(err)
		}
		if err := manager.reconcileLocked(); err != nil {
			t.Fatalf("reconcile %d public namespaces: %v", index+1, err)
		}
	}
	manager.settledOK = make(map[string]bool)
	for _, id := range []string{"system.lan-replay", "workload-no-lan"} {
		subnet := "127.65.0.0/24"
		if id == "workload-no-lan" {
			subnet = "127.65.1.0/24"
		}
		if err := manager.EnsureNamespace(id, subnet, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(resolvPath(id)) })
		if id == "system.lan-replay" {
			if err := manager.SetSystemLAN(id, true); err != nil {
				t.Fatal(err)
			}
		}
		if err := manager.EnsureNamespace(id, subnet, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
			t.Fatal(err)
		}
		if got := manager.namespaces[id].LANEgress; got != (id == "system.lan-replay") {
			t.Fatalf("namespace replay changed LAN grant: %s %t", id, got)
		}
	}
	// Health reads the installed state without changing rules.
	if err := manager.Health(); err != nil {
		t.Fatalf("repeat policy reconciliation: %v", err)
	}
	manager.services = map[string]*vmmdv1.ServiceProjection{"gitea": {Id: "gitea", SourceNamespaceId: "public-0", SourceContainerId: "worker", DestinationNamespaceId: "public-1", DestinationContainerId: "worker", DestinationPort: 3000, ExpiresUnixMillis: time.Now().Add(time.Hour).UnixMilli()}}
	manager.links = map[string]*vmmdv1.NetworkLink{"approved-link": {Id: "approved-link", SourceNamespaceId: "public-0", DestinationNamespaceId: "public-1", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{22}}}
	if err := manager.reconcileLocked(); err != nil {
		t.Fatal(err)
	}
	connection := &nftables.Conn{}
	table := &nftables.Table{Name: "agent-vmm", Family: nftables.TableFamilyINet}
	rules, err := connection.GetRules(table, &nftables.Chain{Name: "forward", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	firstDrop, serviceAccept, linkAccept := -1, -1, -1
	for index, rule := range rules {
		kind := ""
		for _, expression := range rule.Exprs {
			if lookup, ok := expression.(*expr.Lookup); ok {
				if strings.HasPrefix(lookup.SetName, "ss") {
					kind = "service"
				}
				if strings.HasPrefix(lookup.SetName, "ls") {
					kind = "link"
				}
			}
			if verdict, ok := expression.(*expr.Verdict); ok {
				if verdict.Kind == expr.VerdictDrop && firstDrop < 0 {
					firstDrop = index
				}
				if verdict.Kind == expr.VerdictAccept && kind == "service" {
					serviceAccept = index
				}
				if verdict.Kind == expr.VerdictAccept && kind == "link" {
					linkAccept = index
				}
			}
		}
	}
	if firstDrop < 0 || serviceAccept < 0 || linkAccept < 0 || serviceAccept > firstDrop || linkAccept > firstDrop {
		t.Fatalf("installed rule order: service=%d link=%d RFC1918 deny=%d", serviceAccept, linkAccept, firstDrop)
	}
	manager.mu.Lock()
	err = manager.mutateLocked(func(next *state) error {
		projection := proto.Clone(next.services["gitea"]).(*vmmdv1.ServiceProjection)
		projection.ExpiresUnixMillis = time.Now().Add(25 * time.Millisecond).UnixMilli()
		next.services["gitea"] = projection
		return nil
	})
	manager.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	expiryDeadline := time.Now().Add(time.Second)
	for {
		manager.mu.RLock()
		_, pending := manager.services["gitea"]
		settled := len(manager.revoking) == 0 && manager.lastError == nil
		manager.mu.RUnlock()
		if !pending && settled {
			break
		}
		if time.Now().After(expiryDeadline) {
			t.Fatalf("projection expiry did not settle: pending=%t health=%v", pending, manager.Health())
		}
		time.Sleep(time.Millisecond)
	}
	oldDNS := manager.dns["public-0"]
	oldDNS.udp.Close()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.RLock()
		retired := manager.dns["public-0"] != nil && manager.dns["public-0"] != oldDNS
		manager.mu.RUnlock()
		if retired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dead DNS listener remains registered")
		}
		time.Sleep(time.Millisecond)
	}
	if err := manager.Health(); err != nil {
		t.Fatalf("restore listener in network reconciliation: %v", err)
	}
	manager.mu.RLock()
	replacementDNS := manager.dns["public-0"]
	manager.mu.RUnlock()
	if replacementDNS == nil || replacementDNS == oldDNS {
		t.Fatal("network reconciliation did not replace dead DNS listener")
	}
	// Keep nftables authority installed while conntrack loses its kernel authority.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var capabilities [2]unix.CapUserData
	if err := unix.Capget(&header, &capabilities[0]); err != nil {
		t.Fatal(err)
	}
	restored := capabilities
	capabilities[unix.CAP_NET_ADMIN/32].Effective &^= 1 << (unix.CAP_NET_ADMIN % 32)
	if err := unix.Capset(&header, &capabilities[0]); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Capset(&header, &restored[0]); err != nil {
			t.Errorf("restore kernel authority: %v", err)
		}
	}()
	tuple := revokedTuple{netip.MustParseAddr("10.64.0.2"), netip.MustParseAddr("10.64.1.2"), 3000}
	manager.mu.Lock()
	err = manager.mutateLocked(func(next *state) error {
		next.revoking[tuple] = struct{}{}
		return nil
	})
	pending := len(manager.revoking)
	manager.mu.Unlock()
	if err != nil || manager.Health() == nil || pending != 1 {
		t.Fatalf("failed revocation = %v, health = %v, pending = %d", err, manager.Health(), pending)
	}
	if err := manager.EnsureNamespace("system.lan-replay", "127.65.0.0/24", vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatalf("unrelated namespace replay returned conntrack failure: %v", err)
	}
	const cleanupID = "system.revocation-cleanup"
	if err := writeResolvConf(cleanupID, netip.MustParseAddr("127.65.2.1")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(resolvPath(cleanupID)) })
	if err := manager.DeleteNamespace(cleanupID); err != nil {
		t.Fatalf("unrelated namespace cleanup returned conntrack failure: %v", err)
	}
	if _, err := os.Stat(resolvPath(cleanupID)); !os.IsNotExist(err) {
		t.Fatalf("namespace resolver survived committed cleanup: %v", err)
	}
	if manager.Health() == nil {
		t.Fatal("unrelated mutations hid the pending conntrack failure")
	}
	if err := unix.Capset(&header, &restored[0]); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	err = manager.mutateLocked(func(*state) error { return nil })
	pending = len(manager.revoking)
	manager.mu.Unlock()
	if err != nil || manager.Health() != nil || pending != 0 {
		t.Fatalf("revocation retry = %v, health = %v, pending = %d", err, manager.Health(), pending)
	}
	chains, err := connection.ListChains()
	if err != nil {
		t.Fatal(err)
	}
	for _, chain := range chains {
		if chain.Table.Name == "agent-vmm" && chain.Name == "forward" && chain.Policy != nil && *chain.Policy == nftables.ChainPolicyDrop {
			return
		}
	}
	t.Fatal("reconciled policy has no default-drop forward chain")
}

func TestDNSListenerFailureRetiresExactServerAndRestarts(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires Guest root to bind the DNS port")
	}
	for _, transport := range []string{"udp", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			manager := &Manager{namespaces: map[string]Namespace{"dns": {ID: "dns", Gateway: netip.MustParseAddr("127.0.0.2")}}, endpoints: map[string]map[string]Endpoint{"dns": {"worker": {}}}, dns: make(map[string]*dnsServer), logger: slog.Default().With("component", "guest-network")}
			manager.publishDNSLocked()
			manager.mu.Lock()
			err := manager.startDNSLocked("dns")
			old := manager.dns["dns"]
			manager.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { manager.Close() })
			queryDNS := func(network string) {
				t.Helper()
				query, err := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 17}, Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("missing.dns.agent."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}).Pack()
				if err != nil {
					t.Fatal(err)
				}
				connection, err := net.DialTimeout(network, "127.0.0.2:53", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				connection.SetDeadline(time.Now().Add(time.Second))
				if network == "tcp4" {
					query = append([]byte{byte(len(query) >> 8), byte(len(query))}, query...)
				}
				if _, err := connection.Write(query); err != nil {
					t.Fatal(err)
				}
				response := make([]byte, 1024)
				var count int
				if network == "tcp4" {
					var size [2]byte
					if _, err := io.ReadFull(connection, size[:]); err != nil {
						t.Fatal(err)
					}
					count = int(binary.BigEndian.Uint16(size[:]))
					_, err = io.ReadFull(connection, response[:count])
				} else {
					count, err = connection.Read(response)
				}
				if err != nil {
					t.Fatal(err)
				}
				var answer dnsmessage.Message
				if err := answer.Unpack(response[:count]); err != nil || answer.RCode != dnsmessage.RCodeNameError {
					t.Fatalf("expected NXDOMAIN: %v %v", answer, err)
				}
			}
			queryDNS("udp4")
			queryDNS("tcp4")
			manager.mu.Lock()
			answered := make(chan struct{})
			go func() { queryDNS("udp4"); close(answered) }()
			select {
			case <-answered:
				// DNS reads the published view while mutations hold the lock.
			case <-time.After(time.Second):
				manager.mu.Unlock()
				t.Fatal("local DNS blocked on the manager write lock")
			}
			manager.mu.Unlock()
			select {
			case <-answered:
			case <-time.After(time.Second):
				t.Fatal("local DNS did not resume after projection lock release")
			}
			if transport == "udp" {
				old.udp.Close()
			} else {
				old.tcp.Close()
			}
			deadline := time.Now().Add(time.Second)
			for {
				manager.mu.RLock()
				retired := manager.dns["dns"] != nil && manager.dns["dns"] != old
				manager.mu.RUnlock()
				if retired {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("dead listener remains registered")
				}
				time.Sleep(time.Millisecond)
			}
			manager.mu.Lock()
			err = manager.startDNSLocked("dns")
			replacement := manager.dns["dns"]
			manager.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			manager.retireDNS("dns", old)
			manager.mu.RLock()
			current := manager.dns["dns"]
			manager.mu.RUnlock()
			if replacement == old || current != replacement {
				t.Fatal("old worker retired replacement")
			}
			queryDNS("udp4")
			queryDNS("tcp4")
		})
	}
}
