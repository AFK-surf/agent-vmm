package egress

import (
	"context"
	"fmt"
	"gvisor.dev/gvisor/pkg/tcpip"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestPoliciesFailClosed(t *testing.T) {
	policies := new(Policies)
	snapshot := Snapshot{BootID: "boot", Revision: 1, Sources: []Source{{EnvironmentID: "env", NamespaceID: "ns", Subnet: netip.MustParsePrefix("10.64.1.0/24"), Mode: PublicInternet}}}
	if err := policies.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, boot, address string
		allowed             bool
	}{
		{"known", "boot", "10.64.1.8", true},
		{"stale boot", "old", "10.64.1.8", false},
		{"unknown", "boot", "10.64.2.8", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, allowed := policies.Resolve(test.boot, netip.MustParseAddr(test.address))
			if allowed != test.allowed {
				t.Fatalf("Resolve() allowed = %t", allowed)
			}
		})
	}
	if err := policies.Publish(snapshot); err == nil {
		t.Fatal("Publish accepted stale revision")
	}
	snapshot.Revision = 2
	snapshot.Sources = append(snapshot.Sources, Source{EnvironmentID: "other", NamespaceID: "other", Subnet: netip.MustParsePrefix("10.64.1.0/24"), Mode: PublicInternet})
	if err := policies.Publish(snapshot); err == nil {
		t.Fatal("Publish accepted overlapping source")
	}
}

func TestServiceRouteRequiresExactEnvironmentSourceAndVirtualDestination(t *testing.T) {
	subsystem := New()
	source := netip.MustParseAddr("10.64.1.8")
	destination := netip.MustParseAddrPort("198.19.2.9:8443")
	if err := subsystem.ApplyServiceRoute(ServiceRoute{ID: "route", Generation: 1, EnvironmentID: "env", SourceAddress: source,
		Destination: destination, ExpiresAt: time.Now().Add(time.Minute), Dial: func(context.Context) (net.Conn, error) { return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	if _, ok := subsystem.serviceRoute("env", source, destination.Addr(), destination.Port(), time.Now()); !ok {
		t.Fatal("exact service route was unavailable")
	}
	for _, candidate := range []struct {
		environment string
		source      netip.Addr
		destination netip.AddrPort
	}{
		{"other", source, destination},
		{"env", netip.MustParseAddr("10.64.1.9"), destination},
		{"env", source, netip.MustParseAddrPort("198.19.2.10:8443")},
	} {
		if _, ok := subsystem.serviceRoute(candidate.environment, candidate.source, candidate.destination.Addr(), candidate.destination.Port(), time.Now()); ok {
			t.Fatal("forged service route coordinates were accepted")
		}
	}
	subsystem.DeleteServiceRoute("route")
	if _, ok := subsystem.serviceRoute("env", source, destination.Addr(), destination.Port(), time.Now()); ok {
		t.Fatal("deleted service route remained active")
	}
}

func TestDestinationPolicy(t *testing.T) {
	for _, test := range []struct {
		address          string
		allowTransparent bool
		want             bool
	}{
		{"1.1.1.1", false, true}, {"127.0.0.1", false, false}, {"169.254.169.254", false, false},
		{"10.64.1.8", false, false}, {"198.18.0.1", false, false}, {"198.18.0.1", true, true},
	} {
		if got := DestinationAllowed(netip.MustParseAddr(test.address), []netip.Prefix{netip.MustParsePrefix("10.64.1.0/24")}, test.allowTransparent); got != test.want {
			t.Errorf("DestinationAllowed(%s) = %t", test.address, got)
		}
	}
}

func TestInfrastructureLease(t *testing.T) {
	leases := new(Leases)
	lease, err := leases.Acquire(time.Now().Add(time.Minute))
	if err != nil || !leases.Active(time.Now()) {
		t.Fatalf("Acquire() = %#v, %v", lease, err)
	}
	lease.Release()
	if leases.Active(time.Now()) {
		t.Fatal("released lease remains active")
	}
	if _, err := leases.Acquire(time.Now().Add(-time.Second)); err == nil {
		t.Fatal("Acquire accepted expired deadline")
	}
}

func TestInfrastructureLeaseLimit(t *testing.T) {
	leases := new(Leases)
	active := make([]*Lease, 0, MaxInfrastructureLeases)
	for range MaxInfrastructureLeases {
		lease, err := leases.Acquire(time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		active = append(active, lease)
	}
	if _, err := leases.Acquire(time.Now().Add(time.Minute)); err == nil {
		t.Fatal("Acquire exceeded infrastructure lease limit")
	}
	for _, lease := range active {
		lease.Release()
	}
}

func TestInfrastructureLeaseContextEndsOnRelease(t *testing.T) {
	leases := new(Leases)
	lease, err := leases.Acquire(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := leases.Context(context.Background())
	defer cancel()
	lease.Release()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("lease context remained active after release")
	}
}

func TestInfrastructureLeaseContextDoesNotExtendAcrossOverlappingLease(t *testing.T) {
	var leases Leases
	first, err := leases.Acquire(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second, err := leases.Acquire(time.Now().Add(2 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	ctx, cancel := leases.Context(context.Background())
	defer cancel()
	first.Release()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("flow context outlived the lease that authorized it")
	}
	if !leases.Active(time.Now()) {
		t.Fatal("overlapping lease was unexpectedly released")
	}
}

func TestFlowLimitsAndRelease(t *testing.T) {
	for _, test := range []struct {
		name           string
		kind           resourceKind
		environment    string
		infrastructure bool
		limit          int
	}{
		{"environment TCP", tcpResource, "environment", false, MaxTCPPerEnvironment},
		{"environment UDP", udpResource, "environment", false, MaxUDPPerEnvironment},
		{"environment DNS", dnsResource, "environment", false, MaxDNSPerEnvironment},
		{"infrastructure TCP", tcpResource, "", true, MaxInfrastructureTCP},
		{"infrastructure UDP", udpResource, "", true, MaxInfrastructureUDP},
		{"infrastructure DNS", dnsResource, "", true, MaxInfrastructureDNS},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := counters{environment: make(map[string][3]int)}
			for range test.limit {
				if !values.acquire(test.kind, test.environment, test.infrastructure) {
					t.Fatal("acquire rejected below limit")
				}
			}
			if values.acquire(test.kind, test.environment, test.infrastructure) {
				t.Fatal("acquire exceeded limit")
			}
			for range test.limit {
				values.release(test.kind, test.environment, test.infrastructure)
			}
			if !values.empty() {
				t.Fatal("resource counters did not return to zero")
			}
		})
	}
}

func TestGlobalFlowLimits(t *testing.T) {
	for _, test := range []struct {
		name  string
		kind  resourceKind
		limit int
	}{{"TCP", tcpResource, MaxTCP}, {"UDP", udpResource, MaxUDP}, {"DNS", dnsResource, MaxDNS}} {
		t.Run(test.name, func(t *testing.T) {
			values := counters{environment: make(map[string][3]int)}
			for index := range test.limit {
				if !values.acquire(test.kind, fmt.Sprint(index), false) {
					t.Fatal("acquire rejected below global limit")
				}
			}
			if values.acquire(test.kind, "overflow", false) {
				t.Fatal("acquire exceeded global limit")
			}
		})
	}
}

func TestLANGrantKeepsInfrastructureAndNamespaceBoundaries(t *testing.T) {
	subnets := []netip.Prefix{netip.MustParsePrefix("10.64.1.0/24")}
	hosts := []netip.Addr{netip.MustParseAddr("192.168.1.2")}
	for _, test := range []struct {
		address string
		allowed bool
	}{
		{"192.168.1.10", true}, {"10.1.1.1", true}, {"172.20.1.1", true},
		{"192.168.1.2", false}, {"10.64.1.8", false}, {"10.127.255.8", false},
		{"169.254.169.254", false}, {"127.0.0.1", false}, {"100.100.100.200", false},
		{"1.1.1.1", false}, {"fc00::1", false}, {"::ffff:192.168.1.10", false},
	} {
		if got := LANDestinationAllowed(netip.MustParseAddr(test.address), subnets, hosts); got != test.allowed {
			t.Errorf("LAN destination %s = %t", test.address, got)
		}
	}
}

func TestInstallationRevocationCancelsExistingFlowOnly(t *testing.T) {
	policies := new(Policies)
	source := Source{EnvironmentID: "appliance:a", NamespaceID: "system.a", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true, InstanceID: "instance", GrantRevision: 1, HostTCPEndpoints: []netip.AddrPort{netip.MustParseAddrPort("192.168.1.10:3000")}}
	other := Source{EnvironmentID: "workload", NamespaceID: "workload", Subnet: netip.MustParsePrefix("10.64.2.0/24"), Mode: PublicInternet}
	if err := policies.Publish(Snapshot{BootID: "boot", Revision: 1, Sources: []Source{source, other}}); err != nil {
		t.Fatal(err)
	}
	flow, cancel := policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), source)
	defer cancel()
	unrelated, cancelOther := policies.Context(context.Background(), "boot", other.Subnet.Addr().Next(), other)
	defer cancelOther()
	added := Source{EnvironmentID: "new-allocation", NamespaceID: "new-namespace", Subnet: netip.MustParsePrefix("10.64.3.0/24"), Mode: PublicInternet}
	for revision := uint64(2); revision <= 3; revision++ {
		if err := policies.Publish(Snapshot{BootID: "boot", Revision: revision, Sources: []Source{source, other, added}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-flow.Done():
			t.Fatal("unchanged controller flow ended after another allocation or session projection")
		case <-time.After(25 * time.Millisecond):
		}
	}
	policies.RevokeEnvironment("appliance:a")
	select {
	case <-flow.Done():
	case <-time.After(time.Second):
		t.Fatal("existing installation flow survived revocation")
	}
	if _, allowed := policies.Resolve("boot", source.Subnet.Addr().Next()); allowed {
		t.Fatal("revoked source admitted a new flow")
	}
	select {
	case <-unrelated.Done():
		t.Fatal("unrelated workload was revoked")
	default:
	}
	policies.Clear()
	select {
	case <-unrelated.Done():
	case <-time.After(time.Second):
		t.Fatal("existing flow survived boot policy removal")
	}
}

func TestSourceInstanceAndBootReplacementCancelOriginalFlow(t *testing.T) {
	for _, replacement := range []string{"instance", "boot"} {
		t.Run(replacement, func(t *testing.T) {
			policies := new(Policies)
			source := Source{EnvironmentID: "appliance", NamespaceID: "system.appliance", Subnet: netip.MustParsePrefix("10.64.1.0/24"), Mode: PublicInternet, InstanceID: "original", GrantRevision: 1}
			if err := policies.Publish(Snapshot{BootID: "boot", Revision: 1, Sources: []Source{source}}); err != nil {
				t.Fatal(err)
			}
			flow, cancel := policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), source)
			defer cancel()
			boot := "boot"
			if replacement == "instance" {
				source.InstanceID = "replacement"
			} else {
				boot = "replacement"
			}
			if err := policies.Publish(Snapshot{BootID: boot, Revision: 2, Sources: []Source{source}}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-flow.Done():
			case <-time.After(time.Second):
				t.Fatal("original flow survived source instance or boot replacement")
			}
		})
	}
}

func TestFlowContextRejectsRevocationBetweenAuthorizationAndAttachment(t *testing.T) {
	subsystem := New()
	source := Source{EnvironmentID: "appliance:a", NamespaceID: "system.a", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true, InstanceID: "instance", GrantRevision: 1}
	address := source.Subnet.Addr().Next()
	if err := subsystem.Policies.Publish(Snapshot{BootID: "boot", Revision: 1, Sources: []Source{source}}); err != nil {
		t.Fatal(err)
	}
	_, _, allowed, authorizedSource := subsystem.authorizeEndpoint("boot", tcpip.AddrFromSlice(address.AsSlice()), tcpip.AddrFrom4([4]byte{192, 168, 1, 10}), false, 80, true, false)
	if !allowed || !authorizedSource.Equal(source) {
		t.Fatalf("LAN authorization = %t, %+v", allowed, authorizedSource)
	}
	subsystem.Policies.RevokeEnvironment(source.EnvironmentID)
	flow, cancel := subsystem.Policies.Context(context.Background(), "boot", address, authorizedSource)
	defer cancel()
	if flow.Err() != context.Canceled {
		t.Fatal("revoked authorization acquired a live flow context")
	}
}

func TestHostTCPEndpointsKeepProtocolPortAndInfrastructureFences(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var host netip.Addr
	for _, value := range addresses {
		prefix, err := netip.ParsePrefix(value.String())
		if err == nil && prefix.Addr().Is4() && !prefix.Addr().IsLoopback() && !prefix.Addr().IsLinkLocalUnicast() && !namespacePool.Contains(prefix.Addr()) {
			host = prefix.Addr()
			break
		}
	}
	if !host.IsValid() {
		t.Skip("no non-loopback Host IPv4 interface")
	}
	for _, mode := range []Mode{DenyAll, PublicInternet} {
		for _, lan := range []bool{false, true} {
			subsystem := New()
			source := Source{EnvironmentID: "appliance:a", NamespaceID: "system.a", Subnet: netip.MustParsePrefix("10.64.1.0/24"), Mode: mode, LANEgress: lan, InstanceID: "instance", GrantRevision: 1}
			sourceAddress := tcpip.AddrFromSlice(source.Subnet.Addr().Next().AsSlice())
			destination := tcpip.AddrFromSlice(host.AsSlice())
			publish := func(revision uint64) {
				t.Helper()
				if err := subsystem.Policies.Publish(Snapshot{BootID: "boot", Revision: revision, Sources: []Source{source}}); err != nil {
					t.Fatal(err)
				}
			}
			check := func(port uint16, tcp, dns, want bool) {
				t.Helper()
				_, _, got, _ := subsystem.authorizeEndpoint("boot", sourceAddress, destination, false, port, tcp, dns)
				if got != want {
					t.Fatalf("mode=%v LAN=%v port=%v TCP=%v DNS=%v got=%v want=%v", mode, lan, port, tcp, dns, got, want)
				}
			}
			publish(1)
			check(3000, true, false, false)
			source.HostTCPEndpoints = []netip.AddrPort{netip.AddrPortFrom(host, 3000)}
			publish(2)
			check(3000, true, false, true)
			check(3001, true, false, false)
			check(3000, false, false, false)
			check(0, false, true, true)
			source.HostTCPEndpoints[0] = netip.AddrPortFrom(host, 3001)
			check(3000, true, false, true)
			source.HostTCPEndpoints[0] = netip.AddrPortFrom(host, 3000)
			subsystem.HostTCPInfrastructure = func(target netip.AddrPort) bool { return target.Port() == 3000 }
			publish(3)
			check(3000, true, false, false)
			check(0, false, true, false)
			subsystem.HostTCPInfrastructure = nil
			publish(4)
			subsystem.HostTCPInfrastructure = func(target netip.AddrPort) bool { return target.Port() == 8080 }
			check(0, false, true, true)
			subsystem.HostTCPInfrastructure = func(target netip.AddrPort) bool { return target.Port() == 3000 }
			check(3000, true, false, false)
			subsystem.HostTCPInfrastructure = nil
			_, _, _, authorized := subsystem.authorizeEndpoint("boot", sourceAddress, destination, false, 3000, true, false)
			flow, cancel := subsystem.Policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), authorized)
			defer cancel()
			source.HostTCPEndpoints = nil
			source.GrantRevision++
			publish(5)
			select {
			case <-flow.Done():
			case <-time.After(time.Second):
				t.Fatal("grant removal kept existing flow")
			}
			check(3000, true, false, false)
		}
	}
}

func TestServiceRouteFlowLifetime(t *testing.T) {
	for _, change := range []string{"delete", "replace", "republish", "expire"} {
		t.Run(change, func(t *testing.T) {
			subsystem := New()
			route := ServiceRoute{ID: "route", Generation: 1, EnvironmentID: "env", SourceAddress: netip.MustParseAddr("10.64.1.8"),
				Destination: netip.MustParseAddrPort("198.19.2.9:80"), ExpiresAt: time.Now().Add(time.Minute), Dial: func(context.Context) (net.Conn, error) { return nil, nil }}
			if change == "expire" {
				route.ExpiresAt = time.Now().Add(30 * time.Millisecond)
			}
			if err := subsystem.ApplyServiceRoute(route); err != nil {
				t.Fatal(err)
			}
			selected, ok := subsystem.serviceRoute("env", route.SourceAddress, route.Destination.Addr(), route.Destination.Port(), time.Now())
			if !ok {
				t.Fatal("route missing")
			}
			flow, cancel := subsystem.serviceRouteContext(context.Background(), selected)
			defer cancel()
			switch change {
			case "delete":
				subsystem.DeleteServiceRoute(route.ID)
			case "replace":
				route.Generation++
				if err := subsystem.ApplyServiceRoute(route); err != nil {
					t.Fatal(err)
				}
			case "republish":
				if err := subsystem.ApplyServiceRoute(route); err != nil {
					t.Fatal(err)
				}
				if flow.Err() != nil {
					t.Fatal("same route publication cancelled current flow")
				}
				return
			}
			select {
			case <-flow.Done():
			case <-time.After(time.Second):
				t.Fatal("revoked route retained its flow")
			}
			stale, stop := subsystem.serviceRouteContext(context.Background(), selected)
			defer stop()
			select {
			case <-stale.Done():
			case <-time.After(time.Second):
				t.Fatal("selected stale route admitted a new flow")
			}
		})
	}
}
