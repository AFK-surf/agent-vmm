package egress

import (
	"context"
	"fmt"
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
	if err := subsystem.ApplyServiceRoute(ServiceRoute{ID: "route", EnvironmentID: "env", SourceAddress: source,
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
