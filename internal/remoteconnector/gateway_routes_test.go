package remoteconnector

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestGatewayDNSInterfaceScopes(t *testing.T) {
	configs := []gatewayDNSConfig{
		{name: "wifi", servers: []string{"192.0.2.53", "192.0.2.53", "192.0.2.54"}},
		{name: "vpn", servers: []string{"192.0.2.55"}, restricted: true, domains: []string{"private.example"}},
		{name: "ethernet", servers: []string{"fe80::53"}, restricted: true, domains: []string{"example.test"}},
		{name: "down", servers: []string{"192.0.2.57"}},
		{name: "gone", servers: []string{"192.0.2.58"}},
		{name: "wifi", servers: []string{"malformed", "0.0.0.0", "224.0.0.1"}},
	}
	get := func(name string) (*net.Interface, error) {
		switch name {
		case "wifi":
			return &net.Interface{Index: 4, Flags: net.FlagUp}, nil
		case "ethernet":
			return &net.Interface{Index: 5, Flags: net.FlagUp}, nil
		case "vpn":
			return &net.Interface{Index: 2, Flags: net.FlagUp | net.FlagPointToPoint}, nil
		case "down":
			return &net.Interface{Index: 6}, nil
		default:
			return nil, errors.New("interface removed")
		}
	}
	routes := selectGatewayRoutes("gateway.example.test.", configs, get)
	if len(routes) != 2 || routes[0].index != 4 || routes[1].index != 5 {
		t.Fatalf("routes=%+v", routes)
	}
	if len(routes[0].servers) != 2 || routes[1].servers[0] != "fe80::53%ethernet" {
		t.Fatalf("DNS routes=%+v", routes)
	}
	routes = selectGatewayRoutes("notprivate.example", configs, get)
	if len(routes) != 1 || routes[0].index != 4 {
		t.Fatalf("unrelated domain used scoped DNS: %+v", routes)
	}
	routes = selectGatewayRoutes("host.private.example", configs, get)
	if len(routes) != 2 || routes[1].index != 2 {
		t.Fatalf("matched VPN resolver omitted: %+v", routes)
	}
}

func TestGatewayDiscoveryCancellationDoesNotAccumulateReaders(t *testing.T) {
	slot := make(chan struct{}, 1)
	release := make(chan struct{})
	entered := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		boundedGatewayRoutes(ctx, slot, func() []gatewayRoute { close(entered); <-release; return nil })
		close(done)
	}()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("configd blocked cancellation")
	}
	secondCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	boundedGatewayRoutes(secondCtx, slot, func() []gatewayRoute { t.Error("started another blocked reader"); return nil })
	close(release)
}
