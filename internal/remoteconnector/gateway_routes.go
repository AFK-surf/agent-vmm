package remoteconnector

import (
	"context"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"
)

type gatewayDNSConfig struct {
	name       string
	servers    []string
	domains    []string
	restricted bool
}

func selectGatewayRoutes(host string, configs []gatewayDNSConfig, getInterface func(string) (*net.Interface, error)) []gatewayRoute {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	routes := make(map[int]gatewayRoute)
	physical := make(map[int]bool)
	for _, config := range configs {
		if config.restricted {
			matched := false
			for _, domain := range config.domains {
				domain = strings.ToLower(strings.TrimSuffix(domain, "."))
				if domain == "" || host == domain || strings.HasSuffix(host, "."+domain) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		iface, err := getInterface(config.name)
		if err != nil || iface.Index <= 0 || iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		route := routes[iface.Index]
		route.index = iface.Index
		for _, server := range config.servers {
			ip, err := netip.ParseAddr(server)
			if err != nil || ip.IsUnspecified() || ip.IsMulticast() {
				continue
			}
			if ip.Is6() && ip.IsLinkLocalUnicast() && ip.Zone() == "" {
				ip = ip.WithZone(config.name)
			}
			server = ip.String()
			duplicate := false
			for _, existing := range route.servers {
				if existing == server {
					duplicate = true
				}
			}
			if !duplicate && len(route.servers) < 2 {
				route.servers = append(route.servers, server)
			}
		}
		if len(route.servers) != 0 {
			routes[iface.Index] = route
			physical[iface.Index] = iface.Flags&net.FlagPointToPoint == 0
		}
	}
	result := make([]gatewayRoute, 0, len(routes))
	for _, route := range routes {
		result = append(result, route)
	}
	sort.Slice(result, func(i, j int) bool {
		if physical[result[i].index] != physical[result[j].index] {
			return physical[result[i].index]
		}
		return result[i].index < result[j].index
	})
	return result[:min(len(result), gatewayRouteLimit)]
}

// configd IPC has no cancellation API. Permit one in-flight snapshot, so a
// stalled system service cannot accumulate readers across reconnects.
func boundedGatewayRoutes(ctx context.Context, slot chan struct{}, read func() []gatewayRoute) []gatewayRoute {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return nil
	}
	result := make(chan []gatewayRoute, 1)
	go func() { defer func() { <-slot }(); result <- read() }()
	select {
	case routes := <-result:
		return routes
	case <-ctx.Done():
		return nil
	}
}
