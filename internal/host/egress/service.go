package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"
)

var ServiceAddressPrefix = netip.MustParsePrefix("198.19.0.0/16")

type ServiceRoute struct {
	ID            string
	EnvironmentID string
	SourceAddress netip.Addr
	Destination   netip.AddrPort
	ExpiresAt     time.Time
	Dial          func(context.Context) (net.Conn, error)
}

type serviceRouteKey struct {
	environment string
	source      netip.Addr
	destination netip.AddrPort
}

type serviceRoutes struct {
	mu     sync.RWMutex
	values map[serviceRouteKey]ServiceRoute
}

func (subsystem *Subsystem) ApplyServiceRoute(route ServiceRoute) error {
	if route.ID == "" || route.EnvironmentID == "" || !route.SourceAddress.Is4() ||
		!ServiceAddressPrefix.Contains(route.Destination.Addr()) || route.Destination.Port() == 0 ||
		!route.ExpiresAt.After(time.Now()) || route.Dial == nil {
		return errors.New("complete bounded service route is required")
	}
	key := serviceRouteKey{environment: route.EnvironmentID, source: route.SourceAddress, destination: route.Destination}
	subsystem.services.mu.Lock()
	defer subsystem.services.mu.Unlock()
	for existingKey, existing := range subsystem.services.values {
		if existing.ID == route.ID && existingKey != key {
			return errors.New("service route identity is already bound")
		}
		if existingKey == key && existing.ID != route.ID {
			return errors.New("service route destination is already bound")
		}
	}
	subsystem.services.values[key] = route
	return nil
}

func (subsystem *Subsystem) DeleteServiceRoute(id string) {
	subsystem.services.mu.Lock()
	defer subsystem.services.mu.Unlock()
	for key, route := range subsystem.services.values {
		if route.ID == id {
			delete(subsystem.services.values, key)
		}
	}
}

func (subsystem *Subsystem) serviceRoute(environment string, source, destination netip.Addr, port uint16, now time.Time) (ServiceRoute, bool) {
	key := serviceRouteKey{environment: environment, source: source, destination: netip.AddrPortFrom(destination, port)}
	subsystem.services.mu.RLock()
	route, ok := subsystem.services.values[key]
	subsystem.services.mu.RUnlock()
	return route, ok && route.ExpiresAt.After(now)
}
