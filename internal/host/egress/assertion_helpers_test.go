//go:build darwin || linux

package egress

import (
	"context"
	"net/netip"
)

func (counters *counters) empty() bool {
	counters.mu.Lock()
	defer counters.mu.Unlock()
	return counters.global == [3]int{}
}
func (subsystem *Subsystem) resolveDNS(ctx context.Context, query []byte) ([]byte, error) {
	return subsystem.resolveDNSAllowed(ctx, query, func(address netip.Addr) bool { return DestinationAllowed(address, nil, true) })
}
