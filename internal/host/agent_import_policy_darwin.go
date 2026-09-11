//go:build darwin && !e2e

package host

import (
	"net/netip"

	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func importArchiveAddressAllowed(address netip.Addr) bool {
	return egress.DestinationAllowed(address, nil, false)
}
