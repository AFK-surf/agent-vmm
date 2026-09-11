//go:build darwin && e2e

package host

import (
	"net/netip"
	"os"

	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func importArchiveAddressAllowed(address netip.Addr) bool {
	return egress.DestinationAllowed(address, nil, false) || os.Getenv("AGENT_VMM_E2E_ARCHIVE_CA") != "" && address.IsLoopback()
}
