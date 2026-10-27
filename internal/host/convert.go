package host

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
)

func containerConfigurationObservation(container *vmmdv1.Container, expected *hostv1.CreateContainerRequest) *hostv1.Container {
	result := guestconn.Container(container)
	if expected != nil && result != nil {
		matches := container.GetConfigurationFingerprint() == containerConfigurationFingerprint(expected)
		result.ConfigurationMatches = &matches
	}
	return result
}

func optional[T any](value T) *T { return &value }
