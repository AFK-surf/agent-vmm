package lifecycle

import (
	"context"
	"github.com/AFK-surf/agent-vmm/internal/launchdservice"
)

type serviceOwner struct {
	system  launchdservice.System
	observe func(context.Context, string) (hostServiceFacts, error)
	drainVM func(context.Context, paths, string) error
}

func (value paths) serviceOwner() serviceOwner {
	if value.owner != nil {
		return *value.owner
	}
	return serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
}
