package lifecycle

import "context"

type serviceOwner struct {
	drainVM func(context.Context, paths, string) error
}

func (value paths) serviceOwner() serviceOwner {
	if value.owner != nil {
		return *value.owner
	}
	return serviceOwner{drainVM: stopVMForUpdate}
}
