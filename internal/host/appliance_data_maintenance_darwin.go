//go:build darwin

package host

import (
	"context"
)

func (controller *Controller) withApplianceDataOfflineExcept(ctx context.Context, excludedInstallationID string, mutate func() error) error {
	return controller.vmLifecycle.WithApplianceDataOffline(ctx, excludedInstallationID, mutate)
}
