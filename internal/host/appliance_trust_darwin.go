//go:build darwin

package host

import (
	"context"
	"errors"
)

func (controller *Controller) revokeApplianceTrustAuthority(ctx context.Context, installationID string) error {
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	unlock := controller.lockIntegration("service-domain:" + installation.RegistrationID)
	defer unlock()
	imports, err := controller.store.ListApplianceRouteImports(ctx, installationID)
	if err != nil {
		return err
	}
	if err := controller.store.RevokeApplianceTrustAuthority(ctx, installationID); err != nil {
		return err
	}
	var result error
	for _, importID := range imports {
		if err := controller.deleteServiceProjection(ctx, "revoke-appliance-trust:"+importID, importID); err != nil {
			result = errors.Join(result, err)
		}
		controller.egress.DeleteServiceRoute(importID)
	}
	return result
}
