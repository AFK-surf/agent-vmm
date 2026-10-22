//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Controller) refreshApplianceTrust(ctx context.Context, client vmmdv1.VMMDServiceClient, snapshot store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, now time.Time) (string, error) {
	unlock := controller.lockIntegration("appliance-lifecycle:" + snapshot.ID)
	defer unlock()
	installation, err := controller.store.GetApplianceInstallation(ctx, snapshot.ID)
	if err != nil {
		return "", err
	}
	if installation.Revision != snapshot.Revision || !installation.DesiredEnabled ||
		installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) ||
		installation.CurrentBootID != identity.GetBootId() || installation.CurrentContainerInstance != identity.GetContainerInstanceId() {
		return "", nil
	}
	credential, err := controller.store.GetApplianceMembershipCredential(ctx, installation.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "MANAGED_TRUST_UNAVAILABLE", err
	}
	expiry := credential.GetExpiresAt().AsTime()
	if expiry.Sub(now) > 24*time.Hour {
		return "", nil
	}
	status := "MANAGED_TRUST_RENEWAL_PENDING"
	if !expiry.After(now) {
		status = "MANAGED_TRUST_EXPIRED"
	}
	controller.applianceHealthMu.Lock()
	retryAt := controller.applianceTrustRetry[installation.ID]
	controller.applianceHealthMu.Unlock()
	if now.Before(retryAt) {
		return status, nil
	}
	controller.applianceHealthMu.Lock()
	if controller.applianceTrustRetry == nil {
		controller.applianceTrustRetry = make(map[string]time.Time)
	}
	controller.applianceTrustRetry[installation.ID] = now.Add(time.Hour)
	controller.applianceHealthMu.Unlock()
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return status, err
	}
	installation.BootstrapID = ""
	_, err = controller.bootstrapAppliance(ctx, client, installation, grant, identity, installation.Revision)

	if err != nil {
		return status, errors.Join(errors.New("appliance managed trust renewal failed"), err)
	}
	updated, err := controller.store.GetApplianceMembershipCredential(ctx, installation.ID)
	if err != nil {
		return status, err
	}
	if updated.GetExpiresAt().AsTime().Sub(now) <= 24*time.Hour {
		return status, nil
	}
	controller.applianceHealthMu.Lock()
	delete(controller.applianceTrustRetry, installation.ID)
	controller.applianceHealthMu.Unlock()
	return "", nil
}

func (controller *Controller) revokeApplianceTrustAuthority(ctx context.Context, installationID string) error {
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	lock := controller.integrationLock("service-domain:" + installation.RegistrationID)
	for !lock.TryLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return err
		}
	}
	defer lock.Unlock()
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
