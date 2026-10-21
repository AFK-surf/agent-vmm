//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"log"
)

func applianceRecoveryState(state int32) bool {
	return state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
}

func (controller *Controller) recoverApplianceRuntime(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance) error {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	if current.Revision != installation.Revision || current.State != installation.State || !current.DesiredEnabled || current.RegistrationID == "" {
		return nil
	}

	if runtime != nil {
		if err := controller.stopStartedAppliance(ctx, client, runtime.GetIdentity()); err != nil {
			controller.setObservedApplianceFailure(installation, runtime, "INSTANCE_STOP_INCOMPLETE")
			return err
		}
	}
	prepared, recover, err := controller.prepareApplianceRecoveryLocked(ctx, installation)
	if err != nil || !recover {
		return err
	}
	return controller.startApplianceLocked(ctx, client, prepared)
}

func (controller *Controller) prepareApplianceRecoveryLocked(ctx context.Context, installation store.ApplianceInstallation) (store.ApplianceInstallation, bool, error) {
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return store.ApplianceInstallation{}, false, err
	}
	if current.Revision != installation.Revision || current.State != installation.State || !current.DesiredEnabled || current.RegistrationID == "" || !applianceRecoveryState(current.State) {
		return current, false, nil
	}
	controller.stopApplianceFrontends(current.ID)
	controller.clearApplianceHealth(current.ID)
	controller.stopPrivateControllerRoute(current.ID)
	if err := controller.invalidateRemoteController(ctx, current.RegistrationID); err != nil {
		return store.ApplianceInstallation{}, false, err
	}
	if current.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) && current.CurrentBootID == "" && current.CurrentContainerInstance == "" {
		return current, true, nil
	}
	current.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	current.CurrentBootID = ""
	current.CurrentContainerInstance = ""
	updated, err := controller.store.UpdateApplianceInstallation(ctx, current, current.Revision)
	return updated, err == nil, err
}

func (controller *Controller) reconcileDisabledAppliance(ctx context.Context, installation store.ApplianceInstallation) error {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	if current.Revision != installation.Revision || current.DesiredEnabled || current.State != installation.State {
		return nil
	}
	revoked, err := controller.store.HasApplianceRevocation(ctx, current.ID)
	if err != nil {
		return err
	}
	if !revoked {
		return controller.stopAppliance(ctx, current.ID)
	}
	domainErr := controller.revokePrivateControllerDomain(ctx, installation.ID)
	trustErr := controller.revokeApplianceTrustAuthority(ctx, installation.ID)
	if errors.Is(trustErr, sql.ErrNoRows) {
		trustErr = nil
	}
	stopErr := controller.stopAppliance(ctx, installation.ID)
	return errors.Join(trustErr, domainErr, stopErr)
}

func (controller *Controller) invalidateApplianceRuntime(ctx context.Context, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance, reason string) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installation.ID)
	controller.stopApplianceFrontends(installation.ID)
	controller.clearApplianceHealth(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	if installation.RegistrationID != "" {
		if err := controller.invalidateRemoteController(ctx, installation.RegistrationID); err != nil {
			return err
		}
	}
	if runtime != nil {
		controller.setObservedApplianceFailure(installation, runtime, reason)
	} else {
		controller.setApplianceFailure(installation, reason)
	}
	return nil
}

func (controller *Controller) adoptObservedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance) error {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	if current.Revision != installation.Revision || current.State != installation.State || !current.DesiredEnabled {
		return nil
	}
	installation = current
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return err
	}
	controller.stopApplianceFrontends(installation.ID)
	controller.clearApplianceHealth(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	if installation.RegistrationID != "" {
		if err := controller.invalidateRemoteController(ctx, installation.RegistrationID); err != nil {
			return err
		}
	}
	candidate := installation
	candidate.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
	candidate.CurrentBootID = runtime.GetIdentity().GetBootId()
	candidate.CurrentContainerInstance = runtime.GetIdentity().GetContainerInstanceId()
	configRevision := installation.Revision + 1
	persisted, err := controller.store.UpdateApplianceInstallation(ctx, candidate, installation.Revision)
	if err != nil {
		return err
	}
	candidate = persisted
	health, err := controller.bootstrapAppliance(ctx, client, candidate, grant, runtime.GetIdentity(), configRevision)
	if err != nil {
		if errors.Is(err, errBootstrapStoreIntegrity) || (candidate.BootstrapID != "" && bootstrapFailureReason(err) == "BOOTSTRAP_STATE_UNAVAILABLE") {
			return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity())
		}
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	bootstrapID := candidate.BootstrapID
	if bootstrapID != "" {
		if err := controller.applianceSecrets.DeleteBootstrap(ctx, candidate.ID, bootstrapID); err != nil {
			controller.setPendingApplianceHealth(health)
			controller.revokeFailedAppliancePublication(ctx, installation.ID)
			return fmt.Errorf("delete consumed bootstrap envelope %s/%s: %w", candidate.ID, bootstrapID, err)
		}
	}
	candidate, err = controller.store.GetApplianceInstallation(ctx, candidate.ID)
	if err != nil {
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	if err := controller.publishApplianceInstance(ctx, candidate, runtime.GetIdentity(), health); err != nil {
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	candidate.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	candidate.BootstrapID = ""
	if _, err := controller.store.UpdateApplianceInstallation(ctx, candidate, candidate.Revision); err != nil {
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	controller.clearPendingApplianceHealth(candidate.ID)
	return controller.refreshApplianceServiceImports(ctx, candidate)
}

func (controller *Controller) retryPendingApplianceCleanup(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance, health *appliancev1.ApplianceHealth) error {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	if current.Revision != installation.Revision || current.State != installation.State || !current.DesiredEnabled {
		return nil
	}
	installation = current
	candidate, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	bootstrapID := candidate.BootstrapID
	configRevision := runtime.GetConfigRevision()
	if configRevision == 0 {
		configRevision = candidate.Revision
	}
	if health == nil {
		grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
		if err != nil {
			return err
		}
		health, err = controller.bootstrapAppliance(ctx, client, candidate, grant, runtime.GetIdentity(), configRevision)
		if err != nil {
			if errors.Is(err, errBootstrapStoreIntegrity) || (bootstrapID != "" && bootstrapFailureReason(err) == "BOOTSTRAP_STATE_UNAVAILABLE") {
				return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity())
			}
			// A replay proves that the guest durably consumed the initial
			// envelope before the host crashed. Only then is deleting the seed
			// safe; ordinary non-replay failures must preserve it for retry.
			if bootstrapID == "" || bootstrapFailureReason(err) != "BOOTSTRAP_REPLAY_REJECTED" {
				return err
			}
			if err := controller.applianceSecrets.DeleteBootstrap(ctx, installation.ID, bootstrapID); err != nil {
				return err
			}
			candidate.BootstrapID = ""
			health, err = controller.bootstrapAppliance(ctx, client, candidate, grant, runtime.GetIdentity(), configRevision)
			if err != nil {
				if errors.Is(err, errBootstrapStoreIntegrity) || (bootstrapID != "" && bootstrapFailureReason(err) == "BOOTSTRAP_STATE_UNAVAILABLE") {
					return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity())
				}
				return err
			}
		} else if bootstrapID != "" {
			if err := controller.applianceSecrets.DeleteBootstrap(ctx, installation.ID, bootstrapID); err != nil {
				return err
			}
		}
	} else if bootstrapID != "" {
		// The in-memory health is the receipt from the previous successful
		// bootstrap attempt, so the protected seed is consumed already.
		if err := controller.applianceSecrets.DeleteBootstrap(ctx, installation.ID, bootstrapID); err != nil {
			return err
		}
	}
	candidate.BootstrapID = ""
	if err := controller.publishApplianceInstance(ctx, candidate, runtime.GetIdentity(), health); err != nil {
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	candidate.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	candidate.BootstrapID = ""
	if _, err := controller.store.UpdateApplianceInstallation(ctx, candidate, candidate.Revision); err != nil {
		controller.revokeFailedAppliancePublication(ctx, installation.ID)
		return err
	}
	controller.clearPendingApplianceHealth(installation.ID)
	return controller.refreshApplianceServiceImports(ctx, candidate)
}

func (controller *Controller) revokeFailedAppliancePublication(ctx context.Context, installationID string) {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installationID)
	controller.stopApplianceFrontends(installationID)
	controller.clearApplianceHealth(installationID)
	controller.stopPrivateControllerRoute(installationID)
	if installation, err := controller.store.GetApplianceInstallation(ctx, installationID); err == nil && installation.RegistrationID != "" {
		if err := controller.invalidateRemoteController(ctx, installation.RegistrationID); err != nil {
			log.Printf("invalidate failed-publication controller %s: %v", installationID, err)
		}
	}
	// Managed trust is durable installation authority. A failed publication
	// fences only this instance; explicit Revoke retires the authority.
}

func (controller *Controller) setObservedApplianceFailure(installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance, reason string) {
	identity := runtime.GetIdentity()
	controller.setApplianceHealth(&appliancev1.ApplianceHealth{InstallationId: installation.ID,
		BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(),
		ConfigRevision: runtime.GetConfigRevision(), Healthy: false, Status: reason})
}
