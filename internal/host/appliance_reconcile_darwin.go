//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Controller) applianceReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
				continue
			}
			err := controller.reconcileObservedAppliances(ctx)
			if err != nil {
				log.Printf("reconcile observed appliances: %v", err)
			}
		}
	}
}

func (controller *Controller) reconcileObservedAppliances(ctx context.Context) error {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	response, err := client.ListSystemAppliances(ctx, &vmmdv1.ListSystemAppliancesRequest{})
	if err != nil {
		return err
	}
	runningOperations, err := controller.store.ListRunningApplianceOperations(ctx)
	if err != nil {
		return err
	}
	active := make(map[string]bool, len(runningOperations))
	for _, operation := range runningOperations {
		active[operation.InstallationID] = true
	}
	observed := make(map[string]bool, len(response.GetAppliances()))
	var result error
	for _, runtime := range response.GetAppliances() {
		installationID := runtime.GetIdentity().GetInstallationId()
		if installationID == "" {
			continue
		}
		observed[installationID] = true
		installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				result = errors.Join(result, fmt.Errorf("load appliance %s: %w", installationID, err))
			}
			continue
		}
		// An explicit lifecycle call owns STARTING until its running operation
		// settles. An interrupted or failed operation has no running receipt and
		// is retried through the same replacement path below.
		if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) && active[installation.ID] {
			continue
		}
		identity := runtime.GetIdentity()
		if runtime.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING {
			if installation.DesiredEnabled {
				if err := controller.invalidateApplianceRuntime(ctx, installation, runtime, "INSTANCE_NOT_RUNNING"); err != nil {
					result = errors.Join(result, fmt.Errorf("invalidate appliance %s: %w", installation.ID, err))
				}
			} else if !active[installation.ID] {
				if err := controller.reconcileDisabledAppliance(ctx, installation); err != nil {
					result = errors.Join(result, fmt.Errorf("revoke appliance %s: %w", installation.ID, err))
				}
			}
			continue
		}
		if !installation.DesiredEnabled {
			if !active[installation.ID] {
				if err := controller.reconcileDisabledAppliance(ctx, installation); err != nil {
					result = errors.Join(result, fmt.Errorf("revoke appliance %s: %w", installation.ID, err))
				}
			}
			continue
		}
		if installation.RegistrationID == "" {
			continue
		}
		artifact, err := controller.store.GetApplianceArtifact(ctx, installation.ArtifactID)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("load appliance artifact %s: %w", installation.ArtifactID, err))
			continue
		}
		if identity.GetImageManifestDigest() != artifact.ImageDigest {
			if !active[installation.ID] {
				if err := controller.recoverApplianceRuntime(ctx, client, installation, runtime); err != nil {
					result = errors.Join(result, fmt.Errorf("replace appliance artifact %s: %w", installation.ID, err))
				}
			}
			continue
		}
		identityMatches := installation.CurrentBootID == identity.GetBootId() && installation.CurrentContainerInstance == identity.GetContainerInstanceId()
		if identityMatches && installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) {
			health := &appliancev1.ApplianceHealth{InstallationId: installation.ID, BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(), ConfigRevision: runtime.GetConfigRevision(), Healthy: true, Status: runtime.GetStatus()}
			if err := controller.publishApplianceInstance(ctx, installation, identity, health); err != nil {
				controller.setObservedApplianceFailure(installation, runtime, "INSTANCE_PUBLICATION_FAILED")
				result = errors.Join(result, fmt.Errorf("restore appliance publication %s: %w", installation.ID, err))
			}
			continue
		}
		if identityMatches && installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) {
			if err := controller.retryPendingApplianceCleanup(ctx, client, installation, runtime, controller.pendingApplianceHealthProjection(installation.ID)); err != nil {
				result = errors.Join(result, fmt.Errorf("retry appliance publication %s: %w", installation.ID, err))
			}
			continue
		}
		if err := controller.adoptObservedAppliance(ctx, client, installation, runtime); err != nil {
			controller.setObservedApplianceFailure(installation, runtime, "INSTANCE_RECONCILE_FAILED")
			result = errors.Join(result, fmt.Errorf("adopt appliance %s: %w", installation.ID, err))
		}
	}
	installations, err := controller.store.ListApplianceInstallations(ctx)
	if err != nil {
		return errors.Join(result, err)
	}
	for _, installation := range installations {
		if !installation.DesiredEnabled && installation.RegistrationID != "" && !active[installation.ID] {
			if err := controller.reconcileDisabledAppliance(ctx, installation); err != nil {
				result = errors.Join(result, fmt.Errorf("retry revoked appliance %s: %w", installation.ID, err))
			}
			continue
		}
		if installation.DesiredEnabled && installation.RegistrationID != "" && !active[installation.ID] && !observed[installation.ID] && applianceRecoveryState(installation.State) {
			if err := controller.recoverApplianceRuntime(ctx, client, installation, nil); err != nil {
				result = errors.Join(result, fmt.Errorf("recover unobserved appliance %s: %w", installation.ID, err))
			}
		}
	}
	return result
}

func applianceRecoveryState(state int32) bool {
	return state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
}

func (controller *Controller) recoverApplianceRuntime(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance) error {
	if runtime != nil {
		if err := controller.stopStartedAppliance(ctx, client, runtime.GetIdentity()); err != nil {
			return controller.failAppliance(ctx, installation, "STALE_ARTIFACT_STOP_FAILED", err)
		}
	}
	prepared, recover, err := controller.prepareApplianceRecovery(ctx, installation)
	if err != nil || !recover {
		return err
	}
	return controller.startAppliance(ctx, client, prepared)
}

func (controller *Controller) prepareApplianceRecovery(ctx context.Context, installation store.ApplianceInstallation) (store.ApplianceInstallation, bool, error) {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
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
	if err := controller.revokeRemoteLeases(current.RegistrationID); err != nil {
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
	domainErr := controller.revokePrivateControllerDomain(ctx, installation.ID)
	trustErr := controller.revokeApplianceTrustAuthority(ctx, installation.ID)
	if errors.Is(trustErr, sql.ErrNoRows) {
		trustErr = nil
	}
	stopErr := controller.stopAppliance(ctx, installation.ID)
	return errors.Join(trustErr, domainErr, stopErr)
}

func (controller *Controller) invalidateApplianceRuntime(ctx context.Context, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance, reason string) error {
	controller.stopApplianceFrontends(installation.ID)
	controller.clearApplianceHealth(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	if installation.RegistrationID != "" {
		if err := controller.revokeRemoteLeases(installation.RegistrationID); err != nil {
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
		if err := controller.revokeRemoteLeases(installation.RegistrationID); err != nil {
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
			return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity(), "BOOTSTRAP_SECRET_INTEGRITY_FAILED")
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
	return nil
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
				return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity(), "BOOTSTRAP_SECRET_INTEGRITY_FAILED")
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
					return controller.sealStartedAppliance(ctx, client, candidate, runtime.GetIdentity(), "BOOTSTRAP_SECRET_INTEGRITY_FAILED")
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
	return nil
}

func (controller *Controller) revokeFailedAppliancePublication(ctx context.Context, installationID string) {
	controller.stopApplianceFrontends(installationID)
	controller.clearApplianceHealth(installationID)
	controller.stopPrivateControllerRoute(installationID)
	if installation, err := controller.store.GetApplianceInstallation(ctx, installationID); err == nil && installation.RegistrationID != "" {
		if err := controller.revokeRemoteLeases(installation.RegistrationID); err != nil {
			log.Printf("revoke failed-publication leases %s: %v", installationID, err)
		}
	}
	if err := controller.revokeApplianceTrustAuthority(ctx, installationID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Printf("revoke failed-publication trust %s: %v", installationID, err)
	}
}

func (controller *Controller) setObservedApplianceFailure(installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance, reason string) {
	identity := runtime.GetIdentity()
	controller.setApplianceHealth(&appliancev1.ApplianceHealth{InstallationId: installation.ID,
		BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(),
		ConfigRevision: runtime.GetConfigRevision(), Healthy: false, Status: reason})
}
