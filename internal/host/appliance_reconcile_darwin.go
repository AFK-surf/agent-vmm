//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"log"
	"slices"
	"time"
)

func (controller *Controller) applianceReconcileLoop(ctx context.Context) {
	const fallbackDelay = 5 * time.Second
	delay := fallbackDelay
	cursor := ""
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			removeErr := controller.recoverPendingApplianceRemovals(ctx)
			if removeErr != nil {
				log.Printf("reconcile pending appliance removals: %v", removeErr)
			}
			if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
				delay = fallbackDelay
				timer.Reset(delay)
				continue
			}
			next, err := controller.reconcileObservedAppliancePage(ctx, cursor)
			err = errors.Join(removeErr, err)
			cursor = next
			if err != nil {
				log.Printf("reconcile observed appliances: %v", err)
				delay = nextApplianceReconcileDelay(delay)
			} else {
				delay = fallbackDelay
			}
			timer.Reset(delay)
		}
	}
}

func nextApplianceReconcileDelay(current time.Duration) time.Duration {
	next := current * 2
	if next < 5*time.Second {
		return 5 * time.Second
	}
	if next > time.Minute {
		return time.Minute
	}
	return next
}

func (controller *Controller) reconcileObservedAppliances(ctx context.Context) error {
	return controller.reconcileObservedAppliancesExcept(ctx, "", false)
}

func (controller *Controller) reconcileObservedAppliancePage(ctx context.Context, after string) (string, error) {
	installations, err := controller.store.ListApplianceInstallationPage(ctx, after, 32)
	if err != nil {
		return after, err
	}
	if len(installations) == 0 {
		if after == "" {
			return "", nil
		}
		installations, err = controller.store.ListApplianceInstallationPage(ctx, "", 32)
		if err != nil || len(installations) == 0 {
			return "", err
		}
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return after, err
	}
	defer connection.Close()
	err = controller.reconcileObservedApplianceCandidates(ctx, client, installations, "", false)
	return installations[len(installations)-1].ID, err
}

func (controller *Controller) reconcileObservedAppliancesExcept(ctx context.Context, excludedInstallationID string, requireReady bool) error {
	// Model anchor: spec/tla/ApplianceRecoverySelection.tla CaptureHost/ObserveGuest.
	// Read Host candidates before Guest observations. A newer Host revision must
	// never be paired with an older missing-instance observation.
	after := ""
	var result error
	for {
		installations, err := controller.store.ListApplianceInstallationPage(ctx, after, 32)
		if err != nil {
			return errors.Join(result, err)
		}
		if len(installations) == 0 {
			return result
		}
		client, connection, err := controller.guestClient(ctx)
		if err != nil {
			return errors.Join(result, err)
		}
		reconcileErr := controller.reconcileObservedApplianceCandidates(ctx, client, installations, excludedInstallationID, requireReady)
		connection.Close()
		result = errors.Join(result, reconcileErr)
		after = installations[len(installations)-1].ID
	}
}

func (controller *Controller) reconcileObservedApplianceCandidates(ctx context.Context, client vmmdv1.VMMDServiceClient, installations []store.ApplianceInstallation, excludedInstallationID string, requireReady bool) error {
	if controller.applianceSecrets == nil {
		return errors.New("appliance protected store is unavailable")
	}
	retentionInstallations := slices.Clone(installations)
	candidates := make(map[string]store.ApplianceInstallation, len(installations))
	filtered := installations[:0]
	for _, installation := range installations {
		pending, err := controller.store.HasPendingApplianceUpdate(ctx, installation.ID)
		if err != nil {
			return err
		}
		if pending {
			continue
		}
		candidates[installation.ID] = installation
		filtered = append(filtered, installation)
	}
	installations = filtered
	var result error
	retentionFailed := make(map[string]bool)
	for _, installation := range retentionInstallations {
		if installation.ID == excludedInstallationID {
			continue
		}
		if err := controller.reconcileSystemImageRetention(ctx, client, installation.ID); err != nil {
			result = errors.Join(result, fmt.Errorf("retain appliance images %s: %w", installation.ID, err))
			retentionFailed[installation.ID] = true
		}
	}

	active := make(map[string]bool, len(installations))
	for _, installation := range installations {
		running, err := controller.store.HasRunningApplianceOperation(ctx, installation.ID)
		if err != nil {
			return err
		}
		active[installation.ID] = running
	}
	observed := make(map[string]bool, len(installations))
	runtimes := make([]*vmmdv1.SystemAppliance, 0, len(installations))
	for _, installation := range installations {
		if installation.ID == excludedInstallationID {
			continue
		}
		runtime, err := getSystemAppliance(ctx, client, installation.ID)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("observe appliance %s: %w", installation.ID, err))
			continue
		}
		if runtime != nil {
			observed[installation.ID] = true
			runtimes = append(runtimes, runtime)
		}
	}
	for _, runtime := range runtimes {
		installationID := runtime.GetIdentity().GetInstallationId()
		if installationID == "" || installationID == excludedInstallationID {
			continue
		}
		installation, exists := candidates[installationID]
		if !exists {
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
					continue
				}
				if runtime.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED && !active[installation.ID] && applianceRecoveryState(installation.State) && !retentionFailed[installation.ID] {
					if err := controller.recoverApplianceRuntime(ctx, client, installation, runtime); err != nil {
						result = errors.Join(result, fmt.Errorf("recover appliance %s: %w", installation.ID, err))
					}
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
		if retentionFailed[installation.ID] {
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
			trustStatus, trustErr := controller.refreshApplianceTrust(ctx, client, installation, identity, time.Now())
			if trustErr != nil {
				result = errors.Join(result, fmt.Errorf("refresh appliance trust %s: %w", installation.ID, trustErr))
			}
			health := &appliancev1.ApplianceHealth{InstallationId: installation.ID, BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(), ConfigRevision: runtime.GetConfigRevision(), Healthy: true, Status: runtime.GetStatus()}
			if trustStatus != "" {
				health.Status = trustStatus
			}
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
	for _, installation := range installations {
		if installation.ID == excludedInstallationID {
			continue
		}
		if !installation.DesiredEnabled && installation.RegistrationID != "" && !active[installation.ID] {
			if err := controller.reconcileDisabledAppliance(ctx, installation); err != nil {
				result = errors.Join(result, fmt.Errorf("retry revoked appliance %s: %w", installation.ID, err))
			}
			continue
		}
		if installation.DesiredEnabled && installation.RegistrationID != "" && !active[installation.ID] && !observed[installation.ID] && applianceRecoveryState(installation.State) && !retentionFailed[installation.ID] {
			if err := controller.recoverApplianceRuntime(ctx, client, installation, nil); err != nil {
				result = errors.Join(result, fmt.Errorf("recover unobserved appliance %s: %w", installation.ID, err))
			}
		}
	}
	if requireReady {
		bootID := controller.manager.Snapshot().GetBootId()
		for _, candidate := range installations {
			if candidate.ID == excludedInstallationID || !candidate.DesiredEnabled || candidate.RegistrationID == "" {
				continue
			}
			current, err := controller.store.GetApplianceInstallation(ctx, candidate.ID)
			if err != nil {
				result = errors.Join(result, err)
				continue
			}
			if !current.DesiredEnabled {
				continue
			}
			health := controller.applianceHealthProjection(current.ID)
			if current.ArtifactID != candidate.ArtifactID || current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) || current.CurrentBootID != bootID || current.CurrentContainerInstance == "" || !health.GetHealthy() || health.GetBootId() != bootID || health.GetContainerInstance() != current.CurrentContainerInstance {
				result = errors.Join(result, fmt.Errorf("appliance %s recovery has not published the current instance", current.ID))
			}
		}
	}

	return result
}
