//go:build darwin

package host

import (
	"context"
	"errors"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (controller *Controller) withApplianceDataOfflineExcept(ctx context.Context, excludedInstallationID string, mutate func() error) error {
	unlock := controller.lockIntegration("appliance-data-maintenance")
	defer unlock()
	controller.remoteAdmissionMu.Lock()
	controller.projectionMu.Lock()

	wasRunning := controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING
	if wasRunning {
		controller.stopEgress()
		controller.invalidateRuntimeCapabilities(ctx)
		if _, err := controller.manager.Stop(ctx, false); err != nil {
			controller.projectionMu.Unlock()
			controller.remoteAdmissionMu.Unlock()
			return err
		}
	}
	mutationErr := mutate()
	controller.projectionMu.Unlock()
	controller.remoteAdmissionMu.Unlock()
	if !wasRunning {
		return mutationErr
	}
	restartErr := controller.configureApplianceBlocks(ctx)
	if restartErr == nil {
		_, restartErr = controller.manager.Start(ctx)
	}
	if restartErr == nil {
		restartErr = controller.startEgress()
	}
	if restartErr == nil {
		restartErr = controller.reconcileAppliancesExcept(ctx, excludedInstallationID)
	}
	if restartErr == nil {
		restartErr = controller.reconcilePolicies(ctx)
	}
	return errors.Join(mutationErr, restartErr)
}
