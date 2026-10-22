//go:build darwin || linux

package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Controller) reconcileHostShares(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	values, err := controller.store.ListHostShares(ctx, "")
	if err != nil || len(values) == 0 {
		return err
	}
	var failures []error
	for _, value := range values {
		err := func() error {
			unlock := controller.lockEnvironment(value.EnvironmentID)
			defer unlock()
			controller.hostShareMu.Lock()
			defer controller.hostShareMu.Unlock()
			current, err := controller.store.GetHostShare(ctx, value.ID)
			if err != nil {
				return err
			}
			return controller.reconcileHostShareLocked(ctx, client, current)
		}()
		if err != nil {
			failures = append(failures, fmt.Errorf("host share %s: %w", value.ID, err))
		}
	}
	controller.hostShareMu.Lock()
	defer controller.hostShareMu.Unlock()
	return errors.Join(errors.Join(failures...), controller.publishHostSharesLocked(ctx, client))
}

func (controller *Controller) reconcileHostShareLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, value store.HostShare) error {
	if client == nil {
		if value.OperationID != "" {
			operation, err := controller.store.GetOperation(ctx, value.OperationID)
			if err != nil {
				return err
			}
			if operation.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || operation.State == int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) {
				var candidate store.HostShare
				if err := json.Unmarshal(operation.Receipt, &candidate); err != nil {
					return err
				}
				approved, err := controller.store.HostShareEnvironmentApproved(ctx, candidate.EnvironmentID)
				if err != nil {
					return err
				}
				if approved || candidate.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
					_, err = controller.finishHostShareOperationLocked(ctx, operation, candidate)
					return err
				}
			}
		}
		return nil
	}
	approved, err := controller.store.HostShareEnvironmentApproved(ctx, value.EnvironmentID)
	if err != nil {
		return err
	}
	selected, present := controller.hostShares[value.ID]
	if value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED && !present {
		return nil
	}
	reason := value.UnavailableReason
	if !approved {
		reason = "service environment is no longer approved; stop the VM before revoking the host share"
	} else if present && selected.Revision == value.Revision && selected.Source == value.Source && value.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING && value.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		source, sourceErr := reopenHostShareSource(value.Source)
		if sourceErr == nil {
			source.Close()
			return nil
		}
		reason = sourceErr.Error() + "; stop the VM, repair the source, and start the VM"
	} else if reason == "" {
		reason = "host share was not configured for this boot; stop the VM to configure it"
	}
	if present {
		delete(controller.hostShares, value.ID)
		controller.hostSharesDirty = true
	}
	if value.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		if value.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING {
			value.State = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE
		}
		value.UnavailableReason = reason
		if err := controller.store.UpdateHostShare(ctx, value, value.Revision); err != nil {
			return err
		}
	}
	var projectionErr error
	if present {
		projectionErr = controller.publishHostSharesLocked(ctx, client)
	}
	return errors.Join(projectionErr, controller.stopHostShareConsumersLocked(ctx, client, value))
}

// Environment removal already holds its lifecycle and inventory mutation locks.
func (controller *Controller) revokeEnvironmentHostSharesLocked(ctx context.Context, environment string) error {
	controller.hostShareMu.Lock()
	defer controller.hostShareMu.Unlock()
	approved, err := controller.store.HostShareEnvironmentApproved(ctx, environment)
	if err != nil || approved {
		return err
	}
	values, err := controller.store.ListHostShares(ctx, environment)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
			if offlineErr := controller.requireHostShareOffline(); offlineErr != nil {
				client, connection, err := controller.guestClient(ctx)
				if err != nil {
					return errors.Join(offlineErr, err)
				}
				defer connection.Close()
				var failures []error
				for _, value := range values {
					if err := controller.reconcileHostShareLocked(ctx, client, value); err != nil {
						failures = append(failures, err)
					}
				}
				return errors.Join(offlineErr, errors.Join(failures...), controller.publishHostSharesLocked(ctx, client))
			}
			break
		}
	}
	revoked, err := controller.store.CompleteEnvironmentHostShareRevocation(ctx, environment)
	if err != nil {
		return err
	}
	for _, id := range revoked {
		delete(controller.hostShares, id)
	}
	if len(revoked) > 0 {
		controller.hostSharesDirty = true
	}
	return nil
}

func (controller *Controller) settleEnvironmentHostShares(ctx context.Context, environment string) error {
	release, err := controller.lockInventoryMutation(ctx)
	if err != nil {
		return err
	}
	defer release()
	unlock := controller.lockEnvironment(environment)
	defer unlock()
	return controller.revokeEnvironmentHostSharesLocked(ctx, environment)
}
