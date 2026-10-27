//go:build darwin || linux

package remote

import (
	"context"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

const (
	allocationRetirementBudget = 30 * time.Second
	maxRevocationRetryDelay    = time.Minute
)

// revocationResume belongs to allocationRetirementLoop. It records whether the
// interrupted revocations were scanned and when each failed one is retried.
type revocationResume struct {
	scanned bool
	retries map[string]revocationRetry
}

type revocationRetry struct {
	delay time.Duration
	at    time.Time
}

func (controller *Owner) wakeAllocationRetirement() {
	select {
	case controller.allocationRetirementWake <- struct{}{}:
	default:
	}
}

func (controller *Owner) AllocationRetirementLoop(ctx context.Context) {
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	controller.wakeAllocationRetirement()
	var resume revocationResume
	retirements := make(map[string]revocationRetry)
	for {
		select {
		case <-ctx.Done():
			return
		case <-controller.allocationRetirementWake:
		case <-retry.C:
		}
		if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			continue
		}
		controller.resumeRemoteRevocations(ctx, &resume)
		if err := controller.reconcileAllocationRetirements(ctx, retirements); err != nil {
			controller.logger.Warn("allocation retirement requires attention", "error", err)
		}
	}
}

// resumeRemoteRevocations stops the workloads of registrations whose
// revocation was interrupted, for example by a Host crash. Startup recovery
// leaves those workloads to this owner. After admission opens it scans once,
// then retries only the failed registrations with a bounded backoff.
func (controller *Owner) resumeRemoteRevocations(ctx context.Context, resume *revocationResume) {
	if !controller.lifecycle.AdmitsRemoteWork() {
		return
	}
	if !resume.scanned {
		retries, err := controller.scanRemoteRevocations(ctx)
		if err != nil {
			controller.logger.Warn("interrupted remote revocation scan requires attention", "error", err)
			return
		}
		resume.scanned, resume.retries = true, retries
		return
	}
	for id, retry := range resume.retries {
		if time.Now().Before(retry.at) {
			continue
		}
		if err := controller.retireInterruptedRevocation(ctx, id); err != nil {
			retry.delay = min(2*retry.delay, maxRevocationRetryDelay)
			retry.at = time.Now().Add(retry.delay)
			resume.retries[id] = retry
			continue
		}
		delete(resume.retries, id)
	}
}

func (controller *Owner) scanRemoteRevocations(ctx context.Context) (map[string]revocationRetry, error) {
	retries := make(map[string]revocationRetry)
	for cursor := ""; ; {
		registrations, err := controller.store.ListRemoteRegistrationPage(ctx, cursor, 32)
		if err != nil {
			return nil, err
		}
		if len(registrations) == 0 {
			return retries, nil
		}
		for _, registration := range registrations {
			cursor = registration.ID
			if registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING) ||
				registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
				continue
			}
			if err := controller.retireInterruptedRevocation(ctx, registration.ID); err != nil {
				retries[registration.ID] = revocationRetry{delay: time.Second, at: time.Now().Add(time.Second)}
			}
		}
	}
}

func (controller *Owner) retireInterruptedRevocation(ctx context.Context, registrationID string) error {
	ctx, cancel := context.WithTimeout(ctx, allocationRetirementBudget)
	defer cancel()
	err := controller.RetireRegistration(ctx, registrationID)
	if err != nil {
		controller.logger.Warn("interrupted remote revocation requires attention", "registration_id", registrationID, "error", err)
	}
	return err
}

func (controller *Owner) reconcileAllocationRetirements(ctx context.Context, retries map[string]revocationRetry) error {
	seen := make(map[string]bool)
	var result error
	for cursor := ""; ; {
		environments, err := controller.store.ListRetiringAllocations(ctx, cursor, 32)
		if err != nil {
			return errors.Join(result, err)
		}
		if len(environments) == 0 {
			break
		}
		for _, environment := range environments {
			cursor = environment.ID
			seen[environment.ID] = true
			retry := retries[environment.ID]
			if time.Now().Before(retry.at) {
				continue
			}
			if err := controller.retireEmptyAllocation(ctx, environment); err != nil {
				retry.delay = min(max(time.Second, 2*retry.delay), maxRevocationRetryDelay)
				retry.at = time.Now().Add(retry.delay)
				retries[environment.ID] = retry
				result = errors.Join(result, err)
			} else {
				delete(retries, environment.ID)
			}
		}
	}
	for id := range retries {
		if !seen[id] {
			delete(retries, id)
		}
	}
	return result
}

func (controller *Owner) retireEmptyAllocation(ctx context.Context, candidate store.Environment) error {
	ctx, cancel := context.WithTimeout(ctx, allocationRetirementBudget)
	defer cancel()
	ctx, releaseInventory, err := controller.inventory.Enter(ctx)
	if err != nil {
		return err
	}
	defer releaseInventory()
	unlock, lockErr := controller.workloads.LockEnvironment(ctx, candidate.ID)
	if lockErr != nil {
		return lockErr
	}

	defer unlock()
	environment, err := controller.store.GetEnvironment(ctx, candidate.ID)
	if err != nil {
		return err
	}
	if environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return nil
	}
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	if err := controller.retireEmptyAllocationWithClient(ctx, client, environment); err != nil {
		return err
	}
	return controller.finalizeAllocationRetirement(ctx, environment)
}

func (controller *Owner) retireEmptyAllocationWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment) error {
	_, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: opreceipt.DerivedRequestID("retire-" + environment.NamespaceID), NamespaceId: environment.NamespaceID})
	return err
}

func (controller *Owner) finalizeAllocationRetirement(ctx context.Context, environment store.Environment) error {
	if err := controller.services.ProjectionLock().Lock(ctx); err != nil {
		return err
	}

	defer controller.services.ProjectionLock().Unlock()
	if err := controller.services.PublishSourcePoliciesLocked(ctx, environment.ID); err != nil {
		return err
	}
	if err := controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return err
	}
	controller.workloads.ForgetContainers(environment.ID)
	return nil
}
