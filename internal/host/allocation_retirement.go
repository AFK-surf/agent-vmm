//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"log"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
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

func (controller *Controller) wakeAllocationRetirement() {
	select {
	case controller.allocationRetirementWake <- struct{}{}:
	default:
	}
}

func (controller *Controller) allocationRetirementLoop(ctx context.Context) {
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	controller.wakeAllocationRetirement()
	var resume revocationResume
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
		if err := controller.reconcileAllocationRetirements(ctx); err != nil {
			log.Printf("allocation retirement requires attention: %v", err)
		}
	}
}

// resumeRemoteRevocations stops the workloads of registrations whose
// revocation was interrupted, for example by a Host crash. Startup recovery
// leaves those workloads to this owner. After admission opens it scans once,
// then retries only the failed registrations with a bounded backoff.
func (controller *Controller) resumeRemoteRevocations(ctx context.Context, resume *revocationResume) {
	if !controller.vmLifecycle.admitsRemoteWork() {
		return
	}
	if !resume.scanned {
		retries, err := controller.scanRemoteRevocations(ctx)
		if err != nil {
			log.Printf("interrupted remote revocation scan requires attention: %v", err)
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

func (controller *Controller) scanRemoteRevocations(ctx context.Context) (map[string]revocationRetry, error) {
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

func (controller *Controller) retireInterruptedRevocation(ctx context.Context, registrationID string) error {
	ctx, cancel := context.WithTimeout(ctx, allocationRetirementBudget)
	defer cancel()
	err := controller.retireRemoteRegistration(ctx, registrationID)
	if err != nil {
		log.Printf("interrupted remote revocation requires attention: registration=%s error=%v", registrationID, err)
	}
	return err
}

func (controller *Controller) reconcileAllocationRetirements(ctx context.Context) error {
	environments, err := controller.store.ListRetiringAllocations(ctx, time.Now().Add(-allocationRetirementBudget), 8)
	if err != nil {
		return err
	}
	var result error
	for _, environment := range environments {
		if err := controller.retireEmptyAllocation(ctx, environment); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (controller *Controller) retireEmptyAllocation(ctx context.Context, candidate store.Environment) error {
	ctx, cancel := context.WithDeadline(ctx, candidate.UpdatedAt.Add(allocationRetirementBudget))
	defer cancel()
	releaseInventory, err := controller.lockInventoryMutation(ctx)
	if err != nil {
		return err
	}
	defer releaseInventory()
	unlock := controller.lockEnvironment(candidate.ID)
	defer unlock()
	environment, err := controller.store.GetEnvironment(ctx, candidate.ID)
	if err != nil {
		return err
	}
	if environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return nil
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := controller.retireEmptyAllocationWithClient(ctx, client, environment); err != nil {
		return err
	}
	return controller.finalizeAllocationRetirement(ctx, environment)
}

func (controller *Controller) retireEmptyAllocationWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment) error {
	_, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: derivedRequestID("retire-" + environment.NamespaceID), NamespaceId: environment.NamespaceID})
	return err
}

func (controller *Controller) finalizeAllocationRetirement(ctx context.Context, environment store.Environment) error {
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	if err := controller.publishCleanupSourcePoliciesLocked(ctx, environment.ID); err != nil {
		return err
	}
	if err := controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return err
	}
	controller.forgetContainers(environment.ID)
	return nil
}

func (controller *Controller) publishCleanupSourcePoliciesLocked(ctx context.Context, excludedEnvironmentID string) error {
	publish := controller.publishSourcePoliciesLocked
	if controller.sourcePolicyPublisher != nil {
		publish = controller.sourcePolicyPublisher
	}
	return publish(ctx, excludedEnvironmentID)
}
