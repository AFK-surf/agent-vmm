//go:build darwin

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

const workloadReleaseBatchSize = 8
const workloadReleaseTimeout = 2 * time.Second

func (controller *Controller) wakeWorkloadReleaseReconcile() {
	select {
	case controller.workloadReleaseWake <- struct{}{}:
	default:
	}
}

func (controller *Controller) workloadReleaseReconcileLoop(ctx context.Context) {
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	controller.wakeWorkloadReleaseReconcile()
	for {
		select {
		case <-ctx.Done():
			return
		case <-controller.workloadReleaseWake:
		case <-retry.C:
		}
		if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			continue
		}
		more, err := controller.reconcileWorkloadReleaseBatch(ctx, workloadReleaseBatchSize)
		if err != nil {
			log.Printf("reconcile workload releases: %v", err)
		}
		if more && err == nil {
			controller.wakeWorkloadReleaseReconcile()
		}
	}
}

func (controller *Controller) reconcileWorkloadReleaseBatch(ctx context.Context, limit int) (bool, error) {
	reservations, err := controller.store.ListWorkloadReleaseObligations(ctx, limit)
	if err != nil {
		return false, err
	}
	var result error
	for _, candidate := range reservations {
		if err := controller.reconcileWorkloadRelease(ctx, candidate); err != nil {
			result = errors.Join(result, err)
			if deferErr := controller.store.DeferWorkloadRelease(ctx, candidate.AllocationID, candidate.Generation); deferErr != nil {
				result = errors.Join(result, deferErr)
			}
		}
	}
	return len(reservations) == limit, result
}

func (controller *Controller) reconcileWorkloadRelease(ctx context.Context, candidate store.WorkloadReservation) error {
	ctx, cancel := context.WithTimeout(ctx, workloadReleaseTimeout)
	defer cancel()
	unlock := controller.lockEnvironment(candidate.EnvironmentID)
	defer unlock()
	reservation, err := controller.store.GetWorkloadReservation(ctx, candidate.AllocationID, candidate.Generation)
	if err != nil {
		return err
	}
	environment, err := controller.store.GetEnvironment(ctx, reservation.EnvironmentID)
	if err != nil {
		return err
	}
	if environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return errors.New("workload release obligation does not belong to a retained allocation")
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := controller.reconcileWorkloadReleaseWithClient(ctx, client, environment, reservation); err != nil {
		return err
	}
	return controller.finalizeWorkloadRelease(ctx, environment)
}

func (controller *Controller) reconcileWorkloadReleaseWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment, reservation store.WorkloadReservation) error {
	var err error
	if reservation.Phase == store.WorkloadReservationReleaseRequired {
		reservation, err = controller.recoverGuestReservationForRelease(ctx, client, environment, reservation)
		if err != nil {
			return err
		}
	}
	if reservation.Phase != store.WorkloadReservationReleasing && reservation.Phase != store.WorkloadReservationReleasedPendingConfirm {
		return errors.New("workload release obligation has an invalid phase")
	}
	if err := controller.confirmWorkloadRelease(ctx, client, environment, reservation); err != nil {
		return err
	}
	return nil
}

func (controller *Controller) finalizeWorkloadRelease(ctx context.Context, environment store.Environment) error {
	// Publish the new source authority before deleting the durable obligation.
	// A transient publication failure therefore leaves phase 5 available for
	// the same reconciler to retry without a second dirty-state owner.
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	publish := controller.publishSourcePoliciesLocked
	if controller.sourcePolicyPublisher != nil {
		publish = controller.sourcePolicyPublisher
	}
	if err := publish(ctx, environment.ID); err != nil {
		return err
	}
	if err := controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return err
	}
	controller.forgetContainers(environment.ID)
	return nil
}
