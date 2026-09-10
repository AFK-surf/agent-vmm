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

const allocationRetirementBudget = 30 * time.Second

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
		if err := controller.reconcileAllocationRetirements(ctx); err != nil {
			log.Printf("allocation retirement requires attention: %v", err)
		}
	}
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
