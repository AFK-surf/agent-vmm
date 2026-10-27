//go:build darwin || linux

package remote

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Owner) closeAllocationRuntime(ctx context.Context, registrationID, allocationID string, generation uint64) error {
	var failures []error
	for _, id := range controller.sessions.AllocationSessionIDs(registrationID, allocationID, generation) {
		if err := controller.sessions.Revoke(ctx, id); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// RetireRegistration stops the workloads of an explicitly revoked registration.
// It retains allocation identity and private data for later cleanup.
func (controller *Owner) RetireRegistration(ctx context.Context, registrationID string) error {
	ctx, release, err := controller.inventory.Enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	var failures []error
	for cursor := ""; ; {
		environments, err := controller.store.ListRemoteEnvironmentPage(ctx, registrationID, cursor, 32)
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if len(environments) == 0 {
			break
		}
		for _, environment := range environments {
			cursor = environment.AllocationID
			if err := controller.retireRemoteEnvironment(ctx, environment); err != nil {
				failures = append(failures, err)
			}
		}
	}
	controller.wakeAllocationRetirement()
	return errors.Join(failures...)
}

func (controller *Owner) retireRemoteEnvironment(ctx context.Context, environment store.Environment) error {
	unlockEnvironment, lockErr := controller.workloads.LockEnvironment(ctx, environment.ID)
	if lockErr != nil {
		return lockErr
	}

	defer unlockEnvironment()
	current, err := controller.store.GetEnvironment(ctx, environment.ID)
	// Empty allocation retirement can finish after the page was read.
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = controller.RetainEnvironmentLocked(ctx, current, false)
	return err
}

func (controller *Owner) RetainEnvironmentLocked(ctx context.Context, current store.Environment, namespaceMissing bool) (store.Environment, error) {
	if err := controller.closeAllocationRuntime(ctx, current.RegistrationID, current.AllocationID, current.AllocationGeneration); err != nil {
		return current, err
	}
	if !namespaceMissing {
		if err := controller.stopRemoteWorkload(ctx, current.ID); err != nil {
			return current, err
		}
	}
	if current.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return current, nil
	}
	return controller.store.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
}

func (controller *Owner) stopRemoteWorkload(ctx context.Context, environmentID string) error {
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err != nil || environment.OwnerKind != store.OwnerRemoteRegistration {
		return err
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, environment.RegistrationID)
	if err != nil {
		return err
	}
	started := time.Now()
	defer func() { controller.recordDomainStopLatency(registration.ID, time.Since(started)) }()
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	return controller.stopRemoteNamespace(ctx, client, environment, registration.StopGraceSeconds)
}

func (controller *Owner) stopRemoteNamespace(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment, grace uint32) error {
	for cursor := ""; ; {
		page, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, PageSize: vmmdv1.MaxContainerPageSize, AfterContainerId: cursor})
		if err != nil {
			missing, confirmErr := cleanup.ConfirmMissingNamespaceDiscard(ctx, client, environment.NamespaceID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, err)
			if confirmErr != nil {
				return confirmErr
			}
			if missing {
				return nil
			}
			return err
		}
		if len(page.GetContainers()) > vmmdv1.MaxContainerPageSize {
			return errors.New("container retirement page exceeds its budget")
		}
		for _, container := range page.GetContainers() {
			if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_CREATED || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
				continue
			}
			stopped, err := controller.workloads.StopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: opreceipt.DerivedRequestID("remote-stop-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TimeoutSeconds: &grace})
			if err != nil {
				return err
			}
			if stopped.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && stopped.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
				return errors.New("container retirement stop is not confirmed")
			}
		}
		next := page.GetNextCursor()
		if next == "" {
			return nil
		}
		if next <= cursor || len(next) > 200 {
			return errors.New("container retirement cursor did not advance")
		}
		cursor = next
	}
}
