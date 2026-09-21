//go:build darwin

package host

import (
	"context"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Controller) allocationSessionIDs(registrationID, allocationID string, generation uint64) []string {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	ids := make([]string, 0)
	for id, session := range controller.sessions {
		if session.registrationID == registrationID && session.allocationID == allocationID && session.allocationGeneration == generation {
			ids = append(ids, id)
		}
	}
	return ids
}

func (controller *Controller) closeAllocationRuntime(ctx context.Context, registrationID, allocationID string, generation uint64) error {
	controller.hostFrontends.closeAllocation(registrationID, allocationID, generation, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	var failures []error
	for _, id := range controller.allocationSessionIDs(registrationID, allocationID, generation) {
		if err := controller.revokeSession(ctx, id); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// invalidateRemoteController revokes only the failed controller's live command
// paths. The durable allocations and their Guest workloads remain owned by the
// allocation owner and continue independently.
func (controller *Controller) invalidateRemoteController(ctx context.Context, registrationID string) error {
	controller.hostFrontends.closeRegistration(registrationID, "", hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	controller.mu.Lock()
	ids := make([]string, 0)
	for id, session := range controller.sessions {
		if session.registrationID == registrationID {
			ids = append(ids, id)
		}
	}
	controller.mu.Unlock()
	var failures []error
	for _, id := range ids {
		if err := controller.revokeSession(ctx, id); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (controller *Controller) retireRemoteRegistration(ctx context.Context, registrationID string) error {
	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
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
			unlockEnvironment := controller.lockEnvironment(environment.ID)
			current, loadErr := controller.store.GetEnvironment(ctx, environment.ID)
			if loadErr == nil {
				loadErr = controller.closeAllocationRuntime(ctx, registrationID, current.AllocationID, current.AllocationGeneration)
			}
			if loadErr == nil {
				loadErr = controller.stopRemoteWorkload(ctx, current.ID)
			}
			if loadErr == nil && current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
				_, loadErr = controller.store.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
			}
			unlockEnvironment()
			if loadErr != nil {
				failures = append(failures, loadErr)
			}
		}
	}
	controller.wakeAllocationRetirement()
	return errors.Join(failures...)
}

func (controller *Controller) disconnectRemoteRegistration(registrationID, instance string) {
	controller.hostFrontends.closeRegistration(registrationID, instance, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
}

func (controller *Controller) stopRemoteWorkload(ctx context.Context, environmentID string) error {
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
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return err
	}
	timeout := registration.StopGraceSeconds
	for _, container := range containers.GetContainers() {
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_CREATED || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			continue
		}
		_, stopErr := controller.stopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID("remote-stop-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TimeoutSeconds: &timeout})
		if stopErr != nil {
			if _, killErr := client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: derivedRequestID("remote-kill-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), Signal: vmmdv1.ProcessSignal_PROCESS_SIGNAL_KILL}); killErr != nil {
				return errors.Join(stopErr, killErr)
			}
		}
	}
	return nil
}
