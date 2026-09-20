//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type remoteLease struct {
	id                   string
	registrationID       string
	allocationID         string
	environmentID        string
	workloadRef          string
	generation           uint64
	allocationGeneration uint64
	expiresAt            time.Time
	state                hostv1.LeaseState
	sessionID            string
}

func leaseToProto(lease *remoteLease) *hostv1.RemoteLease {
	if lease == nil {
		return nil
	}
	return &hostv1.RemoteLease{LeaseId: lease.id, AllocationId: lease.allocationID, WorkloadRef: lease.workloadRef, Generation: lease.generation, ExpiresAt: timestamppb.New(lease.expiresAt), State: lease.state}
}

func remoteLeaseActive(lease *remoteLease) bool {
	return lease != nil && lease.state == hostv1.LeaseState_LEASE_STATE_ACTIVE
}

func remoteLeaseRenewable(lease *remoteLease, now time.Time) bool {
	return remoteLeaseActive(lease) && lease.expiresAt.After(now)
}

func (controller *Controller) finishLease(id string, generation uint64, state hostv1.LeaseState, stop, retire, reconcileTerminal bool) (*remoteLease, error) {
	controller.mu.Lock()
	lease := controller.remoteLeases[id]
	if lease == nil || lease.generation != generation || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		controller.mu.Unlock()
		return nil, nil
	}
	environmentID := lease.environmentID
	controller.mu.Unlock()
	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
	unlockEnvironment := controller.lockEnvironment(environmentID)
	defer unlockEnvironment()
	return controller.finishLeaseLocked(id, generation, state, stop, retire, reconcileTerminal)
}

// finishLeaseLocked is the single exact lease transition. Callers hold
// remoteAdmissionMu and the environment lock, in that order, so short-lived
// lease revocation cannot race explicit allocation retirement.
func (controller *Controller) finishLeaseLocked(id string, generation uint64, state hostv1.LeaseState, stop, retire, reconcileTerminal bool) (*remoteLease, error) {
	return controller.finishLeaseLockedContext(context.Background(), id, generation, state, stop, retire, reconcileTerminal)
}

func (controller *Controller) finishLeaseLockedContext(ctx context.Context, id string, generation uint64, state hostv1.LeaseState, stop, retire, reconcileTerminal bool) (*remoteLease, error) {
	controller.mu.Lock()
	lease := controller.remoteLeases[id]
	if lease == nil || lease.generation != generation || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		controller.mu.Unlock()
		return nil, nil
	}
	result := *lease
	lease.state = state
	result.state = state
	delete(controller.remoteLeases, id)
	delete(controller.allocationLeases, lease.environmentID)
	controller.mu.Unlock()
	// Remove the capability before any guest or database work. A failed stop
	// must never leave a usable frontend/session behind.
	controller.hostFrontends.closeAllocationLease(id, generation, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	if result.sessionID != "" {
		_ = controller.revokeSession(ctx, result.sessionID)
	}
	var cleanupErr error
	if stop {
		stopContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		cleanupErr = controller.stopRemoteWorkload(stopContext, result.environmentID)
		cancel()
	}
	environment, err := controller.store.GetEnvironment(ctx, result.environmentID)
	if err != nil {
		log.Printf("finish remote lease %q: load allocation: %v", id, err)
		controller.retryRemoteLeaseProjection(result, targetStateForLease(retire), reconcileTerminal, 1)
		return &result, errors.Join(cleanupErr, err)
	}
	targetState := int32(hostv1.AllocationState_ALLOCATION_STATE_READY)
	if retire {
		targetState = int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)
	}
	projected, err := controller.store.FinishRemoteLeaseProjection(ctx, result.registrationID, id, result.environmentID, environment.PolicyRevision, targetState)
	if err != nil {
		log.Printf("finish remote lease %q: commit projection: %v", id, err)
		controller.retryRemoteLeaseProjection(result, targetState, reconcileTerminal, 1)
		return &result, errors.Join(cleanupErr, err)
	}
	if reconcileTerminal {
		if err := controller.reconcileEnvironment(ctx, projected); err != nil {
			log.Printf("finish remote lease %q: reconcile exact retirement: %v", id, err)
			controller.retryRemoteLeaseProjection(result, targetState, true, 1)
		}
	}
	if cleanupErr != nil {
		log.Printf("finish remote lease %q: workload stop remains unresolved in the node observation owner: %v", id, cleanupErr)
	}
	return &result, nil
}

func targetStateForLease(retire bool) int32 {
	if retire {
		return int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)
	}
	return int32(hostv1.AllocationState_ALLOCATION_STATE_READY)
}

func (controller *Controller) retryRemoteLeaseProjection(lease remoteLease, targetState int32, reconcileTerminal bool, attempt int) {
	if attempt > 4 {
		return
	}
	time.AfterFunc(time.Duration(attempt)*time.Second, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		controller.remoteAdmissionMu.Lock()
		defer controller.remoteAdmissionMu.Unlock()
		unlockEnvironment := controller.lockEnvironment(lease.environmentID)
		defer unlockEnvironment()
		environment, err := controller.store.GetEnvironment(ctx, lease.environmentID)
		if err == nil {
			if environment.AllocationState == targetState {
				if reconcileTerminal {
					err = controller.reconcileEnvironment(ctx, environment)
				}
			} else {
				environment, err = controller.store.FinishRemoteLeaseProjection(ctx, lease.registrationID, lease.id, lease.environmentID, environment.PolicyRevision, targetState)
				if err == nil && reconcileTerminal {
					err = controller.reconcileEnvironment(ctx, environment)
				}
			}
		}
		if err != nil {
			controller.retryRemoteLeaseProjection(lease, targetState, reconcileTerminal, attempt+1)
		}
	})
}

// discardUncommittedLease rolls back the in-memory claim only when the atomic
// AcquireRemoteLeaseProjection transaction failed, so no durable lease owner
// exists. Durable terminal paths must use finishLeaseLocked.
func (controller *Controller) discardUncommittedLease(id string, generation uint64, state hostv1.LeaseState) *remoteLease {
	controller.mu.Lock()
	lease := controller.remoteLeases[id]
	if lease == nil || lease.generation != generation || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		controller.mu.Unlock()
		return nil
	}
	lease.state = state
	delete(controller.remoteLeases, id)
	delete(controller.allocationLeases, lease.environmentID)
	result := *lease
	controller.mu.Unlock()
	controller.hostFrontends.closeAllocationLease(id, generation, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	if result.sessionID != "" {
		_ = controller.revokeSession(context.Background(), result.sessionID)
	}
	return &result
}

func (controller *Controller) revokeAllocationLease(ctx context.Context, environment store.Environment, state hostv1.LeaseState, stop bool) error {
	controller.mu.Lock()
	id := controller.allocationLeases[environment.ID]
	lease := controller.remoteLeases[id]
	controller.mu.Unlock()
	if lease != nil {
		finished, err := controller.finishLeaseLocked(lease.id, lease.generation, state, stop, true, false)
		if err != nil {
			return err
		}
		if finished == nil {
			return errors.New("allocation lease terminal claim was lost")
		}
	} else if stop {
		_ = controller.stopRemoteWorkload(ctx, environment.ID)
	}
	return nil
}

func (controller *Controller) revokeRemoteLeases(registrationID string) error {
	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
	return controller.revokeRemoteLeasesLocked(registrationID, false)
}

func (controller *Controller) retireRemoteRegistration(ctx context.Context, registrationID string) error {
	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
	if err := controller.revokeRemoteLeasesLocked(registrationID, true); err != nil {
		return err
	}
	environments, err := controller.store.ListRemoteEnvironmentsBounded(ctx, registrationID, int(maxRemoteEnvironments)+1)
	if err != nil {
		return err
	}
	if len(environments) > int(maxRemoteEnvironments) {
		return errors.New("remote registration allocation inventory exceeds the bounded pool")
	}
	for _, environment := range environments {
		unlockEnvironment := controller.lockEnvironment(environment.ID)
		current, loadErr := controller.store.GetEnvironment(ctx, environment.ID)
		if loadErr == nil && current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
			_, loadErr = controller.store.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
		}
		unlockEnvironment()
		if loadErr != nil {
			return loadErr
		}
	}
	controller.wakeAllocationRetirement()
	return nil
}

func (controller *Controller) revokeRemoteLeasesLocked(registrationID string, retire bool) error {
	controller.mu.Lock()
	leases := make([]remoteLease, 0, len(controller.remoteLeases))
	for _, lease := range controller.remoteLeases {
		if lease.registrationID == registrationID {
			leases = append(leases, *lease)
		}
	}
	controller.mu.Unlock()
	var failures []error
	for _, lease := range leases {
		unlockEnvironment := controller.lockEnvironment(lease.environmentID)
		finished, err := controller.finishLeaseLocked(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED, true, retire, retire)
		unlockEnvironment()
		if finished == nil && err == nil {
			continue
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("revoke remote lease %q: %w", lease.id, err))
		}
	}
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
