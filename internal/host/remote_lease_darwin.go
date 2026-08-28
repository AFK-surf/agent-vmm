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
	id             string
	registrationID string
	allocationID   string
	environmentID  string
	workloadRef    string
	generation     uint64
	expiresAt      time.Time
	state          hostv1.LeaseState
	sessionID      string
}

func leaseToProto(lease *remoteLease) *hostv1.RemoteLease {
	if lease == nil {
		return nil
	}
	return &hostv1.RemoteLease{LeaseId: lease.id, AllocationId: lease.allocationID, WorkloadRef: lease.workloadRef, Generation: lease.generation, ExpiresAt: timestamppb.New(lease.expiresAt), State: lease.state}
}

func (controller *Controller) scheduleLease(id string, generation uint64, deadline time.Time) {
	time.AfterFunc(time.Until(deadline), func() {
		controller.mu.Lock()
		lease := controller.remoteLeases[id]
		if lease == nil || lease.generation != generation || lease.expiresAt.After(deadline) {
			controller.mu.Unlock()
			return
		}
		controller.mu.Unlock()
		_, _ = controller.finishLease(id, generation, hostv1.LeaseState_LEASE_STATE_EXPIRED, true)
	})
}

func (controller *Controller) finishLease(id string, generation uint64, state hostv1.LeaseState, stop bool) (*remoteLease, error) {
	controller.mu.Lock()
	lease := controller.remoteLeases[id]
	if lease == nil || lease.generation != generation || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		controller.mu.Unlock()
		return nil, nil
	}
	result := *lease
	lease.state = state
	delete(controller.remoteLeases, id)
	delete(controller.allocationLeases, lease.environmentID)
	controller.mu.Unlock()
	// Remove the capability before any guest or database work. A failed stop
	// must never leave a usable frontend/session behind.
	controller.hostFrontends.closeAllocationLease(id, generation, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	if result.sessionID != "" {
		_ = controller.revokeSession(context.Background(), result.sessionID)
	}
	var cleanupErr error
	if stop {
		stopContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cleanupErr = controller.stopRemoteWorkload(stopContext, result.environmentID)
		cancel()
		if cleanupErr != nil {
			controller.retryRemoteWorkloadStop(result.environmentID, 1)
		}
	}
	environment, err := controller.store.GetEnvironment(context.Background(), result.environmentID)
	if err != nil {
		log.Printf("finish remote lease %q: load allocation: %v", id, err)
		controller.retryRemoteLeaseProjection(result, targetStateForLease(stop), 1)
		return &result, errors.Join(cleanupErr, err)
	}
	targetState := int32(hostv1.AllocationState_ALLOCATION_STATE_READY)
	if stop {
		targetState = int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)
	}
	if _, err := controller.store.FinishRemoteLeaseProjection(context.Background(), result.registrationID, id, result.environmentID, environment.PolicyRevision, targetState); err != nil {
		log.Printf("finish remote lease %q: commit projection: %v", id, err)
		controller.retryRemoteLeaseProjection(result, targetState, 1)
		return &result, errors.Join(cleanupErr, err)
	}
	return &result, cleanupErr
}

func targetStateForLease(stop bool) int32 {
	if stop {
		return int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED)
	}
	return int32(hostv1.AllocationState_ALLOCATION_STATE_READY)
}

func (controller *Controller) retryRemoteLeaseProjection(lease remoteLease, targetState int32, attempt int) {
	if attempt > 4 {
		return
	}
	time.AfterFunc(time.Duration(attempt)*time.Second, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		controller.remoteAdmissionMu.Lock()
		defer controller.remoteAdmissionMu.Unlock()
		environment, err := controller.store.GetEnvironment(ctx, lease.environmentID)
		if err == nil {
			_, err = controller.store.FinishRemoteLeaseProjection(ctx, lease.registrationID, lease.id, lease.environmentID, environment.PolicyRevision, targetState)
		}
		if err != nil {
			controller.retryRemoteLeaseProjection(lease, targetState, attempt+1)
		}
	})
}

// finishLeaseMemory is used when the durable projection mutation is already
// part of the caller's transaction. It still fences capability use first.
func (controller *Controller) finishLeaseMemory(id string, generation uint64, state hostv1.LeaseState) *remoteLease {
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

func (controller *Controller) revokeAllocationLease(ctx context.Context, environment store.Environment, state hostv1.LeaseState, stop bool) {
	controller.mu.Lock()
	id := controller.allocationLeases[environment.ID]
	lease := controller.remoteLeases[id]
	controller.mu.Unlock()
	if lease != nil {
		_, _ = controller.finishLease(lease.id, lease.generation, state, stop)
	} else if stop {
		controller.stopRemoteWorkload(ctx, environment.ID)
	}
}

func (controller *Controller) revokeRemoteLeases(registrationID string) error {
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
		finished, err := controller.finishLease(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED, true)
		if finished == nil && err == nil {
			continue
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("revoke remote lease %q: %w", lease.id, err))
		}
	}
	return errors.Join(failures...)
}

func (controller *Controller) retryRemoteWorkloadStop(environmentID string, attempt int) {
	if attempt > 4 {
		return
	}
	time.AfterFunc(time.Duration(attempt)*time.Second, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := controller.stopRemoteWorkload(ctx, environmentID)
		cancel()
		if err != nil {
			controller.retryRemoteWorkloadStop(environmentID, attempt+1)
		}
	})
}

func (controller *Controller) disconnectRemoteLeases(registrationID, instance string, disconnectedAt time.Time, grace time.Duration) {
	controller.hostFrontends.closeRegistration(registrationID, instance, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	controller.mu.Lock()
	var sessions []string
	for _, lease := range controller.remoteLeases {
		if lease.registrationID == registrationID && lease.sessionID != "" {
			sessions = append(sessions, lease.sessionID)
			lease.sessionID = ""
		}
	}
	controller.mu.Unlock()
	for _, sessionID := range sessions {
		_ = controller.revokeSession(context.Background(), sessionID)
	}
	time.AfterFunc(grace, func() {
		registration, err := controller.store.GetRemoteRegistration(context.Background(), registrationID)
		if err == nil && registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) && registration.DisconnectedAt.Equal(disconnectedAt) {
			controller.revokeRemoteLeases(registrationID)
		}
	})
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
		if container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			continue
		}
		_, stopErr := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID("remote-stop-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TimeoutSeconds: &timeout})
		if stopErr != nil {
			if _, killErr := client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: derivedRequestID("remote-kill-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), Signal: vmmdv1.ProcessSignal_PROCESS_SIGNAL_KILL}); killErr != nil {
				return errors.Join(stopErr, killErr)
			}
		}
	}
	return nil
}
