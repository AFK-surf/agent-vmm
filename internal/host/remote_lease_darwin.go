//go:build darwin

package host

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type remoteLease struct {
	id            string
	allocationID  string
	environmentID string
	workloadRef   string
	generation    uint64
	expiresAt     time.Time
	state         hostv1.LeaseState
	sessionID     string
}

func leaseToProto(lease *remoteLease) *hostv1.RemoteLease {
	if lease == nil {
		return nil
	}
	return &hostv1.RemoteLease{LeaseId: lease.id, AllocationId: lease.allocationID, WorkloadRef: lease.workloadRef, Generation: lease.generation, ExpiresAt: timestamppb.New(lease.expiresAt), State: lease.state}
}

func (controller *Controller) scheduleLease(id string, generation uint64, deadline time.Time) {
	time.AfterFunc(time.Until(deadline), func() {
		controller.finishLease(id, generation, hostv1.LeaseState_LEASE_STATE_EXPIRED, true)
	})
}

func (controller *Controller) finishLease(id string, generation uint64, state hostv1.LeaseState, stop bool) *remoteLease {
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
	if result.sessionID != "" {
		_ = controller.revokeSession(context.Background(), result.sessionID)
	}
	if stop {
		controller.stopRemoteWorkload(context.Background(), result.environmentID)
		environment, err := controller.store.GetEnvironment(context.Background(), result.environmentID)
		if err == nil && environment.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED) {
			_, _ = controller.store.SetAllocationState(context.Background(), environment.ID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
		}
	}
	return &result
}

func (controller *Controller) revokeAllocationLease(ctx context.Context, environment store.Environment, state hostv1.LeaseState, stop bool) {
	controller.mu.Lock()
	id := controller.allocationLeases[environment.ID]
	lease := controller.remoteLeases[id]
	controller.mu.Unlock()
	if lease != nil {
		controller.finishLease(lease.id, lease.generation, state, stop)
	} else if stop {
		controller.stopRemoteWorkload(ctx, environment.ID)
	}
}

func (controller *Controller) revokeAllRemoteLeases(context.Context) {
	controller.mu.Lock()
	leases := make([]remoteLease, 0, len(controller.remoteLeases))
	for _, lease := range controller.remoteLeases {
		leases = append(leases, *lease)
	}
	controller.mu.Unlock()
	for _, lease := range leases {
		controller.finishLease(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED, true)
	}
}

func (controller *Controller) disconnectRemoteLeases(registrationID, instance string, grace time.Duration) {
	controller.mu.Lock()
	var sessions []string
	for _, lease := range controller.remoteLeases {
		if lease.sessionID != "" {
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
		if err == nil && registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) && registration.ConnectionInstance == instance {
			controller.revokeAllRemoteLeases(context.Background())
		}
	})
}

func (controller *Controller) stopRemoteWorkload(ctx context.Context, environmentID string) {
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err != nil || environment.OwnerKind != store.OwnerRemoteRegistration {
		return
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, environment.RegistrationID)
	if err != nil {
		return
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return
	}
	defer connection.Close()
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return
	}
	timeout := registration.StopGraceSeconds
	for _, container := range containers.GetContainers() {
		if container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			continue
		}
		_, stopErr := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: randomRequestIDFor("remote-stop-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TimeoutSeconds: &timeout})
		if stopErr != nil {
			_, _ = client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: randomRequestIDFor("remote-kill-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), Signal: vmmdv1.ProcessSignal_PROCESS_SIGNAL_KILL})
		}
	}
}
