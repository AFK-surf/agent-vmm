//go:build darwin || linux

package servicenet

import (
	"context"
	"errors"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (controller *Owner) ReconcileEnvironment(ctx context.Context, environment store.Environment) error {
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return err
	}

	defer controller.projectionMu.Unlock()
	return controller.ReconcileEnvironmentLocked(ctx, environment)
}

func (controller *Owner) ReconcileEnvironmentLocked(ctx context.Context, environment store.Environment) error {
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return errors.New("VM is not running")
	}
	if err := controller.projectEnvironment(ctx, environment); err != nil {
		return err
	}
	return controller.PublishSourcePoliciesLocked(ctx, "")
}

func (controller *Owner) projectEnvironment(ctx context.Context, environment store.Environment) error {
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}
	return controller.ProjectEnvironmentFromGuest(ctx, client, environment)
}

func (controller *Owner) ProjectEnvironmentFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment) error {
	disposed, err := controller.store.EnvironmentDisposed(ctx, environment)
	if err != nil {
		return err
	}
	if disposed {
		return nil
	}
	if environment.OwnerKind == store.OwnerRemoteRegistration && environment.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		return nil
	}
	requestID, err := opreceipt.NewID()
	if err != nil {
		return err
	}
	_, err = client.EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{
		RequestId: requestID, NamespaceId: environment.NamespaceID,
		Limits:     &vmmdv1.NamespaceLimits{Pids: environment.PIDs, DiskBytes: environment.DiskBytes},
		EgressMode: vmmdv1.EgressMode(environment.EgressMode),
	})
	if err != nil {
		return err
	}
	if environment.OwnerKind == store.OwnerRemoteRegistration {
		return controller.store.CommitNamespaceProjection(ctx, environment)
	}
	return nil
}

func (controller *Owner) ReconcilePolicies(ctx context.Context) error {
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return err
	}

	defer controller.projectionMu.Unlock()
	return controller.reconcilePoliciesLocked(ctx, client)
}

func (controller *Owner) reconcilePoliciesLocked(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	grants, err := controller.store.AllGrants(ctx)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Kind == "network-link" && grant.Revoked {
			if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: opreceipt.DerivedRequestID("delete-" + grant.ID), LinkId: grant.ID}); err != nil {
				return err
			}
			if err := controller.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
				return err
			}
		}
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.Revoked {
			continue
		}
		link, err := GuestLinkFromPayload(grant.Payload, grant.SourceNamespaceID, grant.TargetNamespaceID)
		if err != nil {
			return err
		}
		_, applyErr := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: opreceipt.DerivedRequestID(grant.ID), Link: link})
		if applyErr != nil {
			return applyErr
		}
	}
	if err := controller.RestoreServiceProjectionsLocked(ctx, client); err != nil {
		return err
	}
	return controller.PublishSourcePoliciesLocked(ctx, "")
}

func (controller *Owner) EnvironmentSnapshot(ctx context.Context, environment store.Environment) *hostv1.AgentEnvironment {
	result := workload.EnvironmentFromStore(environment)
	result.InventoryObservedAt = timestamppb.Now()
	state := controller.manager.Snapshot().GetState()
	client, err := controller.guestConnection.Client()
	if state != hostv1.VMState_VM_STATE_RUNNING || err != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "guest runtime is unavailable"}
		result.Containers = controller.workloads.UnknownContainers(environment.ID)
		if state == hostv1.VMState_VM_STATE_STOPPED {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_VM_STOPPED
			result.InventoryMessage = "Start the virtual machine to inspect containers, images, and volumes."
		} else {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_UNAVAILABLE
			result.InventoryMessage = "Runtime inventory is temporarily unavailable."
		}
		return result
	}
	containers, containerErr := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	volumes, volumeErr := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	usage, usageErr := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
	if containerErr != nil || volumeErr != nil || usageErr != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_DEGRADED}
		result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_STALE
		result.InventoryMessage = "Some runtime inventory could not be refreshed."
	} else {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}
		result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_AVAILABLE
	}
	if containerErr == nil {
		for _, container := range containers.GetContainers() {
			result.Containers = append(result.Containers, guestconn.Container(container))
		}
		controller.workloads.RememberContainers(environment.ID, result.Containers)
	} else {
		result.Containers = controller.workloads.UnknownContainers(environment.ID)
	}
	if volumeErr == nil {
		for _, volume := range volumes.GetVolumes() {
			result.Volumes = append(result.Volumes, guestconn.Volume(volume))
		}
	}
	if usageErr == nil {
		result.Usage = guestconn.Usage(usage.GetUsage())
		controller.workloads.RememberLocalResourceSample(environment, result)
	} else {
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "environment metrics are unavailable"}
	}
	return result
}
