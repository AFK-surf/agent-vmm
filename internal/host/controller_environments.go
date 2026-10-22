//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"sync"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (controller *Controller) ensureLocalEnvironment(ctx context.Context, environmentID, displayName string) (store.Environment, error) {
	if environmentID == "" {
		return store.Environment{}, errors.New("environment ID is required")
	}
	if _, err := controller.vmLifecycle.Start(ctx); err != nil {
		return store.Environment{}, err
	}
	releaseInventory, err := controller.lockInventoryMutation(ctx)
	if err != nil {
		return store.Environment{}, err
	}
	defer releaseInventory()
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return store.Environment{}, errors.New("VM stopped before the environment mutation could begin")
	}
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err == nil {
		if environment.OwnerKind != store.OwnerLocal {
			return store.Environment{}, errors.New("environment is owned by a remote registration")
		}
		return environment, controller.reconcileEnvironmentLocked(ctx, environment)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.Environment{}, err
	}
	if displayName == "" {
		displayName = environmentID
	}
	namespaceID, err := randomID()
	if err != nil {
		return store.Environment{}, err
	}
	environment, err = controller.store.CreateEnvironment(ctx, store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: displayName,
		CPUMillis: 2000, MemoryBytes: 2 << 30, PIDs: 512, DiskBytes: 16 << 30,
		EgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), OwnerKind: store.OwnerLocal,
	})
	if err != nil {
		return store.Environment{}, err
	}
	return environment, controller.reconcileEnvironmentLocked(ctx, environment)
}

func (controller *Controller) lockEnvironment(environmentID string) func() {
	value, _ := controller.environmentLocks.LoadOrStore(environmentID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func (controller *Controller) lockEnvironmentContext(ctx context.Context, environmentID string) (func(), error) {
	value, _ := controller.environmentLocks.LoadOrStore(environmentID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lock.TryLock() {
			if err := ctx.Err(); err != nil {
				lock.Unlock()
				return nil, err
			}
			return lock.Unlock, nil
		}
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (controller *Controller) reconcileEnvironment(ctx context.Context, environment store.Environment) error {
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.reconcileEnvironmentLocked(ctx, environment)
}

func (controller *Controller) reconcileEnvironmentLocked(ctx context.Context, environment store.Environment) error {
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return errors.New("VM is not running")
	}
	if err := controller.projectEnvironment(ctx, environment); err != nil {
		return err
	}
	return controller.publishSourcePoliciesLocked(ctx, "")
}

func (controller *Controller) projectEnvironment(ctx context.Context, environment store.Environment) error {
	connection, err := controller.manager.DialVMMD(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	client := vmmdv1.NewVMMDServiceClient(connection)
	return controller.projectEnvironmentFromGuest(ctx, client, environment)
}

func (controller *Controller) projectEnvironmentFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient, environment store.Environment) error {
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
	requestID, err := randomID()
	if err != nil {
		return err
	}
	_, err = client.EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{
		RequestId: requestID, NamespaceId: environment.NamespaceID,
		Limits:     &vmmdv1.NamespaceLimits{CpuMillis: environment.CPUMillis, MemoryBytes: environment.MemoryBytes, Pids: environment.PIDs, DiskBytes: environment.DiskBytes},
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

func (controller *Controller) reconcilePolicies(ctx context.Context) error {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.reconcilePoliciesLocked(ctx, client)
}

func (controller *Controller) reconcilePoliciesLocked(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	grants, err := controller.store.AllGrants(ctx)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Kind == "network-link" && grant.Revoked {
			if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID("delete-" + grant.ID), LinkId: grant.ID}); err != nil {
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
		link, err := guestLinkFromPayload(grant.Payload, grant.SourceNamespaceID, grant.TargetNamespaceID)
		if err != nil {
			return err
		}
		_, applyErr := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: derivedRequestID(grant.ID), Link: link})
		if applyErr != nil {
			return applyErr
		}
	}
	if err := controller.restoreServiceProjectionsLocked(ctx, client); err != nil {
		return err
	}
	return controller.publishSourcePoliciesLocked(ctx, "")
}

func derivedRequestID(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "reconcile-" + hex.EncodeToString(digest[:26])
}

func (controller *Controller) guestClient(ctx context.Context) (vmmdv1.VMMDServiceClient, *grpc.ClientConn, error) {
	connection, err := controller.manager.DialVMMD(ctx)
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "guest control service is unavailable")
	}
	return vmmdv1.NewVMMDServiceClient(connection), connection, nil
}

func (controller *Controller) environmentSnapshot(ctx context.Context, environment store.Environment) *hostv1.AgentEnvironment {
	result := environmentFromStore(environment)
	result.InventoryObservedAt = timestamppb.Now()
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		result.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "guest runtime is unavailable"}
		result.Containers = controller.unknownContainers(environment.ID)
		if controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_STOPPED {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_VM_STOPPED
			result.InventoryMessage = "Start the virtual machine to inspect containers, images, and volumes."
		} else {
			result.InventoryState = hostv1.InventoryState_INVENTORY_STATE_UNAVAILABLE
			result.InventoryMessage = "Runtime inventory is temporarily unavailable."
		}
		return result
	}
	defer connection.Close()
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
			result.Containers = append(result.Containers, containerFromGuest(container))
		}
		controller.rememberContainers(environment.ID, result.Containers)
	} else {
		result.Containers = controller.unknownContainers(environment.ID)
	}
	if volumeErr == nil {
		for _, volume := range volumes.GetVolumes() {
			result.Volumes = append(result.Volumes, volumeFromGuest(volume))
		}
	}
	if usageErr == nil {
		result.Usage = usageFromGuest(usage.GetUsage())
		controller.rememberLocalResourceSample(environment, result)
	} else {
		result.Usage = &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true, UnavailableReason: "environment metrics are unavailable"}
	}
	return result
}

func (controller *Controller) rememberContainers(environmentID string, containers []*hostv1.Container) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.observed[environmentID] = cloneContainers(containers, false)
}

func (controller *Controller) unknownContainers(environmentID string) []*hostv1.Container {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return cloneContainers(controller.observed[environmentID], true)
}

func (controller *Controller) forgetContainers(environmentID string) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	delete(controller.observed, environmentID)
	delete(controller.localResourceCache, environmentID)
	prefix := environmentID + "\x00"
	for key := range controller.stoppedWorkloadInstances {
		if strings.HasPrefix(key, prefix) {
			delete(controller.stoppedWorkloadInstances, key)
		}
	}
}

func cloneContainers(containers []*hostv1.Container, unknown bool) []*hostv1.Container {
	result := make([]*hostv1.Container, 0, len(containers))
	for _, container := range containers {
		copy := proto.Clone(container).(*hostv1.Container)
		if unknown {
			copy.State = hostv1.ContainerState_CONTAINER_STATE_UNKNOWN
		}
		result = append(result, copy)
	}
	return result
}
