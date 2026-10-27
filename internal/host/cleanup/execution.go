//go:build darwin || linux

package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) executeAcceptedCleanup(client vmmdv1.VMMDServiceClient, operation store.Operation, plan *hostv1.CleanupPlan, beforeCleanup func(context.Context, *hostv1.CleanupPlan) error) (store.Operation, error) {
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), cleanupExecutionTimeout)
	defer cancelExecution()
	if beforeCleanup != nil {
		if err := beforeCleanup(executionCtx, plan); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	if err := server.sessions.RevokeEnvironmentSessions(executionCtx, plan.GetEnvironmentId()); err != nil {
		return operation, server.failCleanupOperation(&operation, err)
	}
	if planHasMissingNamespace(plan) {
		finished, err := server.finishDiscardAfterMissingNamespace(executionCtx, client, operation, plan)
		if err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
		return finished, nil
	}
	if err := server.performCleanup(executionCtx, client, &operation, plan); err != nil {
		return operation, server.failCleanupOperation(&operation, err)
	}
	return operation, server.completeCleanupOperation(&operation)
}

func admissionError(err error, environmentID string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return hostv1.NewError(codes.DeadlineExceeded, hostv1.ErrorReason_ERROR_REASON_CLEANUP_ADMISSION_TIMEOUT, "cleanup admission timed out before an operation was accepted", hostv1.WithResource("environment", environmentID))
	}
	if errors.Is(err, context.Canceled) {
		return hostv1.NewError(codes.Canceled, hostv1.ErrorReason_ERROR_REASON_CLEANUP_ADMISSION_CANCELED, "cleanup admission was canceled before an operation was accepted", hostv1.WithResource("environment", environmentID))
	}
	return err
}

func (server *Owner) failCleanupOperation(operation *store.Operation, failure error) error {
	ctx, cancel := context.WithTimeout(context.Background(), terminalTimeout)
	defer cancel()
	if err := opreceipt.Fail(ctx, server.store, operation, "CLEANUP_FAILED", failure); err != nil {
		return errors.Join(errCleanupTerminalPersistence, failure, fmt.Errorf("persist cleanup failure: %w", err))
	}
	return nil
}

func (server *Owner) completeCleanupOperation(operation *store.Operation) error {
	ctx, cancel := context.WithTimeout(context.Background(), terminalTimeout)
	defer cancel()
	if err := opreceipt.Complete(ctx, server.store, operation, "complete"); err != nil {
		return errors.Join(errCleanupTerminalPersistence, fmt.Errorf("persist cleanup completion: %w", err))
	}
	return nil
}

func planHasMissingNamespace(plan *hostv1.CleanupPlan) bool {
	if plan.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return false
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() == "environment" && impact.GetResource().GetGenerationId() == "namespace-missing" {
			return true
		}
	}
	return false
}

func (server *Owner) finishDiscardAfterMissingNamespace(ctx context.Context, client vmmdv1.VMMDServiceClient, operation store.Operation, plan *hostv1.CleanupPlan) (store.Operation, error) {
	environmentID := plan.GetEnvironmentId()
	if err := server.services.ProjectionLock().Lock(ctx); err != nil {
		return store.Operation{}, err
	}

	defer server.services.ProjectionLock().Unlock()
	environment, err := server.store.GetEnvironment(ctx, environmentID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return operation, err
	}
	if err == nil {
		if err := server.sessions.RevokeEnvironmentSessions(ctx, environmentID); err != nil {
			return operation, err
		}
		grants, grantErr := server.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
		if grantErr != nil {
			return operation, grantErr
		}
		if len(grants) > cleanupPlanMaxInventory {
			return operation, cleanupInventoryLimitError(environmentID)
		}
		var disclosed, actual []string
		for _, impact := range plan.GetImpacts() {
			if impact.GetResource().GetKind() == "grant" {
				disclosed = append(disclosed, impact.GetResource().GetResourceId())
			}
		}
		for _, grant := range grants {
			if !grant.Revoked || grant.Kind == "network-link" {
				actual = append(actual, grant.ID)
			}
		}
		sort.Strings(disclosed)
		sort.Strings(actual)
		if !slices.Equal(disclosed, actual) {
			return operation, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_CLEANUP_PLAN_STALE, "grant inventory changed; request a new cleanup plan", hostv1.WithResource("environment", environmentID))
		}
		if err := server.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
			return operation, err
		}
		var networkGrants []store.Grant
		for _, grant := range grants {
			if grant.Kind == "network-link" && (grant.SourceNamespaceID == environment.NamespaceID || grant.TargetNamespaceID == environment.NamespaceID) {
				networkGrants = append(networkGrants, grant)
			}
		}
		if len(networkGrants) > 0 {
			if server.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
				return operation, errors.New("VM must be running to settle missing-namespace network links")
			}
			for _, grant := range networkGrants {
				_, deleteErr := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-missing-link-" + grant.ID), LinkId: grant.ID})
				if deleteErr != nil && status.Code(deleteErr) != codes.NotFound {
					return operation, fmt.Errorf("delete missing-namespace network link %q: %w", grant.ID, deleteErr)
				}
				if compactErr := server.store.CompactRevokedGrant(ctx, grant.ID); compactErr != nil {
					return operation, fmt.Errorf("compact missing-namespace network link %q: %w", grant.ID, compactErr)
				}
			}
		}
	}
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_RUNNING)
	operation.Stage = "retrying-host-cleanup"
	operation.ErrorReason = ""
	operation.ErrorMessage = ""
	if err := server.store.UpdateOperation(ctx, operation); err != nil {
		return operation, err
	}
	if err == nil {
		if err := server.store.DiscardEnvironment(ctx, environmentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	server.workloads.ForgetContainers(environmentID)
	if server.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if err := server.services.PublishSourcePoliciesLocked(ctx, ""); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	return operation, server.completeCleanupOperation(&operation)
}

func (server *Owner) performCleanup(ctx context.Context, client vmmdv1.VMMDServiceClient, operation *store.Operation, plan *hostv1.CleanupPlan) error {
	if err := server.services.ProjectionLock().Lock(ctx); err != nil {
		return err
	}

	defer server.services.ProjectionLock().Unlock()
	environment, err := server.store.GetEnvironment(ctx, plan.GetEnvironmentId())
	if err != nil {
		return fmt.Errorf("load cleanup environment: %w", err)
	}
	grants, err := server.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
	if err != nil {
		return fmt.Errorf("list cleanup grants: %w", err)
	}
	if len(grants) > cleanupPlanMaxInventory {
		return cleanupInventoryLimitError(environment.ID)
	}
	if err := server.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
		return fmt.Errorf("revoke cleanup grants: %w", err)
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-link-" + grant.ID), LinkId: grant.ID}); err != nil {
			if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
				// The desired Guest state is already present.
			} else {
				return fmt.Errorf("delete cleanup network link %q: %w", grant.ID, err)
			}
		}
		if err := server.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
			return fmt.Errorf("compact cleanup network link %q: %w", grant.ID, err)
		}
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() != "container" {
			continue
		}
		containerID := impact.GetResource().GetResourceId()
		inspected, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
		if err != nil {
			return fmt.Errorf("inspect cleanup container %q: %w", containerID, err)
		}
		container := inspected.GetContainer()
		if container.GetGenerationId() == "" || container.GetGenerationId() != impact.GetResource().GetGenerationId() {
			return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_CLEANUP_PLAN_STALE, "container generation changed; request a new cleanup plan", hostv1.WithResource("container", containerID))
		}
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
			_, err := server.workloads.StopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-stop-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
			if err != nil {
				return fmt.Errorf("stop cleanup container %q: %w", containerID, err)
			}
		}
		if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-delete-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID, ExpectedGenerationId: container.GetGenerationId()}); err != nil {
			return fmt.Errorf("delete cleanup container %q: %w", containerID, err)
		}
	}
	if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		return nil
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() != "volume" {
			continue
		}
		volumeID := impact.GetResource().GetResourceId()
		if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-volume-" + volumeID), ActorNamespaceId: environment.NamespaceID, VolumeId: volumeID, ExpectedGenerationId: impact.GetResource().GetGenerationId()}); err != nil {
			return fmt.Errorf("delete cleanup volume %q: %w", volumeID, err)
		}
	}
	remainingContainers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, Limit: 1})
	if err != nil {
		return fmt.Errorf("verify cleanup containers: %w", err)
	}
	if len(remainingContainers.GetContainers()) != 0 {
		return fmt.Errorf("verify cleanup containers: %d container records remain", len(remainingContainers.GetContainers()))
	}
	remainingVolumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID, Limit: 1, RelatedContainerLimit: 1})
	if err != nil {
		return fmt.Errorf("verify cleanup volumes: %w", err)
	}
	if len(remainingVolumes.GetVolumes()) != 0 {
		return fmt.Errorf("verify cleanup volumes: %d volume records remain", len(remainingVolumes.GetVolumes()))
	}
	if _, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-namespace"), NamespaceId: environment.NamespaceID}); err != nil {
		return fmt.Errorf("delete cleanup namespace: %w", err)
	}
	if err := server.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return fmt.Errorf("discard cleanup host environment: %w", err)
	}
	server.workloads.ForgetContainers(environment.ID)
	if err := server.services.PublishSourcePoliciesLocked(ctx, ""); err != nil {
		return fmt.Errorf("publish cleanup source policies: %w", err)
	}
	return nil
}
