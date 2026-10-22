//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *operatorServer) executeAcceptedCleanup(client vmmdv1.VMMDServiceClient, operation store.Operation, plan *hostv1.CleanupPlan, currentSessionID string, beforeCleanup func(context.Context, *hostv1.CleanupPlan) error) (store.Operation, error) {
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), cleanupExecutionTimeout)
	defer cancelExecution()
	if beforeCleanup != nil {
		if err := beforeCleanup(executionCtx, plan); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	if err := server.controller.revokeEnvironmentSessions(executionCtx, plan.GetEnvironmentId(), currentSessionID); err != nil {
		return operation, server.failCleanupOperation(&operation, err)
	}
	if cleanupPlanHasMissingNamespace(plan) {
		finished, err := server.finishDiscardAfterMissingNamespace(executionCtx, client, operation, plan, currentSessionID)
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

func cleanupAdmissionError(err error, environmentID string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return resourceError(codes.DeadlineExceeded, "CLEANUP_ADMISSION_TIMEOUT", "environment", environmentID, "cleanup admission timed out before an operation was accepted")
	}
	if errors.Is(err, context.Canceled) {
		return resourceError(codes.Canceled, "CLEANUP_ADMISSION_CANCELED", "environment", environmentID, "cleanup admission was canceled before an operation was accepted")
	}
	return err
}

func (server *operatorServer) failCleanupOperation(operation *store.Operation, failure error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	if err := server.controller.failOperation(ctx, operation, "CLEANUP_FAILED", failure); err != nil {
		return errors.Join(errCleanupTerminalPersistence, failure, fmt.Errorf("persist cleanup failure: %w", err))
	}
	return nil
}

func (server *operatorServer) completeCleanupOperation(operation *store.Operation) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	if err := server.controller.completeOperation(ctx, operation, "complete"); err != nil {
		return errors.Join(errCleanupTerminalPersistence, fmt.Errorf("persist cleanup completion: %w", err))
	}
	return nil
}

func cleanupPlanHasMissingNamespace(plan *hostv1.CleanupPlan) bool {
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

func (server *operatorServer) finishDiscardAfterMissingNamespace(ctx context.Context, client vmmdv1.VMMDServiceClient, operation store.Operation, plan *hostv1.CleanupPlan, currentSessionID string) (store.Operation, error) {
	environmentID := plan.GetEnvironmentId()
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return operation, err
	}
	if err == nil {
		if err := server.controller.revokeEnvironmentSessions(ctx, environmentID, currentSessionID); err != nil {
			return operation, err
		}
		grants, grantErr := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
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
		if !equalStrings(disclosed, actual) {
			return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", environmentID, "grant inventory changed; request a new cleanup plan")
		}
		if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
			return operation, err
		}
		var networkGrants []store.Grant
		for _, grant := range grants {
			if grant.Kind == "network-link" && (grant.SourceNamespaceID == environment.NamespaceID || grant.TargetNamespaceID == environment.NamespaceID) {
				networkGrants = append(networkGrants, grant)
			}
		}
		if len(networkGrants) > 0 {
			if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
				return operation, errors.New("VM must be running to settle missing-namespace network links")
			}
			for _, grant := range networkGrants {
				_, deleteErr := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID(operation.RequestID + "-missing-link-" + grant.ID), LinkId: grant.ID})
				if deleteErr != nil && status.Code(deleteErr) != codes.NotFound {
					return operation, fmt.Errorf("delete missing-namespace network link %q: %w", grant.ID, deleteErr)
				}
				if compactErr := server.controller.store.CompactRevokedGrant(ctx, grant.ID); compactErr != nil {
					return operation, fmt.Errorf("compact missing-namespace network link %q: %w", grant.ID, compactErr)
				}
			}
		}
	}
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_RUNNING)
	operation.Stage = "retrying-host-cleanup"
	operation.ErrorReason = ""
	operation.ErrorMessage = ""
	if err := server.controller.store.UpdateOperation(ctx, operation); err != nil {
		return operation, err
	}
	if err == nil {
		if err := server.controller.store.DiscardEnvironment(ctx, environmentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	server.controller.forgetContainers(environmentID)
	if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if err := server.controller.publishCleanupSourcePoliciesLocked(ctx, ""); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	return operation, server.completeCleanupOperation(&operation)
}

func (server *operatorServer) performCleanup(ctx context.Context, client vmmdv1.VMMDServiceClient, operation *store.Operation, plan *hostv1.CleanupPlan) error {
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	environment, err := server.controller.store.GetEnvironment(ctx, plan.GetEnvironmentId())
	if err != nil {
		return fmt.Errorf("load cleanup environment: %w", err)
	}
	grants, err := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
	if err != nil {
		return fmt.Errorf("list cleanup grants: %w", err)
	}
	if len(grants) > cleanupPlanMaxInventory {
		return cleanupInventoryLimitError(environment.ID)
	}
	if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
		return fmt.Errorf("revoke cleanup grants: %w", err)
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID(operation.RequestID + "-link-" + grant.ID), LinkId: grant.ID}); err != nil {
			if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
				// The desired Guest state is already present.
			} else {
				return fmt.Errorf("delete cleanup network link %q: %w", grant.ID, err)
			}
		}
		if err := server.controller.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
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
			return resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "container", containerID, "container generation changed; request a new cleanup plan")
		}
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
			_, err := server.controller.stopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-stop-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
			if err != nil {
				return fmt.Errorf("stop cleanup container %q: %w", containerID, err)
			}
		}
		if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-delete-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID, ExpectedGenerationId: container.GetGenerationId()}); err != nil {
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
		if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: derivedRequestID(operation.RequestID + "-volume-" + volumeID), ActorNamespaceId: environment.NamespaceID, VolumeId: volumeID, ExpectedGenerationId: impact.GetResource().GetGenerationId()}); err != nil {
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
	if _, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: derivedRequestID(operation.RequestID + "-namespace"), NamespaceId: environment.NamespaceID}); err != nil {
		return fmt.Errorf("delete cleanup namespace: %w", err)
	}
	if err := server.controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return fmt.Errorf("discard cleanup host environment: %w", err)
	}
	server.controller.forgetContainers(environment.ID)
	if err := server.controller.publishCleanupSourcePoliciesLocked(ctx, ""); err != nil {
		return fmt.Errorf("publish cleanup source policies: %w", err)
	}
	return nil
}
