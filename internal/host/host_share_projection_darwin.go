//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (controller *Controller) requireHostShareOffline() error {
	if controller.manager == nil {
		return nil
	}
	state := controller.vmLifecycle.snapshot().GetState()
	if (state != hostv1.VMState_VM_STATE_STOPPED && state != hostv1.VMState_VM_STATE_FAILED) || controller.manager.HasMachineAttachment() {
		return resourceError(codes.FailedPrecondition, "HOST_SHARE_REQUIRES_VM_STOPPED", "vm", "local", "stop the VM before creating, rebinding, or revoking host shares")
	}
	return nil
}

func (controller *Controller) publishHostSharesLocked(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	if client == nil || !controller.hostSharesDirty {
		return nil
	}
	projections := make([]*vmmdv1.HostShareProjection, 0, len(controller.hostShares))
	for id, value := range controller.hostShares {
		environment, err := controller.store.GetEnvironment(ctx, value.EnvironmentID)
		if err != nil {
			return err
		}
		projections = append(projections, &vmmdv1.HostShareProjection{ShareId: id, ActorNamespaceId: environment.NamespaceID, ReadOnly: value.ReadOnly, Revision: value.Revision})
	}
	response, projectionErr := client.ApplyHostShares(ctx, &vmmdv1.ApplyHostSharesRequest{Shares: projections})
	partial := false
	for id := range controller.hostShares {
		value, err := controller.store.GetHostShare(ctx, id)
		if err != nil {
			return err
		}
		if value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKING || value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
			continue
		}
		state, reason := hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE, ""
		if projectionErr != nil || !slices.Contains(response.GetAppliedShareIds(), id) {
			state, reason = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE, "Guest host share projection is unavailable"
			partial = true
		}
		if value.State != state || value.UnavailableReason != reason {
			value.State, value.UnavailableReason = state, reason
			if err := controller.store.UpdateHostShare(ctx, value, value.Revision); err != nil {
				return err
			}
		}
	}
	if projectionErr != nil {
		return resourceError(codes.Unavailable, "HOST_SHARE_PROJECTION_UNKNOWN", "vm", "local", projectionErr.Error())
	}
	if partial {
		return resourceError(codes.Unavailable, "HOST_SHARE_PROJECTION_UNKNOWN", "vm", "local", "one or more Guest host share directories are unavailable")
	}
	controller.hostSharesDirty = false
	return nil
}

func (controller *Controller) stopHostShareConsumersLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, value store.HostShare) error {
	if client == nil {
		return nil
	}
	environment, err := controller.store.GetEnvironment(ctx, value.EnvironmentID)
	if err != nil {
		return err
	}
	cursor := ""
	for {
		page, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, AfterContainerId: cursor, PageSize: vmmdv1.MaxContainerPageSize})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		for _, container := range page.GetContainers() {
			uses := false
			for _, mount := range container.GetVolumes() {
				if mount.GetHostShare() == value.ID {
					uses = true
					break
				}
			}
			if !uses {
				continue
			}
			if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
				_, err := controller.stopWorkloadContainerWithIntentLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID(fmt.Sprintf("share-stop/%s/%d/%s/%s", value.ID, value.Revision, container.GetId(), container.GetInstanceId())), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TimeoutSeconds: optional(uint32(1))}, false)
				if err != nil {
					return fmt.Errorf("host share %s consumer %s stop unresolved: %w", value.ID, container.GetId(), err)
				}
			}
			observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId()})
			if status.Code(err) == codes.NotFound {
				continue
			}
			if err != nil {
				return err
			}
			state := observed.GetContainer().GetState()
			if state != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && state != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
				return resourceError(codes.FailedPrecondition, "HOST_SHARE_CONSUMER_UNRESOLVED", "container", container.GetId(), "shared-directory consumer has not stopped")
			}
		}
		cursor = page.GetNextCursor()
		if cursor == "" {
			return nil
		}
	}
}

func (controller *Controller) authorizeHostShareMounts(ctx context.Context, environment string, mounts []*vmmdv1.VolumeMount) error {
	for _, mount := range mounts {
		source, ok := mount.GetSource().(*vmmdv1.VolumeMount_HostShare)
		if !ok {
			continue
		}
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, environment)
		if err != nil {
			return err
		}
		value, err := controller.store.GetHostShare(ctx, source.HostShare)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err != nil || !approved || value.EnvironmentID != environment || (value.ReadOnly && !mount.GetReadOnly()) || value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKING || value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
			return resourceError(codes.PermissionDenied, "HOST_SHARE_NOT_AUTHORIZED", "host_share", source.HostShare, "share is not authorized for this service environment and access mode")
		}
		if value.State != hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE {
			return resourceError(codes.FailedPrecondition, "HOST_SHARE_SOURCE_UNAVAILABLE", "host_share", source.HostShare, "approved host directory is unavailable. Ask the operator to inspect or rebind the share")
		}
		file, err := reopenHostShareSource(value.Source)
		if err != nil {
			return resourceError(codes.FailedPrecondition, "HOST_SHARE_SOURCE_UNAVAILABLE", "host_share", value.ID, "approved host directory is unavailable. Ask the operator to inspect or rebind the share")
		}
		file.Close()
	}
	return nil
}
