//go:build darwin || linux

package host

import (
	"context"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The caller holds the environment lifecycle lock and inventory mutation gate.
func (controller *Controller) deleteWorkloadContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environment string, request *vmmdv1.DeleteContainerRequest) error {
	if _, err := client.DeleteContainer(ctx, request); err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	// A successful replay can acknowledge deletion of an earlier generation.
	if _, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: request.GetActorNamespaceId(), ContainerId: request.GetContainerId()}); err == nil {
		return nil
	} else if status.Code(err) != codes.NotFound {
		return err
	}
	if err := controller.store.DeleteManagedContainer(ctx, environment, request.GetContainerId()); err != nil {
		return err
	}
	controller.mu.Lock()
	controller.deleteWorkloadRunLocked(workloadRunKey(environment, request.GetContainerId()))
	controller.mu.Unlock()
	controller.projectionMu.Lock()
	defer controller.projectionMu.Unlock()
	return controller.projectServiceContainerLocked(ctx, client, environment, request.GetContainerId(), "", "")
}
