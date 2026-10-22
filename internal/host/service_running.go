//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"fmt"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (controller *Controller) restoreManagedServiceContainers(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	containers, err := controller.store.ListManagedContainers(ctx)
	if err != nil {
		return err
	}
	// A saved stop intent can outlive the Host that accepted it while the Guest keeps the instance running.
	tracksRunning := func(managed store.ManagedContainer) bool {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		run := controller.workloadRuns[workloadRunKey(managed.EnvironmentID, managed.ContainerID)]
		return run != nil && !run.stopping
	}
	var failures []error
	for _, managed := range containers {
		if !managed.DesiredRunning && !tracksRunning(managed) {
			continue
		}
		err := func() error {
			unlock := controller.lockEnvironment(managed.EnvironmentID)
			defer unlock()
			current, err := controller.store.GetManagedContainer(ctx, managed.EnvironmentID, managed.ContainerID)
			if err != nil {
				return err
			}
			if !current.DesiredRunning && !tracksRunning(current) {
				return nil
			}
			environment, err := controller.store.GetEnvironment(ctx, current.EnvironmentID)
			if err != nil {
				return err
			}
			observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: current.ContainerID})
			if !current.DesiredRunning {
				if status.Code(err) == codes.NotFound {
					return nil
				}
				if err != nil || observed.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
					return err
				}
				_, err = controller.stopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{
					RequestId:        derivedRequestID(fmt.Sprintf("service-recover-stop/%s/%s/%s", current.EnvironmentID, current.ContainerID, observed.GetContainer().GetInstanceId())),
					ActorNamespaceId: environment.NamespaceID, ContainerId: current.ContainerID})
				return err
			}
			if status.Code(err) == codes.NotFound {
				controller.projectionMu.Lock()
				defer controller.projectionMu.Unlock()
				return controller.projectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, "", "")
			}
			if err != nil {
				return err
			}
			container := observed.GetContainer()
			if container.GetGenerationId() != current.ContainerGenerationID {
				controller.projectionMu.Lock()
				defer controller.projectionMu.Unlock()
				return controller.projectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, container.GetGenerationId(), "")
			}
			if container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				_, err := controller.startWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StartContainerRequest{
					RequestId:        derivedRequestID(fmt.Sprintf("service-recover/%s/%s/%s/%s", current.EnvironmentID, current.ContainerID, container.GetGenerationId(), container.GetStartRequestInstanceId())),
					ActorNamespaceId: environment.NamespaceID, ContainerId: current.ContainerID, ExpectedGenerationId: current.ContainerGenerationID})
				return err
			}
			controller.projectionMu.Lock()
			defer controller.projectionMu.Unlock()
			return controller.projectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, current.ContainerGenerationID, container.GetInstanceId())
		}()
		if err != nil {
			failures = append(failures, fmt.Errorf("recover service container %s/%s: %w", managed.EnvironmentID, managed.ContainerID, err))
		}
	}
	return errors.Join(failures...)
}
