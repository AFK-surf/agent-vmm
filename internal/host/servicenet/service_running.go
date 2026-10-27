//go:build darwin || linux

package servicenet

import (
	"context"
	"errors"
	"fmt"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (controller *Owner) RestoreManagedServiceContainers(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	containers, err := controller.store.ListManagedContainers(ctx)
	if err != nil {
		return err
	}
	// A saved stop intent can outlive the Host that accepted it while the Guest keeps the instance running.
	tracksRunning := func(managed store.ManagedContainer) bool {
		run, exists := controller.workloads.Instance(managed.EnvironmentID, managed.ContainerID)
		return exists && !run.Stopping
	}

	var failures []error
	for _, managed := range containers {
		if !managed.DesiredRunning && !tracksRunning(managed) {
			continue
		}
		err := func() error {
			unlock, lockErr := controller.workloads.LockEnvironment(ctx, managed.EnvironmentID)
			if lockErr != nil {
				return lockErr
			}

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
				_, err = controller.workloads.StopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{
					RequestId:        opreceipt.DerivedRequestID(fmt.Sprintf("service-recover-stop/%s/%s/%s", current.EnvironmentID, current.ContainerID, observed.GetContainer().GetInstanceId())),
					ActorNamespaceId: environment.NamespaceID, ContainerId: current.ContainerID})
				return err
			}
			if status.Code(err) == codes.NotFound {
				if err := controller.projectionMu.Lock(ctx); err != nil {
					return err
				}

				defer controller.projectionMu.Unlock()
				return controller.ProjectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, "", "")
			}
			if err != nil {
				return err
			}
			container := observed.GetContainer()
			if container.GetGenerationId() != current.ContainerGenerationID {
				if err := controller.projectionMu.Lock(ctx); err != nil {
					return err
				}

				defer controller.projectionMu.Unlock()
				return controller.ProjectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, container.GetGenerationId(), "")
			}
			if container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				_, err := controller.workloads.StartWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StartContainerRequest{
					RequestId:        opreceipt.DerivedRequestID(fmt.Sprintf("service-recover/%s/%s/%s/%s", current.EnvironmentID, current.ContainerID, container.GetGenerationId(), container.GetStartRequestInstanceId())),
					ActorNamespaceId: environment.NamespaceID, ContainerId: current.ContainerID, ExpectedGenerationId: current.ContainerGenerationID})
				return err
			}
			if err := controller.projectionMu.Lock(ctx); err != nil {
				return err
			}

			defer controller.projectionMu.Unlock()
			return controller.ProjectServiceContainerLocked(ctx, client, environment.ID, current.ContainerID, current.ContainerGenerationID, container.GetInstanceId())
		}()
		if err != nil {
			failures = append(failures, fmt.Errorf("recover service container %s/%s: %w", managed.EnvironmentID, managed.ContainerID, err))
		}
	}
	return errors.Join(failures...)
}
