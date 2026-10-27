//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
)

func (r *Runtime) DeleteContainer(ctx context.Context, namespaceID, containerID, expectedGenerationID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	if err := validateID("container_id", containerID); err != nil {
		return err
	}
	if expectedGenerationID == "" {
		return errors.New("expected_generation_id is required")
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return err
	}
	defer unlock()
	unlockContainer, err := r.lockContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	defer unlockContainer()
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	if current.GetGenerationId() != expectedGenerationID {
		return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_CONTAINER_GENERATION_CONFLICT, "container was replaced; refresh and try again", hostv1.WithResource("container", containerID))
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_CONTAINER_RUNNING, "running container cannot be deleted", hostv1.WithResource("container", containerID))
	}
	if task, err := container.Task(nsctx, nil); err == nil {
		cleanup, cancel := context.WithTimeout(nsctx, 10*time.Second)
		err = deleteTask(cleanup, task)
		cancel()
		if err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	if err := func() error {
		return container.Delete(nsctx, containerd.WithSnapshotCleanup)
	}(); err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	if err := inventory.removeContainer(namespaceID, containerID); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(logPath(namespaceID, containerID, expectedGenerationID)))
}
