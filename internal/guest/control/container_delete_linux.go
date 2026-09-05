//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
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
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	if current.GetGenerationId() != expectedGenerationID {
		return detailedError(codes.Aborted, "CONTAINER_GENERATION_CONFLICT", "container was replaced; refresh and try again", "container", containerID)
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return detailedError(codes.FailedPrecondition, "CONTAINER_RUNNING", "running container cannot be deleted", "container", containerID)
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
	if err := r.guardOCIMetadataGrowth(ctx, func() error {
		return container.Delete(nsctx, containerd.WithSnapshotCleanup)
	}); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(logPath(namespaceID, containerID, expectedGenerationID)))
}
