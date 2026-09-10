//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
)

func observedSandboxState(ctx context.Context, container containerd.Container, labels map[string]string, empty func() (bool, error)) vmmdv1.ContainerState {
	task, err := container.Task(ctx, nil)
	if err == nil {
		status, err := task.Status(ctx)
		if err == nil && status.Status == containerd.Running && labels[containerStateLabel] == "running" && labels[containerInstanceLabel] != "" {
			return vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
		}
		return vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN
	}
	if !errdefs.IsNotFound(err) {
		return vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN
	}
	stopped, err := empty()
	if err != nil || !stopped {
		return vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN
	}
	if labels[containerStateLabel] == "" || labels[containerStateLabel] == "created" {
		return vmmdv1.ContainerState_CONTAINER_STATE_CREATED
	}
	return vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
}

func sandboxCgroupEmpty(namespaceID, containerID string) (bool, error) {
	values, err := keyValueFile(filepath.Join("/sys/fs/cgroup", resourcev2.NamespaceCgroup(namespaceID), containerID, "cgroup.events"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	populated, ok := values["populated"]
	return ok && populated == 0, nil
}
