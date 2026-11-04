//go:build linux

package control

import (
	"context"
	"errors"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
)

func (r *Runtime) releaseSystemInstallationImagesMaterializerHeld(ctx context.Context, installationID string) error {
	namespaceID := systemInstallationNamespace(installationID)
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespaceID, true); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return err
	}
	for _, container := range containers {
		info, err := container.Info(runtimeContext(ctx))
		if err != nil {
			return err
		}
		if info.Labels[runtimeOwnerLabel] != namespaceID {
			continue
		}
		observed, err := r.inspectContainer(runtimeContext(ctx), namespaceID, container)
		if err != nil {
			return err
		}
		if observed.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			return errors.New("system installation image cleanup requires stopped containers")
		}
	}
	if err := r.client.NamespaceService().SetLabel(ctx, namespaceID, systemImageRetentionLabel, ""); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if err := r.pruneSystemImages(ctx, namespaceID, "", ""); err != nil {
		return err
	}
	r.wakeRuntimeGC()
	return nil
}
