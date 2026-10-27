//go:build linux

package control

import (
	"context"
	"errors"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	coreimages "github.com/containerd/containerd/v2/core/images"
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
	records, err := r.client.ImageService().List(runtimeContext(ctx))
	if err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Name != runtimeImageReference(record.Target.Digest) {
			continue
		}
		retained, err := r.runtimeImageRetained(ctx, record.Target.Digest)
		if err == nil && !retained {
			reference := record.Name
			err = r.client.ImageService().Delete(runtimeContext(ctx), reference, coreimages.SynchronousDelete())
			if errdefs.IsNotFound(err) {
				err = nil
			}
			if err == nil {
				inventory.removeRuntimeImage(reference)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
