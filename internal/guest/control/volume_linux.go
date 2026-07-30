//go:build linux

package control

import (
	"context"
	"errors"
	"os"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func (r *Runtime) CreateVolume(ctx context.Context, namespaceID, volumeID string) (*vmmdv1.Volume, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return nil, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	projectID, _, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	path := volumePath(namespaceID, volumeID)
	if err := ensureDirectory(path, 0o700); err != nil {
		return nil, err
	}
	if err := assignProject(path, projectID); err != nil {
		return nil, err
	}
	return &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID}, nil
}

func (r *Runtime) DeleteVolume(ctx context.Context, namespaceID, volumeID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return err
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return err
	}
	for _, container := range containers {
		for _, mount := range container.GetVolumes() {
			if mount.GetVolumeId() == volumeID {
				return errors.New("volume is referenced by a container")
			}
		}
	}
	return os.RemoveAll(volumePath(namespaceID, volumeID))
}
