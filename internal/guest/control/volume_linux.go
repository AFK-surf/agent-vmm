//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/errdefs"
)

func (r *Runtime) CreateVolume(ctx context.Context, namespaceID, volumeID string, ownerUID, ownerGID, requestedMode *uint32) (*vmmdv1.Volume, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return nil, err
	}
	if (ownerUID == nil) != (ownerGID == nil) {
		return nil, fmt.Errorf("owner_uid and owner_gid must be provided together")
	}
	if requestedMode != nil && *requestedMode > 0o777 {
		return nil, fmt.Errorf("mode must contain only user, group, and other permission bits")
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
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("%w: volume already exists", errdefs.ErrAlreadyExists)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	mode := uint32(0o700)
	if requestedMode != nil {
		mode = *requestedMode
	}
	if err := initializeVolumeDirectory(path, mode, ownerUID, ownerGID, func(path string) error { return assignProject(path, projectID) }); err != nil {
		return nil, err
	}
	r.invalidateVolumeUsage(namespaceID, volumeID)
	return &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID}, nil
}

func initializeVolumeDirectory(path string, mode uint32, ownerUID, ownerGID *uint32, assign func(string) error) (result error) {
	staging, err := os.MkdirTemp(filepath.Dir(path), ".agent-vmm-volume-")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(staging)
	}()
	if err := os.Chmod(staging, os.FileMode(mode)); err != nil {
		return err
	}
	if ownerUID != nil {
		if err := os.Chown(staging, int(*ownerUID), int(*ownerGID)); err != nil {
			return err
		}
	}
	if err := assign(staging); err != nil {
		return err
	}
	return os.Rename(staging, path)
}

func cleanupVolumeStaging() error {
	root := filepath.Join(stateRoot, "volumes")
	namespaces, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, namespace := range namespaces {
		if !namespace.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, namespace.Name()))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".agent-vmm-volume-") {
				if err := os.RemoveAll(filepath.Join(root, namespace.Name(), entry.Name())); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
				return failedPrecondition("volume is referenced by a container")
			}
		}
	}
	if err := os.RemoveAll(volumePath(namespaceID, volumeID)); err != nil {
		return err
	}
	r.invalidateVolumeUsage(namespaceID, volumeID)
	return nil
}
