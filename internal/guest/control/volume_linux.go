//go:build linux

package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
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
	if requestedMode != nil && *requestedMode & ^uint32(0o1777) != 0 {
		return nil, fmt.Errorf("mode must contain only permission bits and the sticky bit")
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
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
	projectID, namespace, err := namespaceFromLabels(namespaceID, labels)
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
	generationID, err := volumeGenerationID(path)
	if err != nil {
		return nil, err
	}
	return &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID, LimitBytes: namespace.GetLimits().GetDiskBytes(), GenerationId: generationID}, nil
}

func initializeVolumeDirectory(path string, mode uint32, ownerUID, ownerGID *uint32, assign func(string) error) (result error) {
	staging, err := os.MkdirTemp(filepath.Dir(path), ".agent-vmm-volume-")
	if err != nil {
		return err
	}
	markerCreated := false
	defer func() {
		_ = os.RemoveAll(staging)
		if result != nil && markerCreated {
			_ = os.Remove(volumeGenerationMarkerPath(path))
		}
	}()
	if ownerUID != nil {
		if err := os.Chown(staging, int(*ownerUID), int(*ownerGID)); err != nil {
			return err
		}
	}
	fileMode := os.FileMode(mode & 0o777)
	if mode&0o1000 != 0 {
		fileMode |= os.ModeSticky
	}
	if err := os.Chmod(staging, fileMode); err != nil {
		return err
	}
	if err := assign(staging); err != nil {
		return err
	}
	if _, err := replaceVolumeGenerationID(path); err != nil {
		return err
	}
	markerCreated = true
	if err := os.Rename(staging, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func cleanupVolumeStaging() error {
	for _, path := range []string{stateRoot, controlRoot} {
		if err := cleanupVolumeStagingAt(filepath.Join(path, "volumes")); err != nil {
			return err
		}
	}
	return nil
}

func cleanupVolumeStagingAt(root string) error {
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

func (r *Runtime) DeleteVolume(ctx context.Context, namespaceID, volumeID, expectedGenerationID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	if err := validateID("volume_id", volumeID); err != nil {
		return err
	}
	unlock, err := r.lockVolumeGeneration(ctx, namespaceID, volumePath(namespaceID, volumeID), expectedGenerationID)
	if err != nil {
		return err
	}
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
			if mount.GetOwnedVolume() == volumeID {
				return failedPrecondition("volume is referenced by a container")
			}
		}
	}
	if err := removeVolumeDirectory(volumePath(namespaceID, volumeID)); err != nil {
		return err
	}
	r.invalidateVolumeUsage(namespaceID, volumeID)
	return nil
}

func (r *Runtime) lockVolumeGeneration(ctx context.Context, namespaceID, path, expectedGenerationID string) (func(), error) {
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	if expectedGenerationID != "" {
		if err := requireVolumeGeneration(path, expectedGenerationID); err != nil {
			unlock()
			return nil, err
		}
	}
	return unlock, nil
}

func removeVolumeDirectory(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := os.Remove(volumeGenerationMarkerPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func volumeGenerationMarkerPath(path string) string {
	namespaceID := filepath.Base(filepath.Dir(path))
	volumeID := filepath.Base(path)
	state := filepath.Dir(filepath.Dir(filepath.Dir(path)))
	return filepath.Join(state, "volume-generations", namespaceID, volumeID)
}

func replaceVolumeGenerationID(path string) (string, error) {
	marker := volumeGenerationMarkerPath(path)
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		return "", err
	}
	value, err := newVolumeGenerationID()
	if err != nil {
		return "", err
	}
	if err := atomicfile.ReplaceFile(marker, []byte(value+"\n"), 0600); err != nil {
		return "", err
	}
	return value, nil
}

func backfillVolumeGenerationID(path string, before os.FileInfo) (string, error) {
	marker := volumeGenerationMarkerPath(path)
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		return "", err
	}
	value, err := newVolumeGenerationID()
	if err != nil {
		return "", err
	}
	if err := atomicfile.PublishOnce(marker, []byte(value+"\n"), 0600); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return readVolumeGenerationID(marker)
		}
		return "", err
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		if err != nil {
			return "", err
		}
		return "", errors.New("volume changed while its generation marker was created")
	}
	return value, nil
}

func readVolumeGenerationID(marker string) (string, error) {
	body, err := os.ReadFile(marker)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(body))
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 || len(value) != 32 {
		return "", errors.New("volume generation marker is invalid")
	}
	return value, nil
}

func newVolumeGenerationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func requireVolumeGeneration(path, expectedGenerationID string) error {
	actualGenerationID, err := volumeGenerationID(path)
	if err != nil {
		return err
	}
	if actualGenerationID != expectedGenerationID {
		return failedPrecondition("volume generation changed")
	}
	return nil
}

func volumeGenerationID(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("volume incarnation is unavailable")
	}
	value, err := readVolumeGenerationID(volumeGenerationMarkerPath(path))
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return backfillVolumeGenerationID(path, info)
}
