//go:build linux

package control

import (
	"context"
	"encoding/json"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
)

// Convert the owned-volume label while rebuilding the existing inventory.
func convertContainerMountLabel(ctx context.Context, container containerd.Container, labels map[string]string) error {
	value := labels[containerVolumeLabel]
	if value != "null" && !strings.HasPrefix(value, "[") {
		return nil
	}
	var previous []struct {
		VolumeID    string `json:"volume_id"`
		Destination string `json:"destination"`
		ReadOnly    bool   `json:"read_only"`
	}
	if err := json.Unmarshal([]byte(value), &previous); err != nil {
		return err
	}
	mounts := make([]*vmmdv1.VolumeMount, 0, len(previous))
	for _, mount := range previous {
		mounts = append(mounts, &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: mount.VolumeID}, Destination: mount.Destination, ReadOnly: mount.ReadOnly})
	}
	encoded, err := encodeContainerMounts(mounts)
	if err != nil {
		return err
	}
	_, err = container.SetLabels(ctx, map[string]string{containerVolumeLabel: encoded})
	if err == nil {
		labels[containerVolumeLabel] = encoded
	}
	return err
}
