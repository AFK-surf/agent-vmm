//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	containerd "github.com/containerd/containerd/v2/client"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

func (r *Runtime) completeLegacyStateMigration(ctx context.Context) error {
	marker := filepath.Join(controlRoot, namespaceconfig.LegacyStateMarker)
	if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	store := r.client.NamespaceService()
	names, err := store.List(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == runtimeContainerdNamespace {
			continue
		}
		labels, err := store.Labels(ctx, name)
		if err != nil {
			return err
		}
		if err := migrateLegacyNamespaceLabels(ctx, store, name, labels); err != nil {
			return err
		}
	}
	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return err
	}
	if err := discardMigratedSystemContainers(runtimeContext(ctx), containers); err != nil {
		return err
	}
	for _, name := range names {
		if !isSystemNamespace(name) {
			continue
		}
		nsctx := namespaceContext(ctx, name)
		images, err := r.client.ListImages(nsctx)
		if err != nil {
			return err
		}
		for _, image := range images {
			if err := r.client.ImageService().Delete(nsctx, image.Name(), coreimages.SynchronousDelete()); err != nil {
				return fmt.Errorf("discard migrated system image %s: %w", image.Name(), err)
			}
		}
	}
	for _, name := range names {
		if isSystemNamespace(name) {
			if err := discardMigratedSystemSnapshots(runtimeContext(ctx), r.client.SnapshotService(containerSnapshotter), name); err != nil {
				return err
			}
		}
	}
	if err := syncStateFilesystem(); err != nil {
		return err
	}
	if err := os.Remove(marker); err != nil {
		return err
	}
	return syncFilesystem(controlRoot)
}

func discardMigratedSystemContainers(ctx context.Context, containers []containerd.Container) error {
	for _, container := range containers {
		labels, err := container.Labels(ctx)
		if err != nil {
			return err
		}
		if !isSystemNamespace(labels[runtimeOwnerLabel]) {
			continue
		}
		info, err := container.Info(ctx)
		if err != nil {
			return err
		}
		if info.Snapshotter != containerSnapshotter {
			return fmt.Errorf("migrated system container %s uses unexpected snapshotter %q", info.ID, info.Snapshotter)
		}
		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
			return fmt.Errorf("discard migrated system container %s: %w", info.ID, err)
		}
	}
	return nil
}

func discardMigratedSystemSnapshots(ctx context.Context, snapshotter snapshots.Snapshotter, namespaceID string) error {
	prefix := runtimeSnapshotKey(namespaceID, "")
	selected := make(map[string]snapshots.Info)
	if err := snapshotter.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if !strings.HasPrefix(info.Name, prefix) {
			return nil
		}
		if len(selected) >= 1024 {
			return errors.New("migrated system snapshot count exceeds recovery bound")
		}
		selected[info.Name] = info
		return nil
	}); err != nil {
		return err
	}
	var ordered []snapshots.Info
	for _, info := range selected {
		ordered = append(ordered, info)
	}
	depth := func(info snapshots.Info) int {
		result := 0
		seen := make(map[string]bool)
		for parent := info.Parent; parent != "" && selected[parent].Name != "" && !seen[parent]; parent = selected[parent].Parent {
			seen[parent] = true
			result++
		}
		return result
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := depth(ordered[i]), depth(ordered[j])
		if left != right {
			return left > right
		}
		return ordered[i].Name < ordered[j].Name
	})
	for _, info := range ordered {
		if err := snapshotter.Remove(ctx, info.Name); err != nil {
			return fmt.Errorf("discard migrated system snapshot %s: %w", info.Name, err)
		}
	}
	return nil
}

func migrateLegacyNamespaceLabels(ctx context.Context, store namespaces.Store, name string, labels map[string]string) error {
	converted, changed, err := namespaceconfig.ConvertLegacy(labels)
	if err != nil {
		return fmt.Errorf("convert namespace %s labels: %w", name, err)
	}
	if !changed {
		return nil
	}
	for _, key := range []string{"cpu", "memory", "pids", "disk"} {
		if err := store.SetLabel(ctx, name, labelPrefix+key, converted[labelPrefix+key]); err != nil {
			return err
		}
	}
	var retired []string
	for key := range labels {
		if strings.HasPrefix(key, namespaceconfig.LegacyReservationPrefix) {
			retired = append(retired, key)
		}
	}
	sort.Strings(retired)
	allocation := namespaceconfig.LegacyReservationPrefix + "allocation"
	for _, key := range retired {
		if key == allocation {
			continue
		}
		if err := store.SetLabel(ctx, name, key, ""); err != nil {
			return err
		}
	}
	if labels[allocation] != "" {
		if err := store.SetLabel(ctx, name, allocation, ""); err != nil {
			return err
		}
	}
	return nil
}
