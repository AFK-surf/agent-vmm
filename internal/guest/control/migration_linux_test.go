//go:build linux

package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

func TestMigrateLegacyNamespaceLabelsBootsWithCurrentQuotaContract(t *testing.T) {
	store := &migrationNamespaceStore{labels: legacyNamespaceLabels()}
	if err := migrateLegacyNamespaceLabels(t.Context(), store, "workload-a", store.labels); err != nil {
		t.Fatal(err)
	}
	projectID, namespace, err := namespaceFromLabels("workload-a", store.labels)
	if err != nil {
		t.Fatal(err)
	}
	if projectID != 10001 || namespace.GetLimits().GetCpuMillis() != 7000 || namespace.GetLimits().GetMemoryBytes() != 9_000_000_000 || namespace.GetLimits().GetPids() != 2048 || namespace.GetLimits().GetDiskBytes() != 8_000_000_000 {
		t.Fatalf("migrated namespace=%+v", namespace)
	}
	for key := range store.labels {
		if strings.HasPrefix(key, namespaceconfig.LegacyReservationPrefix) {
			t.Fatalf("retired namespace label remains: %s", key)
		}
	}
}

func TestMigrateLegacyNamespaceLabelsResumesAfterCanonicalWrite(t *testing.T) {
	store := &migrationNamespaceStore{labels: legacyNamespaceLabels()}
	for current, legacy := range map[string]string{"cpu": "cpu-max", "memory": "memory-max", "pids": "pid-max", "disk": "writable-quota"} {
		store.labels[labelPrefix+current] = store.labels[namespaceconfig.LegacyReservationPrefix+legacy]
	}
	delete(store.labels, namespaceconfig.LegacyReservationPrefix+"allocation")
	if err := migrateLegacyNamespaceLabels(t.Context(), store, "workload-a", store.labels); err != nil {
		t.Fatal(err)
	}
	if _, _, err := namespaceFromLabels("workload-a", store.labels); err != nil {
		t.Fatal(err)
	}
	for key := range store.labels {
		if strings.HasPrefix(key, namespaceconfig.LegacyReservationPrefix) {
			t.Fatalf("retired namespace label remains after resumed conversion: %s", key)
		}
	}
}

func TestDiscardMigratedSystemContainersKeepsWorkloadRootfs(t *testing.T) {
	system := &migrationContainer{labels: map[string]string{runtimeOwnerLabel: "system.appliance"}, info: containers.Container{ID: "system", Snapshotter: containerSnapshotter}}
	workload := &migrationContainer{labels: map[string]string{runtimeOwnerLabel: "workload-a"}, info: containers.Container{ID: "workload", Snapshotter: containerSnapshotter}}
	if err := discardMigratedSystemContainers(t.Context(), []containerd.Container{system, workload}); err != nil {
		t.Fatal(err)
	}
	if !system.deleted || workload.deleted {
		t.Fatalf("system deleted=%t workload deleted=%t", system.deleted, workload.deleted)
	}
}

func TestDiscardMigratedSystemSnapshotsRemovesChildrenFirst(t *testing.T) {
	root := runtimeSnapshotKey("system.appliance", "root")
	child := runtimeSnapshotKey("system.appliance", "child")
	snapshotter := &migrationSnapshotter{infos: []snapshots.Info{
		{Name: "sha256:shared"}, {Name: root}, {Name: child, Parent: root},
	}}
	if err := discardMigratedSystemSnapshots(t.Context(), snapshotter, "system.appliance"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(snapshotter.removed, ",") != child+","+root {
		t.Fatalf("removed snapshots=%v", snapshotter.removed)
	}
}

func legacyNamespaceLabels() map[string]string {
	return map[string]string{
		labelPrefix + "project-id": "10001", labelPrefix + "subnet": "10.64.1.0/24", labelPrefix + "egress": "1",
		namespaceconfig.LegacyReservationPrefix + "allocation":     "allocation-a",
		namespaceconfig.LegacyReservationPrefix + "generation":     "7",
		namespaceconfig.LegacyReservationPrefix + "cpu-guarantee":  "1000",
		namespaceconfig.LegacyReservationPrefix + "cpu-max":        "7000",
		namespaceconfig.LegacyReservationPrefix + "cpu-weight":     "100",
		namespaceconfig.LegacyReservationPrefix + "memory-high":    "8000000000",
		namespaceconfig.LegacyReservationPrefix + "memory-max":     "9000000000",
		namespaceconfig.LegacyReservationPrefix + "pid-max":        "2048",
		namespaceconfig.LegacyReservationPrefix + "writable-quota": "8000000000",
	}
}

type migrationNamespaceStore struct{ labels map[string]string }

func (*migrationNamespaceStore) Create(context.Context, string, map[string]string) error {
	return errors.New("unexpected create")
}
func (store *migrationNamespaceStore) Labels(context.Context, string) (map[string]string, error) {
	return store.labels, nil
}
func (store *migrationNamespaceStore) SetLabel(_ context.Context, _ string, key, value string) error {
	if value == "" {
		delete(store.labels, key)
	} else {
		store.labels[key] = value
	}
	return nil
}
func (*migrationNamespaceStore) List(context.Context) ([]string, error) { return nil, nil }
func (*migrationNamespaceStore) Delete(context.Context, string, ...namespaces.DeleteOpts) error {
	return errors.New("unexpected delete")
}

type migrationContainer struct {
	containerd.Container
	labels  map[string]string
	info    containers.Container
	deleted bool
}

func (container *migrationContainer) Labels(context.Context) (map[string]string, error) {
	return container.labels, nil
}
func (container *migrationContainer) Info(context.Context, ...containerd.InfoOpts) (containers.Container, error) {
	return container.info, nil
}
func (container *migrationContainer) Delete(context.Context, ...containerd.DeleteOpts) error {
	container.deleted = true
	return nil
}

type migrationSnapshotter struct {
	snapshots.Snapshotter
	infos   []snapshots.Info
	removed []string
}

func (snapshotter *migrationSnapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, _ ...string) error {
	for _, info := range snapshotter.infos {
		if err := fn(ctx, info); err != nil {
			return err
		}
	}
	return nil
}

func (snapshotter *migrationSnapshotter) Remove(_ context.Context, key string) error {
	snapshotter.removed = append(snapshotter.removed, key)
	return nil
}
