//go:build linux

package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/introspection"
	metadata "github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/core/mount"
	snapshotsapi "github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	digest "github.com/opencontainers/go-digest"
	specsVersion "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
)

type warmSnapshotIntrospection struct{ introspection.Service }

func (warmSnapshotIntrospection) Plugins(context.Context, ...string) (*introspectionapi.PluginsResponse, error) {
	return &introspectionapi.PluginsResponse{Plugins: []*introspectionapi.Plugin{{ID: containerSnapshotter, Type: "io.containerd.snapshotter.v1", Platforms: []*types.Platform{{OS: "linux", Architecture: "arm64"}}}}}, nil
}

func TestWarmOverlayPreparationPreservesActiveImportAllowance(t *testing.T) {
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable ext4 project-quota mount")
	}
	if err := PrepareOCIStorage(device); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	manager := semaphore.NewWeighted(1)
	err := manager.Acquire(ctx, 1)
	unlock := func() { manager.Release(1) }
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	root, err := os.MkdirTemp(filepath.Join(stateRoot, "containerd", "overlayfs"), "warm-budget-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	snapshots, err := overlay.NewSnapshotter(root)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshots.Close()
	mounts, err := snapshots.Prepare(ctx, "seed", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mounts[0].Source, "payload"), make([]byte, 4<<20), 0600); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Commit(ctx, digest.FromString("warm-layer").String(), "seed"); err != nil {
		t.Fatal(err)
	}
	before, err := projectUsage(device, ociProjectID)
	if err != nil {
		t.Fatal(err)
	}
	allowance := before + guestdisk.DefaultMetadataBytes + 64<<20
	if err := setProjectQuota(device, ociProjectID, allowance); err != nil {
		t.Fatal(err)
	}
	defer tightenOCIProjectQuota(device)
	cold := filepath.Join(root, "active-cold-writer")
	if err := os.WriteFile(cold, make([]byte, 8<<20), 0600); err != nil {
		t.Fatal(err)
	}
	baseline, err := projectUsage(device, ociProjectID)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"prepare", "view"} {
		var mounts []mount.Mount
		if kind == "prepare" {
			mounts, err = snapshots.Prepare(ctx, "warm-"+kind, digest.FromString("warm-layer").String())
		} else {
			mounts, err = snapshots.View(ctx, "warm-"+kind, digest.FromString("warm-layer").String())
		}
		if err != nil {
			t.Fatal(err)
		}
		usage, err := projectUsage(device, ociProjectID)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s shared project delta=%d bytes before writable reassignment", kind, usage-baseline)
		if usage-baseline > 1<<20 {
			t.Fatalf("warm snapshot copied bulk data: %d", usage-baseline)
		}
		if kind == "prepare" {
			if err := assignSnapshotMountProject(mounts, 10011); err != nil {
				t.Fatal(err)
			}
			if err := setProjectQuota(device, 10011, 32<<20); err != nil {
				t.Fatal(err)
			}
			if err := mount.WithTempMount(ctx, mounts, func(root string) error {
				return os.WriteFile(filepath.Join(root, "writable"), make([]byte, 4<<20), 0600)
			}); err != nil {
				t.Fatal(err)
			}
			owned, err := projectUsage(device, 10011)
			if err != nil || owned < 4<<20 {
				t.Fatalf("writable owner usage=%d err=%v", owned, err)
			}
		}
	}
	contentStore, err := local.NewStore(filepath.Join(root, "content"))
	if err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, Config: ocispec.ImageConfig{Cmd: []string{"true"}}, RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("warm-layer")}}})
	configDesc := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	manifest, _ := json.Marshal(ocispec.Manifest{Versioned: specsVersion.Versioned{SchemaVersion: 2}, Config: configDesc})
	target := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	for name, blob := range map[string][]byte{"config": config, "manifest": manifest} {
		if err := content.WriteBlob(ctx, contentStore, name, bytes.NewReader(blob), ocispec.Descriptor{Digest: digest.FromBytes(blob), Size: int64(len(blob))}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := bolt.Open(filepath.Join(root, "container-metadata.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	metadataDB := metadata.NewDB(db, contentStore, map[string]snapshotsapi.Snapshotter{containerSnapshotter: snapshots})
	if err := metadataDB.Init(ctx); err != nil {
		t.Fatal(err)
	}
	reference := "registry.example/warm:latest"
	record := &imagesapi.Image{Name: reference, Target: &types.Descriptor{MediaType: target.MediaType, Digest: target.Digest.String(), Size: target.Size}}
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{"warm-create": {reference: record}, runtimeContainerdNamespace: {runtimeImageReference(target.Digest): {Name: runtimeImageReference(target.Digest), Target: record.Target}}}, containers: map[string]*containersapi.Container{}}
	limits := &vmmdv1.NamespaceLimits{DiskBytes: 32 << 20, Pids: 64}
	facts := &operationNamespaces{labels: map[string]map[string]string{"warm-create": namespaceLabels(limits, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10011, "10.42.11.0/24")}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithIntrospectionService(warmSnapshotIntrospection{}), containerd.WithContainerStore(metadata.NewContainerStore(metadataDB)), containerd.WithLeasesService(metadata.NewLeaseManager(metadataDB)), containerd.WithContentStore(contentStore), containerd.WithSnapshotters(map[string]snapshotsapi.Snapshotter{containerSnapshotter: snapshots})))
	if _, err := os.Stat(supervisorHostPath); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(supervisorHostPath, []byte("fixture-supervisor"), 0755); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(supervisorHostPath)
	}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("warm-create")
	if err := index.addImage("warm-create", reference, target.Digest.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(runtimeImageReference(target.Digest))
	runtime := &Runtime{client: client, inventory: index, materializationSlot: manager, stateDevice: device, logger: slog.Default().With("component", "guest-control")}
	fingerprint := "configuration-fingerprint"
	created, err := runtime.CreateContainer(ctx, &vmmdv1.CreateContainerRequest{RequestId: "warm-create", ActorNamespaceId: "warm-create", ContainerId: "cached", Image: reference, Limits: &vmmdv1.ResourceLimits{}, LogLimitBytes: 4096, ConfigurationFingerprint: &fingerprint})
	if err != nil || created.Id != "cached" || created.CreateRequestSha256 == "" || created.GetConfigurationFingerprint() != fingerprint {
		t.Fatalf("occupied-slot cached create=%v err=%v", created, err)
	}
	observed, err := runtime.InspectContainer(ctx, "warm-create", "cached")
	if err != nil || observed.GetConfigurationFingerprint() != fingerprint {
		t.Fatalf("persisted configuration observation=%v err=%v", observed, err)
	}
	listed, err := runtime.ListContainers(ctx, "warm-create")
	if err != nil || len(listed) != 1 || listed[0].GetConfigurationFingerprint() != fingerprint {
		t.Fatalf("listed configuration observation=%v err=%v", listed, err)
	}
	if retained, err := runtime.runtimeImageRetained(ctx, target.Digest); err != nil || !retained {
		t.Fatalf("cached create lost namespace reference: %v %v", retained, err)
	}
	if removed, err := runtime.reclaimOCI(ctx, 32); err != nil || removed != 0 {
		t.Fatalf("cold writer allowed OCI GC: %d %v", removed, err)
	}
	fd, err := os.OpenFile(cold, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	if err := unix.Fallocate(int(fd.Fd()), 0, 8<<20, int64(guestdisk.DefaultMetadataBytes)+8<<20); err != nil {
		t.Fatalf("warm snapshot tightened active import allowance: %v", err)
	}
	if manager.TryAcquire(1) {
		manager.Release(1)
		t.Fatal("warm preparation released cold/GC exclusion")
	}
}
