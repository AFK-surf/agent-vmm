//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
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
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	digest "github.com/opencontainers/go-digest"
	specsVersion "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// Uses the privileged startup test's disposable ext4 mount and the same
// containerd implementations that create production content and snapshots.
func seedOCIInheritance(t *testing.T) map[string]uint32 {
	t.Helper()
	ctx := context.Background()
	var layer bytes.Buffer
	writer := tar.NewWriter(&layer)
	for _, header := range []*tar.Header{
		{Name: "etc", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "etc/content", Typeflag: tar.TypeReg, Mode: 0o644, Size: 4},
		{Name: "etc/hardlink", Typeflag: tar.TypeLink, Linkname: "etc/content"},
		{Name: "absolute", Typeflag: tar.TypeSymlink, Linkname: "/usr/bin/oci-missing"},
		{Name: "relative", Typeflag: tar.TypeSymlink, Linkname: "etc/content"},
		{Name: "dangling", Typeflag: tar.TypeSymlink, Linkname: "missing"},
		{Name: "fifo", Typeflag: tar.TypeFifo, Mode: 0o600},
		{Name: "null", Typeflag: tar.TypeChar, Mode: 0o600, Devmajor: 1, Devminor: 3},
	} {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	storeRoot := filepath.Join(containerdRoot, "io.containerd.content.v1.content")
	store, err := local.NewStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	desc := ocispec.Descriptor{Digest: digest.FromBytes(layer.Bytes()), Size: int64(layer.Len()), MediaType: ocispec.MediaTypeImageLayer}
	if err := content.WriteBlob(ctx, store, "inheritance", bytes.NewReader(layer.Bytes()), desc); err != nil {
		t.Fatal(err)
	}
	paths := []string{filepath.Join(storeRoot, "blobs", "sha256", desc.Digest.Encoded())}
	unfinished, err := store.Writer(ctx, content.WithRef("interrupted-ingest"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unfinished.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := unfinished.Close(); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, filepath.Join(storeRoot, "ingest", digest.FromString("interrupted-ingest").Encoded(), "data"))

	snapshotter, err := overlay.NewSnapshotter(filepath.Join(containerdRoot, "io.containerd.snapshotter.v1.overlayfs"))
	if err != nil {
		t.Fatal(err)
	}
	defer snapshotter.Close()
	interruptedMounts, err := snapshotter.Prepare(ctx, "interrupted-snapshot", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(interruptedMounts) != 1 || interruptedMounts[0].Type != "bind" {
		t.Fatalf("unexpected interrupted snapshot: %v", interruptedMounts)
	}
	partial := filepath.Join(interruptedMounts[0].Source, "partial-layer")
	if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, interruptedMounts[0].Source, partial)

	mounts, err := snapshotter.Prepare(ctx, "lower", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 1 || mounts[0].Type != "bind" {
		t.Fatalf("unexpected lower mounts: %v", mounts)
	}
	lower := mounts[0].Source
	if _, err := archive.Apply(ctx, lower, bytes.NewReader(layer.Bytes())); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, lower, filepath.Join(lower, "etc"), filepath.Join(lower, "etc", "content"), filepath.Join(lower, "etc", "hardlink"))
	if err := snapshotter.Commit(ctx, "committed", "lower"); err != nil {
		t.Fatal(err)
	}
	mounts, err = snapshotter.Prepare(ctx, "writable", "committed")
	if err != nil {
		t.Fatal(err)
	}
	var upper, work string
	for _, mount := range mounts {
		for _, option := range mount.Options {
			if value, ok := strings.CutPrefix(option, "workdir="); ok {
				work = value
			}
			if value, ok := strings.CutPrefix(option, "upperdir="); ok {
				upper = value
			}
		}
	}
	if upper == "" {
		t.Fatal("missing writable upper")
	}
	if err := assignSnapshotMountProject(mounts, 10002); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(upper, "workload-data")
	if err := os.WriteFile(child, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	if id, err := projectID(child); err != nil || id != 10002 {
		t.Fatalf("writable child project=%d err=%v", id, err)
	}
	linked, err := os.Stat(filepath.Join(lower, "etc", "hardlink"))
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(filepath.Join(lower, "etc", "content"))
	if err != nil || !os.SameFile(original, linked) {
		t.Fatalf("layer hardlink lost inode identity: %v", err)
	}
	zero := filepath.Join(stateRoot, "unowned-source")
	if err := os.WriteFile(zero, []byte("unowned"), 0o600); err != nil {
		t.Fatal(err)
	}
	for destination, source := range map[string]string{
		filepath.Join(upper, "foreign-lower"): filepath.Join(lower, "etc", "content"),
		filepath.Join(lower, "foreign-zero"):  zero,
	} {
		if err := os.Link(source, destination); !errors.Is(err, unix.EXDEV) {
			t.Fatalf("cross-project hardlink error=%v", err)
		}
		if err := os.Rename(source, destination); !errors.Is(err, unix.EXDEV) {
			t.Fatalf("cross-project rename error=%v", err)
		}
	}
	renamed := filepath.Join(lower, "etc", "renamed-content")
	if err := os.Rename(filepath.Join(lower, "etc", "hardlink"), renamed); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(renamed, filepath.Join(lower, "etc", "hardlink")); err != nil {
		t.Fatal(err)
	}
	if id, err := projectID(zero); err != nil || id != 0 {
		t.Fatalf("external source project=%d err=%v", id, err)
	}
	merged := t.TempDir()
	if err := mount.All(mounts, merged); err != nil {
		t.Fatal(err)
	}
	defer mount.UnmountAll(merged, 0)
	if err := os.WriteFile(filepath.Join(merged, "etc", "content"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(lower, "etc", "content")); err != nil || string(data) != "data" {
		t.Fatalf("copy-up changed lower: %q %v", data, err)
	}
	if err := mount.UnmountAll(merged, 0); err != nil {
		t.Fatal(err)
	}
	if err := mount.All(mounts, merged); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(merged, "etc", "after-remount"), []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	projects := map[string]uint32{work: 10002, filepath.Join(upper, "etc", "after-remount"): 10002, upper: 10002, child: 10002, filepath.Join(upper, "etc"): 10002, filepath.Join(upper, "etc", "content"): 10002}
	for _, path := range paths {
		projects[path] = ociProjectID
	}
	for path, want := range projects {
		if id, err := projectID(path); err != nil || id != want {
			t.Fatalf("OCI creator project path=%s id=%d err=%v", path, id, err)
		}
	}
	return projects
}

// Exercise the build workspace owner and its actual context extractor. The
// full VM Dockerfile E2E covers builder execution and output import into CAS.
func seedBuildInheritance(t *testing.T) (map[string]uint32, func()) {
	t.Helper()
	// Namespace admission owns build-tmp and charges it to the writable budget.
	root := filepath.Join(stateRoot, "build-tmp", "inheritance")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := assignProject(root, 10003); err != nil {
		t.Fatal(err)
	}
	builder := guestbuild.NewBuilder(nil, nil, nil)
	workspace, err := builder.Begin(&vmmdv1.BuildImageHeader{RequestId: "inheritance", ActorNamespaceId: "inheritance", DockerfilePath: "Dockerfile", Image: "agent.local/inheritance:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE})
	if err != nil {
		t.Fatal(err)
	}
	closeWorkspace := func() { _ = workspace.Close() }
	var contextArchive bytes.Buffer
	writer := tar.NewWriter(&contextArchive)
	data := []byte("FROM scratch\n")
	if err := writer.WriteHeader(&tar.Header{Name: "Dockerfile", Mode: 0o600, Size: int64(len(data))}); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if err := workspace.AppendContext(contextArchive.Bytes(), true); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	roots, err := filepath.Glob(filepath.Join(stateRoot, "build-tmp", "inheritance", "*"))
	if err != nil || len(roots) != 1 {
		closeWorkspace()
		t.Fatalf("build workspace roots=%v err=%v", roots, err)
	}
	projects := make(map[string]uint32)
	for _, suffix := range []string{"", "context.tar", "context", "context/Dockerfile", "output", "output/tmp", "output/home", "output/buildkit"} {
		path := filepath.Join(roots[0], suffix)
		if id, err := projectID(path); err != nil || id != 10003 {
			closeWorkspace()
			t.Fatalf("build project path=%s id=%d err=%v", path, id, err)
		}
		projects[path] = 10003
	}
	return projects, closeWorkspace
}

func TestWritableSnapshotRootSupportsNativeAndOverlaySnapshots(t *testing.T) {
	tests := []struct {
		name   string
		mounts []mount.Mount
		want   string
	}{
		{
			name:   "single-layer",
			mounts: []mount.Mount{{Type: "bind", Source: "/state/overlay/active"}},
			want:   "/state/overlay/active",
		},
		{
			name: "overlay",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"lowerdir=/pool/lower", "upperdir=/pool/upper", "workdir=/pool/work"},
			}},
			want: "/pool/upper",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := writableSnapshotRoot(test.mounts)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("writableSnapshotRoot()=%q, want %q", got, test.want)
			}
		})
	}
}

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
	manager := new(ocimaterializer.Manager)
	unlock, err := manager.LockBuild(ctx)
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
	runtime := &Runtime{client: client, inventory: index, materializer: manager, stateDevice: device}
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
	if release, acquired := manager.TryLockBuild(); acquired {
		release()
		t.Fatal("warm preparation released cold/GC exclusion")
	}
}
