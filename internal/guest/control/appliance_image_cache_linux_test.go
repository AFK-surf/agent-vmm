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
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/metadata"
	snapshotsapi "github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/native"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBundledSystemImageUsesOldCacheBeforeCurrentArchive(t *testing.T) {
	if os.Getenv("AGENT_VMM_SYSTEM_IMAGE_CACHE_TEST") != "1" {
		t.Skip("requires a disposable privileged container")
	}
	if _, err := os.Stat(bundledMCPPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture requires an absent bundled archive", err)
	}
	ctx := context.Background()
	nsctx := runtimeContext(ctx)
	root := t.TempDir()
	physical, err := native.NewSnapshotter(filepath.Join(root, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	defer physical.Close()
	localContent, err := local.NewStore(filepath.Join(root, "content"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(root, "metadata.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	metadataDB := metadata.NewDB(db, localContent, map[string]snapshotsapi.Snapshotter{containerSnapshotter: physical})
	if err := metadataDB.Init(ctx); err != nil {
		t.Fatal(err)
	}
	snapshots, contentStore := metadataDB.Snapshotter(containerSnapshotter), metadataDB.ContentStore()
	mounts, err := snapshots.Prepare(nsctx, "cached-system-root", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"opt/agent-vmm/content", "run/agent-vmm", "var/lib/agent-vmm", "etc"} {
		if err := os.MkdirAll(filepath.Join(mounts[0].Source, path), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"etc/resolv.conf", strings.TrimPrefix(supervisorPath, "/")} {
		if err := os.WriteFile(filepath.Join(mounts[0].Source, path), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}
	layer := digest.FromString("old-system-root")
	if err := snapshots.Commit(nsctx, layer.String(), "cached-system-root"); err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{layer}}})
	configDesc := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	manifest, _ := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, Config: configDesc})
	target := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	for name, blob := range map[string]struct {
		data       []byte
		descriptor ocispec.Descriptor
	}{"config": {config, configDesc}, "manifest": {manifest, target}} {
		if err := content.WriteBlob(nsctx, contentStore, name, bytes.NewReader(blob.data), blob.descriptor); err != nil {
			t.Fatal(err)
		}
	}
	namespace := "system.cached"
	facts := &operationNamespaces{labels: map[string]map[string]string{namespace: namespaceLabels(&vmmdv1.NamespaceLimits{DiskBytes: 32 << 20, Pids: 64}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10100, "10.42.100.0/24")}}
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithIntrospectionService(warmSnapshotIntrospection{}), containerd.WithImageStore(metadata.NewImageStore(metadataDB)), containerd.WithLeasesService(metadata.NewLeaseManager(metadataDB)), containerd.WithContentStore(contentStore), containerd.WithSnapshotters(map[string]snapshotsapi.Snapshotter{containerSnapshotter: snapshots})))
	canonical := runtimeImageReference(target.Digest)
	if _, err := client.ImageService().Create(nsctx, coreimages.Image{Name: canonical, Target: target}); err != nil {
		t.Fatal(err)
	}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(namespace)
	index.addRuntimeImage(canonical)
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	reference := bundledMCPImage + target.Digest.String()
	for range 2 {
		got, resolved, size, err := runtime.importBundledSystemImage(ctx, namespace, reference)
		if err != nil || got != reference || resolved != target.Digest.String() || size == 0 {
			t.Fatalf("cached controller: %s %s %d %v", got, resolved, size, err)
		}
	}
	coldHeader := &vmmdv1.ImportImageHeader{RequestId: bundledImportRequestID(reference), ActorNamespaceId: namespace,
		ImageReference: reference, ManifestDigest: target.Digest.String(), Platform: "linux/arm64", ArchiveSize: 4096}
	coldFingerprint, err := importFingerprint(coldHeader)
	if err != nil {
		t.Fatal(err)
	}
	coldReceipt := importRequestValue(coldHeader.RequestId, coldFingerprint)
	logicalContext := namespaceContext(ctx, namespace)
	logical, err := client.ImageService().Get(logicalContext, reference)
	if err != nil {
		t.Fatal(err)
	}
	logical.Labels = map[string]string{importRequestLabel: coldReceipt}
	if _, err := client.ImageService().Update(logicalContext, logical, "labels"); err != nil {
		t.Fatal(err)
	}
	if got, resolved, size, err := runtime.importBundledSystemImage(ctx, namespace, reference); err != nil || got != reference || resolved != target.Digest.String() || size == 0 {
		t.Fatalf("cached controller with cold receipt: %s %s %d %v", got, resolved, size, err)
	}
	logical, err = client.ImageService().Get(logicalContext, reference)
	if err != nil || logical.Labels[importRequestLabel] != coldReceipt {
		t.Fatalf("cold import receipt changed: %v %v", logical.Labels, err)
	}
	if !index.imageDigestReferenced(target.Digest.String()) {
		t.Fatal("cached import did not grant namespace accounting")
	}
	if err := client.ImageService().Delete(namespaceContext(ctx, namespace), reference); err != nil {
		t.Fatal(err)
	}
	if err := client.ImageService().Delete(nsctx, canonical); err != nil {
		t.Fatal(err)
	}
	for _, currentArchive := range []bool{false, true} {
		if currentArchive {
			archive, _ := testOCIArchive(t, nil, ocispec.Platform{OS: "linux", Architecture: "arm64"})
			if err := os.MkdirAll(filepath.Dir(bundledMCPPath), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(bundledMCPPath, archive, 0644); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Remove(bundledMCPPath) })
		}
		_, _, _, err := runtime.importBundledSystemImage(ctx, namespace, reference)
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "update") {
			t.Fatalf("missing old source: %v", err)
		}
	}
}
