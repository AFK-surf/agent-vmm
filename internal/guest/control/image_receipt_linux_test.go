//go:build linux

package control

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	coreimages "github.com/containerd/containerd/v2/core/images"
	metadata "github.com/containerd/containerd/v2/core/metadata"
	snapshotsapi "github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	specsVersion "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestImageOperationReceiptBindsImmutableContent(t *testing.T) {
	record := coreimages.Image{Target: ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("a", 64))}}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "original-build", InputSha256: strings.Repeat("b", 64)}
	if err := setImageOperationReceipt(&record, "build", receipt); err != nil {
		t.Fatal(err)
	}
	got := imageOperationReceipt(record, "build")
	if got == nil || got.GetRequestId() != receipt.GetRequestId() || got.GetInputSha256() != receipt.GetInputSha256() {
		t.Fatalf("receipt changed: %v", got)
	}
	if imageOperationReceipt(record, "copy") != nil {
		t.Fatal("build receipt became copy provenance")
	}
	record.Target.Digest = digest.Digest("sha256:" + strings.Repeat("c", 64))
	if imageOperationReceipt(record, "build") != nil {
		t.Fatal("receipt survived content replacement")
	}
}

func TestRetainedBuildSurvivesSourceReleaseAndScopesCollection(t *testing.T) {
	ctx := context.Background()
	target := digest.FromString("successful-build")
	reference := "docker.io/library/fixture:latest"
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{
		"preparation":              {reference: {Name: reference, Target: &types.Descriptor{Digest: target.String()}}},
		runtimeContainerdNamespace: {runtimeImageReference(target): {Name: runtimeImageReference(target), Target: &types.Descriptor{Digest: target.String()}}},
	}, containers: map[string]*containersapi.Container{}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("preparation")
	if err := index.addImage("preparation", reference, target.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(runtimeImageReference(target))
	facts := &operationNamespaces{labels: map[string]map[string]string{}}
	runtime := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts))), inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "build", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD,
		ImageReference: reference, ManifestDigest: target.String(), State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED}
	if err := runtime.retainBuiltImage(ctx, "controller-a", "preparation", receipt); err == nil {
		t.Fatal("unsettled writer granted retention")
	}
	receipt.WriterSettled = true
	if err := runtime.retainBuiltImage(ctx, "controller-a", "preparation", receipt); err != nil {
		t.Fatal(err)
	}
	if err := runtime.retainBuiltImage(ctx, "controller-a", "preparation", receipt); err != nil {
		t.Fatal(err)
	}
	second := proto.Clone(receipt).(*vmmdv1.ImageOperationReceipt)
	second.RequestId = "second-build"
	if err := runtime.retainBuiltImage(ctx, "controller-a", "preparation", second); err != nil {
		t.Fatal(err)
	}
	delete(fixture.images, "preparation")
	if err := index.removeImage("preparation", reference); err != nil {
		t.Fatal(err)
	}
	if err := runtime.rebuildRuntimeInventory(ctx); err != nil {
		t.Fatal(err)
	}

	record, err := runtime.retainedImage(ctx, "controller-a", reference, target.String(), receipt.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.retainedImage(ctx, "controller-b", reference, target.String(), receipt.RequestId); err == nil {
		t.Fatal("other controller saw retained image")
	}
	if _, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil {
		t.Fatal(err)
	}
	if fixture.images[runtimeContainerdNamespace][runtimeImageReference(target)] == nil {
		t.Fatal("source release collected retained content")
	}
	listed, err := runtime.listRetainedImages(ctx, "controller-a", 0)
	if err != nil || len(listed) != 2 {
		t.Fatalf("retained provenance: %v %v", listed, err)
	}
	if err := runtime.deleteRetainedImage(ctx, "controller-a", reference, "stale", target.String()); err == nil {
		t.Fatal("stale generation deleted retention")
	}
	secondRecord, err := runtime.retainedImage(ctx, "controller-a", reference, target.String(), second.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	if imageOperationReceipt(record, "build").RequestId != "build" || imageOperationReceipt(secondRecord, "build").RequestId != "second-build" {
		t.Fatal("same-content build replaced original operation proof")
	}
	copyReceipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, InputSha256: "input", ImageReference: reference, ManifestDigest: target.String()}
	run, _, err := runtime.acceptImageOperation(ctx, "target", copyReceipt, record.Name, func() {})
	if err != nil {
		t.Fatal(err)
	}
	copyReceipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
	if err := runtime.saveImageOperation(ctx, "target", copyReceipt, ""); err != nil {
		t.Fatal(err)
	}
	runtime.imageOperations.Delete(imageOperationRecordKey("target", "copy", copyReceipt.Kind))
	facts.mu.Lock()
	facts.labelReads = 0
	facts.mu.Unlock()
	if _, err := runtime.reclaimRetainedImageCache(ctx, 32); err != nil {
		t.Fatal(err)
	}
	facts.mu.Lock()
	reads := facts.labelReads
	facts.mu.Unlock()
	if reads != 1 {
		t.Fatalf("one controller scope scanned image receipts %d times", reads)
	}
	if _, err := runtime.retainedImage(ctx, "controller-a", reference, target.String(), "build"); err != nil {
		t.Fatal("lost handler released source pin", err)
	}
	if _, err := runtime.retainedImage(ctx, "controller-a", reference, target.String(), "second-build"); err == nil {
		t.Fatal("idle cache stayed pinned")
	}
	if err := runtime.finishImageOperation("target", copyReceipt, run, context.Canceled, nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.reclaimRetainedImageCache(ctx, 32); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil {
		t.Fatal(err)
	}

	if fixture.images[runtimeContainerdNamespace][runtimeImageReference(target)] != nil {
		t.Fatal("unreferenced physical content stayed retained")
	}
}

func TestRetainedCacheCollectionReadsCopyPinAfterScopeAdmission(t *testing.T) {
	ctx := context.Background()
	target := digest.FromString("retained-interleave")
	reference := "docker.io/library/fixture:latest"
	record := retainedImageReference("controller", reference, target.String(), "build")
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{runtimeContainerdNamespace: {record: {Name: record, Target: &types.Descriptor{Digest: target.String()}, Labels: map[string]string{retainedScopeLabel: "controller", retainedReferenceLabel: reference}}}}, containers: map[string]*containersapi.Container{}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(retainedImageOwner("controller"))
	if err := index.addImage(retainedImageOwner("controller"), record, target.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(record)
	runtime := &Runtime{client: systemImageClient(t, fixture), inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	var accepted sync.Once
	fixture.mu.Lock()
	fixture.beforeGet = func(name string) {
		if name != record {
			return
		}
		accepted.Do(func() {
			unlock, lockErr := runtime.lockNamespace(context.Background(), retainedImageOwner("controller"))
			if lockErr != nil {
				t.Fatal(lockErr)
			}
			defer unlock()
			receipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy-before-gc-lock", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ImageReference: reference, ManifestDigest: target.String(), InputSha256: strings.Repeat("a", 64)}
			if _, _, err := runtime.acceptImageOperation(ctx, "target", receipt, record, func() {}); err != nil {
				t.Error(err)
			}
		})
	}
	fixture.mu.Unlock()
	if count, err := runtime.reclaimRetainedImageCache(ctx, 32); err != nil || count != 0 {
		t.Fatalf("collection crossed accepted copy pin: %d %v", count, err)
	}
}

func TestRetainedCopyMissingCacheCannotHideOriginalAdmission(t *testing.T) {
	ctx := context.Background()
	namespaces := &operationNamespaces{labels: map[string]map[string]string{}}
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{runtimeContainerdNamespace: {}}, containers: map[string]*containersapi.Container{}}
	runtime := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(namespaces))), inventory: newRuntimeInventoryIndex(), materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	server := &Server{runtime: runtime}
	request := &vmmdv1.CopyImageRequest{RequestId: "copy", TargetNamespaceId: "target", ControllerCacheScope: "controller", RetainedBuildRequestId: "build", ImageReference: "docker.io/library/fixture:latest", ManifestDigest: digest.FromString("missing").String(), ControllerRequestSha256: strings.Repeat("a", 64)}
	fixture.images["preparation"] = map[string]*imagesapi.Image{request.ImageReference: {Name: request.ImageReference, Target: &types.Descriptor{Digest: request.ManifestDigest}}}
	built := &vmmdv1.ImageOperationReceipt{RequestId: "build", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, ImageReference: request.ImageReference, ManifestDigest: request.ManifestDigest, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true}
	if err := runtime.retainBuiltImage(ctx, "controller", "preparation", built); err != nil {
		t.Fatal(err)
	}
	listed, err := runtime.listRetainedImages(ctx, "controller", 0)
	if err != nil || len(listed) != 1 {
		t.Fatalf("cache probe: %v %v", listed, err)
	}
	if removed, err := runtime.reclaimRetainedImageCache(ctx, 32); err != nil || removed != 1 {
		t.Fatalf("cache GC window: %d %v", removed, err)
	}
	_, err = server.CopyImage(ctx, request)
	found := false
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "IMAGE_CACHE_NOT_ADMITTED" && info.Metadata["admission"] == "not_accepted" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing precise pre-admission fact: %v", err)
	}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy", InputSha256: request.ControllerRequestSha256, Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ImageReference: request.ImageReference}
	if _, _, err := runtime.acceptImageOperation(ctx, "target", receipt, "retained-original", func() {}); err != nil {
		t.Fatal(err)
	}
	runtime.imageOperations.Delete(imageOperationRecordKey("target", "copy", receipt.Kind))
	if _, err := server.CopyImage(ctx, request); status.Code(err) != codes.Aborted {
		t.Fatalf("lost accepted writer became cache miss: %v", err)
	}
}

func TestCopyReceiptHandsSourceRetentionToTargetGrant(t *testing.T) {
	ctx := context.Background()
	target, unrelated := digest.FromString("copy-source"), digest.FromString("idle-image")
	reference := "docker.io/library/source:latest"
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{
		"source":                   {reference: {Name: reference, Target: &types.Descriptor{Digest: target.String()}}},
		runtimeContainerdNamespace: {runtimeImageReference(target): {Name: runtimeImageReference(target), Target: &types.Descriptor{Digest: target.String()}}, runtimeImageReference(unrelated): {Name: runtimeImageReference(unrelated), Target: &types.Descriptor{Digest: unrelated.String()}}},
	}, containers: map[string]*containersapi.Container{}}
	facts := &operationNamespaces{labels: map[string]map[string]string{"source": {}}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("source")
	index.ensureNamespace("target")
	if err := index.addImage("source", reference, target.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(runtimeImageReference(target))
	index.addRuntimeImage(runtimeImageReference(unrelated))
	runtime := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts))), inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	unlock, lockErr := runtime.lockNamespace(context.Background(), "source")
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy-handoff", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ManifestDigest: target.String(), ImageReference: reference, InputSha256: strings.Repeat("a", 64)}
	run, _, err := runtime.acceptImageOperation(ctx, "target", receipt, reference, func() {})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	source, err := runtime.client.ImageService().Get(namespaceContext(ctx, "source"), reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.DeleteImage(ctx, "source", reference, imageGenerationID(source), target.String()); err != nil {
		t.Fatal(err)
	}
	facts.mu.Lock()
	facts.labelReads = 0
	facts.mu.Unlock()
	err = runtime.materializationSlot.Acquire(ctx, 1)
	unlockGC := func() { runtime.materializationSlot.Release(1) }
	if err != nil {
		t.Fatal(err)
	}
	removed, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32)
	unlockGC()
	if err != nil || removed != 1 {
		t.Fatalf("active copy / unrelated GC: removed=%d err=%v", removed, err)
	}
	facts.mu.Lock()
	reads := facts.labelReads
	facts.mu.Unlock()
	if reads != 0 {
		t.Fatalf("ordinary GC scanned image receipts %d times", reads)
	}
	if fixture.images[runtimeContainerdNamespace][runtimeImageReference(target)] == nil || fixture.images[runtimeContainerdNamespace][runtimeImageReference(unrelated)] != nil {
		t.Fatal("copy pin was not specific to its digest")
	}
	if err := index.addImage("target", reference, target.String()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.finishImageOperation("target", receipt, run, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if removed, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil || removed != 0 {
		t.Fatalf("target grant lost at settlement: %d %v", removed, err)
	}
	if err := index.removeImage("target", reference); err != nil {
		t.Fatal(err)
	}
	if removed, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil || removed != 1 {
		t.Fatalf("settled source remained pinned: %d %v", removed, err)
	}
}

func TestRetainedAliasGCLeavesCopiedSeedMaterialized(t *testing.T) {
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable ext4 project-quota mount")
	}
	if err := PrepareOCIStorage(device); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(stateRoot, "containerd", "overlayfs"), "retained-gc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	physical, err := overlay.NewSnapshotter(filepath.Join(root, "snapshots"))
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
	ctx := context.Background()
	nsctx := runtimeContext(ctx)
	if err := metadataDB.Init(ctx); err != nil {
		t.Fatal(err)
	}
	snapshots, store := metadataDB.Snapshotter(containerSnapshotter), metadataDB.ContentStore()
	layerID := digest.FromString("retained-seed-layer")
	mounts, err := snapshots.Prepare(nsctx, "seed-layer", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mounts[0].Source, "payload"), []byte("retained seed data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Commit(nsctx, layerID.String(), "seed-layer"); err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, Config: ocispec.ImageConfig{Cmd: []string{"true"}}, RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{layerID}}})
	configDesc := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	manifest, _ := json.Marshal(ocispec.Manifest{Versioned: specsVersion.Versioned{SchemaVersion: 2}, Config: configDesc})
	target := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	if err := content.WriteBlob(nsctx, store, "config", bytes.NewReader(config), configDesc, content.WithLabels(map[string]string{"containerd.io/gc.ref.snapshot." + containerSnapshotter: layerID.String()})); err != nil {
		t.Fatal(err)
	}
	if err := content.WriteBlob(nsctx, store, "manifest", bytes.NewReader(manifest), target, content.WithLabels(map[string]string{"containerd.io/gc.ref.content.config": configDesc.Digest.String()})); err != nil {
		t.Fatal(err)
	}
	limits := &vmmdv1.NamespaceLimits{DiskBytes: 32 << 20, Pids: 64}
	facts := &operationNamespaces{labels: map[string]map[string]string{}}
	index := newRuntimeInventoryIndex()
	for i, namespace := range []string{"preparation", "seed", "next"} {
		facts.labels[namespace] = namespaceLabels(limits, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, uint32(10100+i), fmt.Sprintf("10.42.%d.0/24", 100+i))
		index.ensureNamespace(namespace)
		if err := setProjectQuota(device, uint32(10100+i), 32<<20); err != nil {
			t.Fatal(err)
		}
	}
	fixture := &systemImageFixture{tasksAbsent: true, images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithIntrospectionService(warmSnapshotIntrospection{}), containerd.WithImageStore(metadata.NewImageStore(metadataDB)), containerd.WithContainerStore(metadata.NewContainerStore(metadataDB)), containerd.WithLeasesService(metadata.NewLeaseManager(metadataDB)), containerd.WithContentStore(store), containerd.WithSnapshotters(map[string]snapshotsapi.Snapshotter{containerSnapshotter: snapshots})))
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), stateDevice: device, logger: slog.Default().With("component", "guest-control")}
	reference := "registry.example/seed:latest"
	canonical := runtimeImageReference(target.Digest)
	if _, err := client.ImageService().Create(nsctx, coreimages.Image{Name: canonical, Target: target}); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(canonical)
	if _, err := client.ImageService().Create(namespaceContext(ctx, "preparation"), coreimages.Image{Name: reference, Target: target}); err != nil {
		t.Fatal(err)
	}
	if err := index.addImage("preparation", reference, target.Digest.String()); err != nil {
		t.Fatal(err)
	}
	built := &vmmdv1.ImageOperationReceipt{RequestId: "built", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true, ImageReference: reference, ManifestDigest: target.Digest.String()}
	if err := runtime.retainBuiltImage(ctx, "controller", "preparation", built); err != nil {
		t.Fatal(err)
	}
	server := &Server{runtime: runtime}
	copyRequest := &vmmdv1.CopyImageRequest{RequestId: "copy-seed", ControllerCacheScope: "controller", RetainedBuildRequestId: "built", TargetNamespaceId: "seed", ImageReference: reference, ManifestDigest: target.Digest.String(), ControllerRequestSha256: strings.Repeat("a", 64)}
	if _, err := server.CopyImage(ctx, copyRequest); err != nil {
		t.Fatal(err)
	}
	deleteGrant := func(namespace string) {
		record, err := client.ImageService().Get(namespaceContext(ctx, namespace), reference)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.DeleteImage(ctx, namespace, reference, imageGenerationID(record), target.Digest.String()); err != nil {
			t.Fatal(err)
		}
	}
	deleteGrant("preparation")
	if removed, err := runtime.reclaimRetainedImageCache(ctx, 32); err != nil || removed != 1 {
		t.Fatalf("retained alias collection: %d %v", removed, err)
	}
	if _, err := runtime.retainedImage(ctx, "controller", reference, target.Digest.String(), "built"); !errdefs.IsNotFound(err) {
		t.Fatalf("alias stayed authorized: %v", err)
	}
	if removed, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil || removed != 0 {
		t.Fatalf("seed grant lost canonical: %d %v", removed, err)
	}
	if _, err := metadataDB.GarbageCollect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshots.Stat(nsctx, layerID.String()); err != nil {
		t.Fatalf("alias deletion collected seed lower: %v", err)
	}
	if _, err := store.Info(nsctx, configDesc.Digest); err != nil {
		t.Fatalf("alias deletion collected seed content: %v", err)
	}
	if _, err := client.ImageService().Get(nsctx, canonical); err != nil {
		t.Fatalf("alias deletion removed canonical: %v", err)
	}
	if _, err := os.Stat(supervisorHostPath); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(supervisorHostPath, []byte("fixture-supervisor"), 0755); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(supervisorHostPath)
	}
	created, err := runtime.CreateContainer(ctx, &vmmdv1.CreateContainerRequest{RequestId: "cached-create", ActorNamespaceId: "seed", ContainerId: "cached", Image: reference, Limits: &vmmdv1.ResourceLimits{}, LogLimitBytes: 4096})
	if err != nil || created.Id != "cached" {
		t.Fatalf("cached create after alias GC: %v %v", created, err)
	}
	next := proto.Clone(copyRequest).(*vmmdv1.CopyImageRequest)
	next.RequestId = "copy-next"
	next.ControllerCacheScope = ""
	next.RetainedBuildRequestId = ""
	next.SourceNamespaceId = "seed"
	next.TargetNamespaceId = "next"
	if _, err := server.CopyImage(ctx, next); err != nil {
		t.Fatalf("allocation copy after alias GC: %v", err)
	}
	seedRecord, err := client.ImageService().Get(namespaceContext(ctx, "seed"), reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.DeleteImage(ctx, "seed", reference, imageGenerationID(seedRecord), target.Digest.String()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("active container allowed source deletion: %v", err)
	}
	if err := runtime.DeleteContainer(ctx, "seed", "cached", created.GenerationId); err != nil {
		t.Fatal(err)
	}
	deleteGrant("seed")
	deleteGrant("next")
	if removed, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil || removed != 1 {
		t.Fatalf("final grant release did not collect canonical: %d %v", removed, err)
	}
	if _, err := metadataDB.GarbageCollect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshots.Stat(nsctx, layerID.String()); !errdefs.IsNotFound(err) {
		t.Fatalf("unused lower stayed retained: %v", err)
	}
	if _, err := store.Info(nsctx, configDesc.Digest); !errdefs.IsNotFound(err) {
		t.Fatalf("unused content stayed retained: %v", err)
	}
}

func TestImageOperationStatusLabelBudgetPreservesCapacityDetails(t *testing.T) {
	const finalCause = "late failure: storage capacity unavailable"
	failure := resourceCapacityExhaustedWithBytes(resourcev2.CapacityStorageHeadroom, "import_admission", strings.Repeat("容量\"\n", 4096)+finalCause, 1024, 2048)
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "build", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, InputSha256: strings.Repeat("a", 64), State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED, WriterSettled: true, Status: status.Convert(failure).Proto()}
	for _, key := range []string{labelPrefix + "build-receipt", imageOperationRecordKey("owner", receipt.RequestId, receipt.Kind)} {
		encoded, err := encodeImageReceiptLabel(key, receipt, imageReceiptLabel{SourceImage: strings.Repeat("source", 20), WriterID: "writer", Digest: "sha256:" + strings.Repeat("b", 64)})
		if err != nil {
			t.Fatal(err)
		}
		if len(key)+len(encoded) > 4096 {
			t.Fatalf("label used %d bytes", len(key)+len(encoded))
		}
		var label imageReceiptLabel
		if err := json.Unmarshal([]byte(encoded), &label); err != nil {
			t.Fatal(err)
		}
		bytes, err := base64.StdEncoding.DecodeString(label.Receipt)
		if err != nil {
			t.Fatal(err)
		}
		var actual vmmdv1.ImageOperationReceipt
		if err := proto.Unmarshal(bytes, &actual); err != nil {
			t.Fatal(err)
		}
		if actual.Status.Code != int32(codes.ResourceExhausted) || !actual.WriterSettled || len(actual.Status.Details) != 2 {
			t.Fatalf("status changed: %+v", &actual)
		}
		info := new(errdetails.ErrorInfo)
		if err := actual.Status.Details[0].UnmarshalTo(info); err != nil {
			t.Fatal(err)
		}
		if info.Reason != "RESOURCE_CAPACITY_EXHAUSTED" || info.Metadata["required_bytes"] != "2048" {
			t.Fatalf("capacity details lost: %v", info)
		}
		if !utf8.ValidString(actual.Status.Message) || actual.Status.Message == receipt.Status.Message || !strings.HasSuffix(actual.Status.Message, finalCause) {
			t.Fatal("diagnostic was not bounded at a UTF-8 boundary")
		}
	}
}
