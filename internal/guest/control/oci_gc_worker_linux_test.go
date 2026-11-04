//go:build linux

package control

import (
	"context"
	"fmt"
	coreimages "github.com/containerd/containerd/v2/core/images"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	digest "github.com/opencontainers/go-digest"
	"golang.org/x/sync/semaphore"
)

func orphanWorkerFixture(t *testing.T) (*Runtime, *systemImageFixture, *operationNamespaces) {
	t.Helper()
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{runtimeContainerdNamespace: {}}, containers: map[string]*containersapi.Container{}}
	facts := &operationNamespaces{labels: map[string]map[string]string{}}
	r := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithLeasesService(&operationLeases{}))), inventory: newRuntimeInventoryIndex(), materializationSlot: semaphore.NewWeighted(1), logger: slog.Default()}
	return r, fixture, facts
}

func waitForRuntimeImages(t *testing.T, fixture *systemImageFixture, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fixture.mu.Lock()
		got := len(fixture.images[runtimeContainerdNamespace])
		fixture.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	t.Fatalf("runtime images=%d, want %d", len(fixture.images[runtimeContainerdNamespace]), want)
}

func TestRuntimeGCResumesAcrossRetainedPagesRetriesAndStops(t *testing.T) {
	r, fixture, _ := orphanWorkerFixture(t)
	for i := 0; i < 70; i++ {
		target := digest.FromString(fmt.Sprint(i))
		name := runtimeImageReference(target)
		fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
		r.inventory.addRuntimeImage(name)
	}
	// Noncanonical roots precede all canonical roots. No page may be treated as
	// finished merely because none of its records can be deleted.
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("000-retained-%03d", i)
		fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: digest.FromString(name).String()}}
		r.inventory.addRuntimeImage(name)
	}
	fixture.failDelete = true
	r.gcRuntimeImageCursor = "zz-pressure-cursor"
	r.startRuntimeGC()
	defer r.stopRuntimeGC()
	deadline := time.Now().Add(time.Second)
	for {
		fixture.mu.Lock()
		attempts := fixture.deleteAttempts
		fixture.mu.Unlock()
		if attempts > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("startup GC made no attempt")
		}
		time.Sleep(time.Millisecond)
	}
	fixture.mu.Lock()
	fixture.failDelete = false
	fixture.mu.Unlock()
	waitForRuntimeImages(t, fixture, 40)
	if r.gcRuntimeImageCursor != "zz-pressure-cursor" {
		t.Fatal("ordinary GC moved the pressure cursor")
	}
	r.stopRuntimeGC()
	target := digest.FromString("after-stop")
	name := runtimeImageReference(target)
	fixture.mu.Lock()
	fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
	fixture.mu.Unlock()
	r.inventory.addRuntimeImage(name)
	time.Sleep(20 * time.Millisecond)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.images[runtimeContainerdNamespace][name] == nil {
		t.Fatal("stopped worker deleted an image")
	}
}

func TestRuntimeGCWakeDuringPassRevisitsReleasedGrant(t *testing.T) {
	r, fixture, _ := orphanWorkerFixture(t)
	targets := []digest.Digest{digest.FromString("first"), digest.FromString("second")}
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	for _, target := range targets {
		name := runtimeImageReference(target)
		fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
		r.inventory.addRuntimeImage(name)
	}
	r.inventory.ensureNamespace("source")
	if err := r.inventory.addImage("source", "image", targets[0].String()); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	fixture.beforeGet = func(name string) {
		if name == runtimeImageReference(targets[1]) {
			once.Do(func() {
				if err := r.inventory.removeImage("source", "image"); err != nil {
					t.Error(err)
				}
			})
		}
	}
	r.startRuntimeGC()
	defer r.stopRuntimeGC()
	waitForRuntimeImages(t, fixture, 0)
}

func TestRuntimeGCRestartRebuildsAcceptedSourcePins(t *testing.T) {
	r, fixture, facts := orphanWorkerFixture(t)
	target := digest.FromString("accepted-source")
	name := runtimeImageReference(target)
	fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
	r.inventory.addRuntimeImage(name)
	r.imageOperationWriterID = "old-writer"
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ManifestDigest: target.String(), ImageReference: "registry.example/source:latest"}
	_, _, err := r.acceptImageOperation(context.Background(), "gone-target", receipt, "source", func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.rebuildRuntimeInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.startRuntimeGC()
	defer r.stopRuntimeGC()
	if err := r.collectRuntimeImages(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	exists := fixture.images[runtimeContainerdNamespace][name] != nil
	fixture.mu.Unlock()
	if !exists {
		t.Fatal("startup GC lost accepted source")
	}
	r.imageOperations.Delete(imageOperationRecordKey("gone-target", "copy", receipt.Kind))
	r.imageOperationsRecovered = true
	r.imageOperationWriterID = "new-writer"
	observed, err := (&Server{runtime: r}).ObserveImageOperation(context.Background(), &vmmdv1.ImageOperationRequest{ActorNamespaceId: "gone-target", RequestId: "copy", Kind: receipt.Kind})
	if err != nil || !observed.GetWriterSettled() || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN {
		t.Fatalf("recovered deleted-namespace writer: %v %v", observed, err)
	}
	waitForRuntimeImages(t, fixture, 0)
	facts.mu.Lock()
	defer facts.mu.Unlock()
	if facts.labels["gone-target"] != nil {
		t.Fatal("settlement recreated deleted namespace")
	}
	if facts.labels[runtimeContainerdNamespace][imageOperationRecordKey("gone-target", "copy", receipt.Kind)] == "" {
		t.Fatal("GC erased accepted receipt")
	}
}

func TestRuntimeGCStopCancelsMaterializerWait(t *testing.T) {
	r, fixture, _ := orphanWorkerFixture(t)
	target := digest.FromString("waiting")
	name := runtimeImageReference(target)
	fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
	r.inventory.addRuntimeImage(name)
	if err := r.materializationSlot.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	defer r.materializationSlot.Release(1)
	r.startRuntimeGC()
	done := make(chan struct{})
	go func() { r.stopRuntimeGC(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for materializer")
	}
}

func TestRuntimeGCPinToGrantHandoffCannotFallBetweenReads(t *testing.T) {
	r, fixture, facts := orphanWorkerFixture(t)
	ctx := context.Background()
	target := digest.FromString("reverse-handoff")
	name := runtimeImageReference(target)
	fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
	r.inventory.addRuntimeImage(name)
	r.inventory.ensureNamespace("target")
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ManifestDigest: target.String(), ImageReference: "registry.example/source:latest"}
	run, _, err := r.acceptImageOperation(ctx, "target", receipt, "source", func() {})
	if err != nil {
		t.Fatal(err)
	}
	var transferred atomic.Bool
	handoff := func() {
		if transferred.Swap(true) {
			return
		}
		_, err := r.client.ImageService().Create(namespaceContext(ctx, "target"), coreimages.Image{Name: receipt.ImageReference, Target: ocispec.Descriptor{Digest: target}})
		if err != nil {
			t.Error(err)
			return
		}
		if err := r.indexCommittedImage("target", receipt.ImageReference, target.String()); err != nil {
			t.Error(err)
			return
		}
		if err := r.finishImageOperation("target", receipt, run, nil, nil, false); err != nil {
			t.Error(err)
		}
	}
	// Old GC first read zero grants, then read receipt labels. A writer can
	// publish its target grant and settle the source pin between those reads.
	facts.beforeLabels = func(namespace string) {
		if namespace == runtimeContainerdNamespace {
			handoff()
		}
	}
	if err := r.collectRuntimeImages(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	exists := fixture.images[runtimeContainerdNamespace][name] != nil
	fixture.mu.Unlock()
	if !exists {
		t.Fatal("GC collected content during accepted-source handoff")
	}
	handoff()
	if err := r.collectRuntimeImages(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.images[runtimeContainerdNamespace][name] == nil {
		t.Fatal("settled target grant lost content")
	}
}
