//go:build linux

package control

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type uncertainReceiptStore struct {
	*operationNamespaces
	faultMu     sync.Mutex
	loseReply   bool
	rejectWrite bool
	failRead    bool
}

func (s *uncertainReceiptStore) SetLabel(ctx context.Context, namespace, key, value string) error {
	s.faultMu.Lock()
	lose, reject := s.loseReply, s.rejectWrite
	s.faultMu.Unlock()
	if reject {
		return status.Error(codes.Unavailable, "write not submitted")
	}
	if err := s.operationNamespaces.SetLabel(ctx, namespace, key, value); err != nil {
		return err
	}
	if lose {
		return status.Error(codes.Unavailable, "reply lost after commit")
	}
	return nil
}
func (s *uncertainReceiptStore) Labels(ctx context.Context, namespace string) (map[string]string, error) {
	s.faultMu.Lock()
	fail := s.failRead
	s.faultMu.Unlock()
	if fail && namespace == runtimeContainerdNamespace {
		return nil, status.Error(codes.Unavailable, "receipt read unavailable")
	}
	return s.operationNamespaces.Labels(ctx, namespace)
}
func uncertainRetentionFixture(t *testing.T) (*Runtime, *systemImageFixture, *uncertainReceiptStore, digest.Digest, string) {
	t.Helper()
	target := digest.FromString("uncertain-source")
	reference := "docker.io/library/source:latest"
	root := runtimeImageReference(target)
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{"source": {reference: {Name: reference, Target: &types.Descriptor{Digest: target.String()}}}, runtimeContainerdNamespace: {root: {Name: root, Target: &types.Descriptor{Digest: target.String()}}}}, containers: map[string]*containersapi.Container{}}
	facts := &uncertainReceiptStore{operationNamespaces: &operationNamespaces{labels: map[string]map[string]string{"source": namespaceLabels(&vmmdv1.NamespaceLimits{Pids: 64, DiskBytes: 1 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10100, "10.64.1.0/24")}}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("source")
	index.ensureNamespace("target")
	if err := index.addImage("source", reference, target.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(root)
	r := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithLeasesService(&operationLeases{}))), inventory: index, materializationSlot: semaphore.NewWeighted(1), imageOperationWriterID: "writer", logger: slog.Default()}
	return r, fixture, facts, target, reference
}
func assertCollected(t *testing.T, r *Runtime, target digest.Digest, want bool) {
	t.Helper()
	if err := r.collectRuntimeImages(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := r.client.ImageService().Get(runtimeContext(context.Background()), runtimeImageReference(target))
	if want && !errdefs.IsNotFound(err) {
		t.Fatalf("unreferenced image remains: %v", err)
	}
	if !want && err != nil {
		t.Fatalf("retained image lost: %v", err)
	}
}
func releaseSource(t *testing.T, r *Runtime, reference string, target digest.Digest) {
	t.Helper()
	record, err := r.client.ImageService().Get(namespaceContext(context.Background(), "source"), reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteImage(context.Background(), "source", reference, imageGenerationID(record), target.String()); err != nil {
		t.Fatal(err)
	}
}

func TestImageRetentionSurvivesLostAcceptanceAndTerminalReplies(t *testing.T) {
	r, _, facts, target, reference := uncertainRetentionFixture(t)
	ctx := context.Background()
	facts.loseReply = true
	request := &vmmdv1.CopyImageRequest{RequestId: "lost-copy", SourceNamespaceId: "source", TargetNamespaceId: "target", ImageReference: reference, ManifestDigest: target.String(), ControllerRequestSha256: strings.Repeat("a", 64)}
	server := &Server{runtime: r}
	if _, err := server.CopyImage(ctx, request); err == nil {
		t.Fatal("lost receipt reply was acknowledged")
	}
	observe := &vmmdv1.ImageOperationRequest{ActorNamespaceId: "target", RequestId: request.RequestId, Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, InputSha256: request.ControllerRequestSha256}
	receipt, err := server.ObserveImageOperation(ctx, observe)
	if err != nil || receipt.GetWriterSettled() {
		t.Fatalf("accepted receipt=%v %v", receipt, err)
	}
	releaseSource(t, r, reference, target)
	assertCollected(t, r, target, false)
	// Writer settlement commits, but its reply is also lost. Positive terminal
	// observation must release the pin without waiting for another restart.
	receipt.WriterSettled = true
	receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
	if err := r.saveImageOperation(ctx, "target", receipt, ""); err == nil {
		t.Fatal("lost settlement reply was acknowledged")
	}
	assertCollected(t, r, target, false)
	if observed, err := server.ObserveImageOperation(ctx, observe); err != nil || !observed.GetWriterSettled() {
		t.Fatalf("terminal observation=%v %v", observed, err)
	}
	assertCollected(t, r, target, true)
}

func TestUnsubmittedRetentionPinSurvivesSameDaemonRecovery(t *testing.T) {
	r, _, facts, target, reference := uncertainRetentionFixture(t)
	facts.rejectWrite = true
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "uncertain-copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ManifestDigest: target.String(), ImageReference: reference}
	if _, _, err := r.acceptImageOperation(context.Background(), "target", receipt, reference, func() {}); err == nil {
		t.Fatal("expected failed acceptance write")
	}
	releaseSource(t, r, reference, target)
	r.inventory.mu.Lock()
	r.inventory.sourcePinsReady = false
	r.inventory.mu.Unlock()
	assertCollected(t, r, target, false)
	// Only the daemon-fenced cold rebuild may discard an absent provisional
	// write. The production startup settles the previous containerd writer first.
	r.imageOperationWriterID = "replacement-writer"
	if err := r.rebuildRuntimeInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCollected(t, r, target, true)
}

func TestUnknownLogicalPublicationProtectsNewAndPreviousTargets(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "replace"}[replace], func(t *testing.T) {
			r, fixture, _, old, reference := uncertainRetentionFixture(t)
			target := digest.FromString("new-publication")
			name := runtimeImageReference(target)
			fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target.String()}}
			r.inventory.addRuntimeImage(name)
			if !replace {
				releaseSource(t, r, reference, old)
			}
			fixture.lostCreateReplyNamespace = "source"
			fixture.lostUpdateReplyNamespace = "source"
			header := &vmmdv1.ImportImageHeader{RequestId: "publish", ActorNamespaceId: "source", ImageReference: reference, ManifestDigest: target.String(), Platform: "linux/arm64"}
			if _, err := r.commitImportedImage(context.Background(), header, reference, ocispec.Descriptor{Digest: target}, false, true, ""); err == nil {
				t.Fatal("lost publication reply was acknowledged")
			}
			assertCollected(t, r, target, false)
			if replace {
				assertCollected(t, r, old, false)
			}
			fixture.lostCreateReplyNamespace = ""
			fixture.lostUpdateReplyNamespace = ""
			// The acknowledged retry publishes and indexes its owner. This metadata
			// stand-in has no unpack service, so only the later image inspection fails.
			if _, err := r.commitImportedImage(context.Background(), header, reference, ocispec.Descriptor{Digest: target}, false, true, ""); status.Code(err) != codes.Internal {
				t.Fatalf("expected unavailable unpack observation after publication: %v", err)
			}
			assertCollected(t, r, old, true)
			releaseSource(t, r, reference, target)
			assertCollected(t, r, target, true)
		})
	}
}

func TestUnknownRetainedPublicationSurvivesSourceRemovalAndRetry(t *testing.T) {
	r, fixture, _, target, reference := uncertainRetentionFixture(t)
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "built", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true, ImageReference: reference, ManifestDigest: target.String()}
	fixture.lostCreateReplyNamespace = runtimeContainerdNamespace
	if err := r.retainBuiltImage(context.Background(), "controller", "source", receipt); err == nil {
		t.Fatal("lost retained write was acknowledged")
	}
	releaseSource(t, r, reference, target)
	assertCollected(t, r, target, false)
	// A retry that cannot find the old source cannot discard an earlier
	// uncertain retained-root publication.
	if err := r.retainBuiltImage(context.Background(), "controller", "source", receipt); err == nil {
		t.Fatal("missing source retry succeeded")
	}
	assertCollected(t, r, target, false)
	fixture.lostCreateReplyNamespace = ""
	record, err := r.retainedImage(context.Background(), "controller", reference, target.String(), "built")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.deleteRetainedImage(context.Background(), "controller", reference, imageGenerationID(record), target.String()); err != nil {
		t.Fatal(err)
	}
	assertCollected(t, r, target, true)
}

func TestReceiptRecoveryFailureOnlyDefersOrphanGC(t *testing.T) {
	r, fixture, facts, target, reference := uncertainRetentionFixture(t)
	orphan := digest.FromString("independent-orphan")
	name := runtimeImageReference(orphan)
	fixture.images[runtimeContainerdNamespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: orphan.String()}}
	facts.failRead = true
	if err := r.rebuildRuntimeInventory(context.Background()); err != nil {
		t.Fatal("receipt failure blocked runtime recovery", err)
	}
	if namespace, err := r.GetNamespace(context.Background(), "source"); err != nil || namespace.GetId() != "source" {
		t.Fatalf("existing namespace unavailable: %v %v", namespace, err)
	}
	if err := r.collectRuntimeImages(context.Background()); status.Code(errors.Unwrap(err)) != codes.Unavailable {
		t.Fatalf("unknown retention allowed GC: %v", err)
	}
	facts.failRead = false
	facts.operationNamespaces.mu.Lock()
	facts.labels[runtimeContainerdNamespace] = map[string]string{imageOperationRecordKey("source", "build", vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD): `{"receipt":"malformed source-less build payload"}`}
	facts.operationNamespaces.mu.Unlock()
	assertCollected(t, r, orphan, true)
	assertCollected(t, r, target, false)
	releaseSource(t, r, reference, target)
	assertCollected(t, r, target, true)
}
