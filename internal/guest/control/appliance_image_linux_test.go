//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/containerd/containerd/api/types"
	tasktypes "github.com/containerd/containerd/api/types/task"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/typeurl/v2"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestBundledSystemImageClassification(t *testing.T) {
	reference := bundledMCPImage + "sha256:" + strings.Repeat("0", 64)
	if !isBundledSystemImage(reference) {
		t.Fatalf("bundled image %q was routed to the registry", reference)
	}
	if isBundledSystemImage("registry.example/appliance@sha256:" + strings.Repeat("0", 64)) {
		t.Fatal("external image was routed to the bundled importer")
	}
}

func TestBundledImportRequestIDChangesWithCanonicalDigest(t *testing.T) {
	first := bundledImportRequestID(bundledMCPImage + "sha256:" + strings.Repeat("0", 64))
	second := bundledImportRequestID(bundledMCPImage + "sha256:" + strings.Repeat("1", 64))
	if first == second {
		t.Fatalf("bundled import request IDs must be unique per canonical digest: %q %q", first, second)
	}
	if len(first) > 63 {
		t.Fatalf("bundled import request ID exceeds request limit: %d", len(first))
	}
}

func TestSystemImagePruningUsesNamespaceRetentionAndRetriesFailures(t *testing.T) {
	ctx := context.Background()
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}, failDelete: true}
	index := newRuntimeInventoryIndex()
	for _, namespace := range []string{"system.a", "system.b"} {
		index.ensureNamespace(namespace)
		fixture.images[namespace] = make(map[string]*imagesapi.Image)
	}
	for _, item := range []struct{ namespace, reference, digest string }{{"system.a", "current", "current"}, {"system.a", "old", "old"}, {"system.a", "shared", "shared"}, {"system.a", "used", "used"}, {"system.b", "shared", "shared"}} {
		target := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(item.digest)))
		fixture.images[item.namespace][item.reference] = &imagesapi.Image{Name: item.reference, Target: &types.Descriptor{Digest: target}}
		if err := index.addImage(item.namespace, item.reference, target); err != nil {
			t.Fatal(err)
		}
		physical := runtimeImageReference(digest.Digest(target))
		index.addRuntimeImage(physical)
		if fixture.images[runtimeContainerdNamespace] == nil {
			fixture.images[runtimeContainerdNamespace] = make(map[string]*imagesapi.Image)
		}
		fixture.images[runtimeContainerdNamespace][physical] = &imagesapi.Image{Name: physical, Target: &types.Descriptor{Digest: target}}
	}
	cacheDigest := digest.FromString("retained-environment")
	cacheReference := retainedImageReference("controller-a", "environment", cacheDigest.String(), "successful-build")
	fixture.images[runtimeContainerdNamespace][cacheReference] = &imagesapi.Image{Name: cacheReference, Target: &types.Descriptor{Digest: cacheDigest.String()}, Labels: map[string]string{retainedScopeLabel: "controller-a"}}
	index.ensureNamespace(retainedImageOwner("controller-a"))
	if err := index.addImage(retainedImageOwner("controller-a"), cacheReference, cacheDigest.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(cacheReference)
	process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"controller"}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.containers["used-container"] = &containersapi.Container{ID: "used-container", Labels: map[string]string{runtimeOwnerLabel: "system.a", runtimeExternalIDLabel: "payload", runtimeImageLabel: "used", containerStateLabel: "running", containerInstanceLabel: "instance"}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	fixture.containers["stopped-container"] = &containersapi.Container{ID: "stopped-container", Labels: map[string]string{runtimeOwnerLabel: "system.a", runtimeExternalIDLabel: "stopped-payload", runtimeImageLabel: "old", containerStateLabel: "stopped"}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	if err := index.addContainer("system.a", "stopped-payload"); err != nil {
		t.Fatal(err)
	}
	client := systemImageClient(t, fixture)
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	if err := runtime.pruneSystemImages(ctx, "system.a", "current", ""); err == nil {
		t.Fatal("delete failure was ignored")
	}
	fixture.mu.Lock()
	fixture.failDelete = false
	fixture.mu.Unlock()
	if err := runtime.pruneSystemImages(ctx, "system.a", "current", ""); err != nil {
		t.Fatal(err)
	}
	if err := runtime.pruneSystemImages(ctx, "system.a", "current", ""); err != nil {
		t.Fatal(err)
	}
	if err := runtime.collectRuntimeImages(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.containers["stopped-container"] != nil || fixture.containers["used-container"] == nil {
		t.Fatal("stopped/running container fence failed")
	}
	for _, reference := range []string{"current", "used"} {
		if fixture.images["system.a"][reference] == nil {
			t.Fatalf("removed retained %s", reference)
		}
	}
	if len(fixture.images["system.a"]) != 2 || fixture.images["system.b"]["shared"] == nil {
		t.Fatalf("unexpected retention: %+v", fixture.images)
	}
	if !index.imageDigestReferenced(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("shared")))) {
		t.Fatal("cross namespace retention lost")
	}
	if fixture.images[runtimeContainerdNamespace][runtimeImageReference(digest.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("old")))))] != nil {
		t.Fatal("unreferenced physical image retained")
	}
	if fixture.images[runtimeContainerdNamespace][cacheReference] == nil || !index.imageDigestReferenced(cacheDigest.String()) {
		t.Fatal("system pruning evicted retained environment cache")
	}
}

type systemImageFixture struct {
	imagesapi.UnimplementedImagesServer
	mu                       sync.Mutex
	images                   map[string]map[string]*imagesapi.Image
	containers               map[string]*containersapi.Container
	tasksAbsent              bool
	failDelete               bool
	deleteAttempts           int
	lostCreateReplyNamespace string
	lostUpdateReplyNamespace string
	failDeleteNamespace      string
	beforeGet                func(string)
}

func imageNamespace(ctx context.Context) string {
	metadata, _ := metadata.FromIncomingContext(ctx)
	return metadata.Get("containerd-namespace")[0]
}
func (f *systemImageFixture) List(ctx context.Context, _ *imagesapi.ListImagesRequest) (*imagesapi.ListImagesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := &imagesapi.ListImagesResponse{}
	for _, image := range f.images[imageNamespace(ctx)] {
		result.Images = append(result.Images, image)
	}
	return result, nil
}
func (f *systemImageFixture) Create(ctx context.Context, request *imagesapi.CreateImageRequest) (*imagesapi.CreateImageResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	namespace := imageNamespace(ctx)
	if f.images[namespace] == nil {
		f.images[namespace] = make(map[string]*imagesapi.Image)
	}
	if f.images[namespace][request.Image.Name] != nil {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	f.images[namespace][request.Image.Name] = request.Image
	if f.lostCreateReplyNamespace == namespace {
		return nil, status.Error(codes.Unavailable, "reply lost after create commit")
	}
	return &imagesapi.CreateImageResponse{Image: request.Image}, nil
}

func (f *systemImageFixture) Update(ctx context.Context, request *imagesapi.UpdateImageRequest) (*imagesapi.UpdateImageResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	namespace := imageNamespace(ctx)
	if f.images[namespace][request.Image.Name] == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	f.images[namespace][request.Image.Name] = request.Image
	if f.lostUpdateReplyNamespace == namespace {
		return nil, status.Error(codes.Unavailable, "reply lost after update commit")
	}
	return &imagesapi.UpdateImageResponse{Image: request.Image}, nil
}

func (f *systemImageFixture) Get(ctx context.Context, request *imagesapi.GetImageRequest) (*imagesapi.GetImageResponse, error) {
	f.mu.Lock()
	hook := f.beforeGet
	f.mu.Unlock()
	if hook != nil {
		hook(request.Name)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	image := f.images[imageNamespace(ctx)][request.Name]
	if image == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return &imagesapi.GetImageResponse{Image: image}, nil
}
func (f *systemImageFixture) Delete(ctx context.Context, request *imagesapi.DeleteImageRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteAttempts++
	if f.failDelete || f.failDeleteNamespace == imageNamespace(ctx) {
		return nil, status.Error(codes.Unavailable, "retry")
	}
	delete(f.images[imageNamespace(ctx)], request.Name)
	return &emptypb.Empty{}, nil
}

type systemContainerFixture struct {
	containersapi.UnimplementedContainersServer
	fixture *systemImageFixture
}

func (f *systemContainerFixture) List(context.Context, *containersapi.ListContainersRequest) (*containersapi.ListContainersResponse, error) {
	f.fixture.mu.Lock()
	defer f.fixture.mu.Unlock()
	result := &containersapi.ListContainersResponse{}
	for _, container := range f.fixture.containers {
		result.Containers = append(result.Containers, container)
	}
	return result, nil
}
func (f *systemContainerFixture) Get(_ context.Context, request *containersapi.GetContainerRequest) (*containersapi.GetContainerResponse, error) {
	f.fixture.mu.Lock()
	defer f.fixture.mu.Unlock()
	container := f.fixture.containers[request.ID]
	if container == nil {
		return nil, status.Error(codes.NotFound, "container not found")
	}
	return &containersapi.GetContainerResponse{Container: container}, nil
}
func (f *systemContainerFixture) Delete(_ context.Context, request *containersapi.DeleteContainerRequest) (*emptypb.Empty, error) {
	f.fixture.mu.Lock()
	defer f.fixture.mu.Unlock()
	delete(f.fixture.containers, request.ID)
	return &emptypb.Empty{}, nil
}

type systemTaskFixture struct {
	tasksapi.UnimplementedTasksServer
	absent bool
}

func (f *systemTaskFixture) Get(_ context.Context, request *tasksapi.GetRequest) (*tasksapi.GetResponse, error) {
	if f.absent || request.ContainerID == "stopped-container" {
		return nil, status.Error(codes.NotFound, "stopped")
	}
	return &tasksapi.GetResponse{Process: &tasktypes.Process{ContainerID: request.ContainerID, Status: tasktypes.Status_RUNNING}}, nil
}
func systemImageClient(t *testing.T, fixture *systemImageFixture, options ...containerd.Opt) *containerd.Client {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "containerd.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	imagesapi.RegisterImagesServer(server, fixture)
	containersapi.RegisterContainersServer(server, &systemContainerFixture{fixture: fixture})
	tasksapi.RegisterTasksServer(server, &systemTaskFixture{absent: fixture.tasksAbsent})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	options = append([]containerd.Opt{containerd.WithServices(containerd.WithNamespaceService(&operationNamespaces{labels: map[string]map[string]string{}}))}, options...)
	client, err := containerd.New(listener.Addr().String(), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestPermanentSystemImageReleasePreservesOtherNamespace(t *testing.T) {
	ctx := context.Background()
	identity := sha256.Sum256([]byte("installation"))
	namespace := fmt.Sprintf("system.%x", identity[:16])
	target := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("shared")))
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}}
	index := newRuntimeInventoryIndex()
	for _, owner := range []string{namespace, "system.other"} {
		index.ensureNamespace(owner)
		fixture.images[owner] = map[string]*imagesapi.Image{"shared": {Name: "shared", Target: &types.Descriptor{Digest: target}}}
		if err := index.addImage(owner, "shared", target); err != nil {
			t.Fatal(err)
		}
	}
	physical := runtimeImageReference(digest.Digest(target))
	index.addRuntimeImage(physical)
	fixture.images[runtimeContainerdNamespace] = map[string]*imagesapi.Image{physical: {Name: physical, Target: &types.Descriptor{Digest: target}}}
	ownDigest := digest.FromString("owned-only")
	ownPhysical := runtimeImageReference(ownDigest)
	fixture.images[namespace]["owned-only"] = &imagesapi.Image{Name: "owned-only", Target: &types.Descriptor{Digest: ownDigest.String()}}
	fixture.images[runtimeContainerdNamespace][ownPhysical] = &imagesapi.Image{Name: ownPhysical, Target: &types.Descriptor{Digest: ownDigest.String()}}
	if err := index.addImage(namespace, "owned-only", ownDigest.String()); err != nil {
		t.Fatal(err)
	}
	index.addRuntimeImage(ownPhysical)
	runtime := &Runtime{client: systemImageClient(t, fixture), inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	ok := runtime.materializationSlot.TryAcquire(1)
	unlockMaterializer := func() { runtime.materializationSlot.Release(1) }
	if !ok {
		t.Fatal("materializer stayed occupied after failure")
	}
	defer unlockMaterializer()
	process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"controller"}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.containers["owned"] = &containersapi.Container{ID: "owned", Labels: map[string]string{runtimeOwnerLabel: namespace, runtimeExternalIDLabel: "payload", runtimeImageLabel: "shared", containerStateLabel: "running", containerInstanceLabel: "instance"}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	if err := index.addContainer(namespace, "payload"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.releaseSystemInstallationImagesMaterializerHeld(ctx, "installation"); err == nil {
		t.Fatal("running installation removal accepted")
	}
	fixture.mu.Lock()
	if fixture.images[namespace]["shared"] == nil {
		t.Fatal("rejected removal released retention")
	}
	fixture.containers["owned"].Labels[containerStateLabel] = "stopped"
	fixture.containers["owned"].ID = "stopped-container"
	fixture.containers["stopped-container"] = fixture.containers["owned"]
	delete(fixture.containers, "owned")
	fixture.mu.Unlock()
	fixture.mu.Lock()
	fixture.failDeleteNamespace = runtimeContainerdNamespace
	fixture.mu.Unlock()
	if err := runtime.releaseSystemInstallationImagesMaterializerHeld(ctx, "installation"); err != nil {
		t.Fatal(err)
	}
	// Logical release does not wait for the physical GC worker.
	if _, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err == nil {
		t.Fatal("GC failure ignored")
	}
	fixture.mu.Lock()
	fixture.failDeleteNamespace = ""
	fixture.mu.Unlock()
	for range 2 {
		if err := runtime.releaseSystemInstallationImagesMaterializerHeld(ctx, "installation"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runtime.pruneUnreferencedRuntimeImages(ctx, 32); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.images[runtimeContainerdNamespace][ownPhysical] != nil {
		t.Fatal("half-failed removal retained physical image")
	}
	if len(fixture.images[namespace]) != 0 || fixture.images["system.other"]["shared"] == nil || fixture.images[runtimeContainerdNamespace][physical] == nil {
		t.Fatal("permanent removal crossed image ownership")
	}
}

func TestSystemRetentionSurvivesStoppedActiveAndProjectionReordering(t *testing.T) {
	ctx := context.Background()
	namespace := "system.retention"
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{namespace: {}, runtimeContainerdNamespace: {}}, containers: map[string]*containersapi.Container{}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(namespace)
	for _, name := range []string{"active", "pending", "obsolete"} {
		target := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(name)))
		fixture.images[namespace][name] = &imagesapi.Image{Name: name, Target: &types.Descriptor{Digest: target}}
		if err := index.addImage(namespace, name, target); err != nil {
			t.Fatal(err)
		}
		physical := runtimeImageReference(digest.Digest(target))
		fixture.images[runtimeContainerdNamespace][physical] = &imagesapi.Image{Name: physical, Target: &types.Descriptor{Digest: target}}
		index.addRuntimeImage(physical)
	}
	facts := &operationNamespaces{labels: map[string]map[string]string{namespace: {}}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts)))
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	active := fixture.images[namespace]["active"].Target.Digest
	pending := fixture.images[namespace]["pending"].Target.Digest
	projection := &vmmdv1.SystemImageRetention{InstallationRevision: 2, ManifestDigests: []string{active, pending}}
	if err := runtime.projectSystemImageRetention(ctx, namespace, projection); err != nil {
		t.Fatal(err)
	}
	if err := runtime.pruneSystemImages(ctx, namespace, "pending", pending); err != nil {
		t.Fatal(err)
	}
	if fixture.images[namespace]["active"] == nil || fixture.images[namespace]["pending"] == nil || fixture.images[namespace]["obsolete"] != nil {
		t.Fatal("stopped active or pending retention lost")
	}
	runtime = &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	if err := runtime.projectSystemImageRetention(ctx, namespace, &vmmdv1.SystemImageRetention{InstallationRevision: 1, ManifestDigests: []string{active}}); status.Code(err) != codes.Aborted {
		t.Fatalf("old projection=%v", err)
	}
	if err := runtime.pruneSystemImages(ctx, namespace, "", ""); err != nil {
		t.Fatal(err)
	}
	if fixture.images[namespace]["pending"] == nil {
		t.Fatal("restart or old projection removed new pending")
	}
	if err := runtime.projectSystemImageRetention(ctx, namespace, &vmmdv1.SystemImageRetention{InstallationRevision: 3, ManifestDigests: []string{pending}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.pruneSystemImages(ctx, namespace, "", ""); err != nil {
		t.Fatal(err)
	}
	if fixture.images[namespace]["active"] != nil || fixture.images[namespace]["pending"] == nil {
		t.Fatal("terminal projection did not release old active")
	}
	if err := runtime.projectSystemImageRetention(ctx, namespace, &vmmdv1.SystemImageRetention{InstallationRevision: 4}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.pruneSystemImages(ctx, namespace, "", ""); err != nil {
		t.Fatal(err)
	}
	if len(fixture.images[namespace]) != 0 {
		t.Fatal("removed projection retained pending forever")
	}
}

func TestSystemRetentionRetriesFailedPruneWithOriginalRequest(t *testing.T) {
	ctx := context.Background()
	namespace := systemInstallationNamespace("prune-retry")
	target := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("obsolete")))
	fixture := &systemImageFixture{failDelete: true, images: map[string]map[string]*imagesapi.Image{namespace: {"obsolete": {Name: "obsolete", Target: &types.Descriptor{Digest: target}}}, runtimeContainerdNamespace: {}}, containers: map[string]*containersapi.Container{}}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(namespace)
	if err := index.addImage(namespace, "obsolete", target); err != nil {
		t.Fatal(err)
	}
	client := systemImageClient(t, fixture)
	server := &Server{runtime: &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}}
	err := server.runtime.materializationSlot.Acquire(ctx, 1)
	unlock := func() { server.runtime.materializationSlot.Release(1) }
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	unlockNamespace, lockErr := server.runtime.lockNamespace(ctx, namespace)
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	busy := &vmmdv1.ReconcileSystemImagesRequest{RequestId: "namespace-busy", InstallationId: "prune-retry", Retention: &vmmdv1.SystemImageRetention{InstallationRevision: 1, ManifestDigests: []string{target}}}
	if _, err := server.ReconcileSystemImages(deadline, busy); status.Code(err) != codes.Unavailable {
		t.Fatalf("busy namespace projection: %v", err)
	}
	unlockNamespace()
	if _, err := server.ReconcileSystemImages(deadline, busy); err != nil {
		t.Fatalf("original deferred namespace request did not resume: %v", err)
	}
	for _, revision := range []uint64{1, 2} {
		started := time.Now()
		request := &vmmdv1.ReconcileSystemImagesRequest{RequestId: fmt.Sprintf("protected-%d", revision), InstallationId: "prune-retry", Retention: &vmmdv1.SystemImageRetention{InstallationRevision: revision, ManifestDigests: []string{target}}}
		if revision == 2 {
			request.Retention.ManifestDigests = append(request.Retention.ManifestDigests, fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("pending-not-imported"))))
		}
		if _, err := server.ReconcileSystemImages(deadline, request); err != nil {
			t.Fatalf("long builder blocked protected projection: %v", err)
		}
		if time.Since(started) > 250*time.Millisecond {
			t.Fatal("protected projection waited for long builder")
		}
	}
	request := &vmmdv1.ReconcileSystemImagesRequest{RequestId: "original-prune", InstallationId: "prune-retry", Retention: &vmmdv1.SystemImageRetention{InstallationRevision: 3}}
	started := time.Now()
	if _, err := server.ReconcileSystemImages(deadline, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("long writer collection result: %v", err)
	}
	if time.Since(started) > 250*time.Millisecond {
		t.Fatal("deferred collection waited for long builder")
	}
	unlock()
	if _, err := server.ReconcileSystemImages(ctx, request); err == nil {
		t.Fatal("failed prune disappeared")
	}
	fixture.mu.Lock()
	fixture.failDelete = false
	fixture.mu.Unlock()
	if _, err := server.ReconcileSystemImages(ctx, request); err != nil {
		t.Fatalf("original request replayed transient prune failure: %v", err)
	}
	if fixture.images[namespace]["obsolete"] != nil {
		t.Fatal("retry did not finish obsolete cleanup")
	}
}
