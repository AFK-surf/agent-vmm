//go:build linux

package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	specsVersion "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/semaphore"
)

// This uses the daemon's synchronous graph collector, physical content store,
// and overlay snapshotter. It never connects to the machine's running daemon.
func TestRuntimeOrphanGCPhysicalGraph(t *testing.T) {
	binary := os.Getenv("AGENT_VMM_TEST_CONTAINERD")
	if binary == "" {
		t.Skip("requires isolated containerd binary")
	}
	root := t.TempDir()
	address := filepath.Join(root, "containerd.sock")
	logFile, err := os.Create(filepath.Join(root, "containerd.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	config := filepath.Join(root, "config.toml")
	if err := os.WriteFile(config, []byte("version = 3\ndisabled_plugins = [\"io.containerd.cri.v1.images\", \"io.containerd.cri.v1.runtime\"]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	daemonCtx, stop := context.WithCancel(context.Background())
	command := exec.CommandContext(daemonCtx, binary, "--config", config, "--root", filepath.Join(root, "root"), "--state", filepath.Join(root, "state"), "--address", address, "--log-level", "error")
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stop()
		_ = command.Wait()
		if t.Failed() {
			data, _ := os.ReadFile(logFile.Name())
			t.Log(string(data))
		}
	}()
	client, err := containerd.New(address, containerd.WithTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if _, err := client.Version(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := ensureRuntimeContainerdNamespace(ctx, client); err != nil {
		t.Fatal(err)
	}
	nsctx := runtimeContext(ctx)
	staging, err := client.LeasesService().Create(nsctx, leases.WithRandomID())
	if err != nil {
		t.Fatal(err)
	}
	nsctx = leases.WithLease(nsctx, staging.ID)
	snapshots, store := client.SnapshotService(containerSnapshotter), client.ContentStore()
	mounts, err := snapshots.Prepare(nsctx, "lower-write", "")
	if err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(mounts[0].Source, "payload")
	if err := os.WriteFile(payload, bytes.Repeat([]byte("physical-sharing"), 65536), 0600); err != nil {
		t.Fatal(err)
	}
	lower := digest.FromString("physical-shared-lower")
	if err := snapshots.Commit(nsctx, lower.String(), "lower-write"); err != nil {
		t.Fatal(err)
	}
	configBytes, _ := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{lower}}, Config: ocispec.ImageConfig{Cmd: []string{"true"}}})
	configDesc := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(configBytes), Size: int64(len(configBytes))}
	if err := content.WriteBlob(nsctx, store, "shared-config", bytes.NewReader(configBytes), configDesc, content.WithLabels(map[string]string{"containerd.io/gc.ref.snapshot." + containerSnapshotter: lower.String()})); err != nil {
		t.Fatal(err)
	}
	index := newRuntimeInventoryIndex()
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default()}
	reference := "registry.example/image:latest"
	targets := make([]ocispec.Descriptor, 2)
	for i, namespace := range []string{"source-a", "source-b"} {
		manifest, _ := json.Marshal(ocispec.Manifest{Versioned: specsVersion.Versioned{SchemaVersion: 2}, Config: configDesc, Annotations: map[string]string{"fixture": namespace}})
		target := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
		targets[i] = target
		if err := content.WriteBlob(nsctx, store, namespace, bytes.NewReader(manifest), target, content.WithLabels(map[string]string{"containerd.io/gc.ref.content.config": configDesc.Digest.String()})); err != nil {
			t.Fatal(err)
		}
		if _, err := client.ImageService().Create(nsctx, coreimages.Image{Name: runtimeImageReference(target.Digest), Target: target}); err != nil {
			t.Fatal(err)
		}
		index.addRuntimeImage(runtimeImageReference(target.Digest))
		if err := client.NamespaceService().Create(ctx, namespace, map[string]string{}); err != nil {
			t.Fatal(err)
		}
		index.ensureNamespace(namespace)
		if _, err := client.ImageService().Create(namespaceContext(ctx, namespace), coreimages.Image{Name: reference, Target: target}); err != nil {
			t.Fatal(err)
		}
		if err := index.addImage(namespace, reference, target.Digest.String()); err != nil {
			t.Fatal(err)
		}
	}
	built := &vmmdv1.ImageOperationReceipt{RequestId: "built", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true, ImageReference: reference, ManifestDigest: targets[1].Digest.String()}
	if err := runtime.retainBuiltImage(ctx, "controller", "source-b", built); err != nil {
		t.Fatal(err)
	}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "accepted-copy", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ManifestDigest: targets[0].Digest.String(), ImageReference: reference}
	if err := client.NamespaceService().Create(ctx, "removed-target", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	run, _, err := runtime.acceptImageOperation(ctx, "removed-target", receipt, reference, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.NamespaceService().Delete(ctx, "removed-target"); err != nil {
		t.Fatal(err)
	}
	for i, namespace := range []string{"source-a", "source-b"} {
		record, err := client.ImageService().Get(namespaceContext(ctx, namespace), reference)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.DeleteImage(ctx, namespace, reference, imageGenerationID(record), targets[i].Digest.String()); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.LeasesService().Delete(nsctx, staging, leases.SynchronousDelete); err != nil {
		t.Fatal(err)
	}
	nsctx = runtimeContext(ctx)
	runtime.startRuntimeGC()
	defer runtime.stopRuntimeGC()
	// A synchronous pass proves both accepted-source and retained-build roots
	// survive ordinary GC without pressure eviction.
	if err := runtime.collectRuntimeImages(ctx); err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if _, err := client.ImageService().Get(nsctx, runtimeImageReference(target.Digest)); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.finishImageOperation("removed-target", receipt, run, context.Canceled, nil, true); err != nil {
		t.Fatal(err)
	}
	waitMissing := func(read func() error) {
		t.Helper()
		for {
			err := read()
			if errdefs.IsNotFound(err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("physical GC did not converge", ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	waitMissing(func() error {
		_, err := client.ImageService().Get(nsctx, runtimeImageReference(targets[0].Digest))
		return err
	})
	waitMissing(func() error { _, err := store.Info(nsctx, targets[0].Digest); return err })
	if _, err := snapshots.Stat(nsctx, lower.String()); err != nil {
		t.Fatal("shared lower collected", err)
	}
	if _, err := store.Info(nsctx, configDesc.Digest); err != nil {
		t.Fatal("shared config collected", err)
	}
	if _, err := snapshots.Prepare(nsctx, "active-upper", lower.String()); err != nil {
		t.Fatal(err)
	}
	active, err := client.NewContainer(nsctx, "active-container", containerd.WithSnapshotter(containerSnapshotter), containerd.WithSnapshot("active-upper"))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := runtime.retainedImage(ctx, "controller", reference, targets[1].Digest.String(), "built")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.deleteRetainedImage(ctx, "controller", reference, imageGenerationID(retained), targets[1].Digest.String()); err != nil {
		t.Fatal(err)
	}
	waitMissing(func() error {
		_, err := client.ImageService().Get(nsctx, runtimeImageReference(targets[1].Digest))
		return err
	})
	waitMissing(func() error { _, err := store.Info(nsctx, configDesc.Digest); return err })
	if _, err := snapshots.Stat(nsctx, lower.String()); err != nil {
		t.Fatal("active container lost shared lower", err)
	}
	if _, err := client.LoadContainer(nsctx, "active-container"); err != nil {
		t.Fatal("GC removed active container", err)
	}
	if err := active.Delete(nsctx, containerd.WithSnapshotCleanup); err != nil {
		t.Fatal(err)
	}
	waitMissing(func() error { _, err := snapshots.Stat(nsctx, lower.String()); return err })
	for {
		_, err := os.Stat(payload)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("physical lower remains", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
