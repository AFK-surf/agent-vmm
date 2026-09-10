//go:build linux

package build

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	runtimeoptions "github.com/containerd/containerd/api/types/runtimeoptions/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
)

const (
	InternalContainerLabel = "agentvmm.afk.surf/internal-build"
	builderArchive         = "/usr/lib/agent-vmm/buildkit.oci.tar"
	builderImage           = "agent-vmm.local/buildkit:0.30.0"
	builderSnapshotter     = "native"
	builderRuntime         = "io.containerd.runsc.v1"
	maxSecretBytes         = 1 << 20
	maxSecretsBytes        = 4 << 20
)

type Builder struct {
	client     *containerd.Client
	network    *guestnetwork.Manager
	semaphore  chan struct{}
	mu         sync.Mutex
	active     map[string]int
	namespace  map[string]*sync.Mutex
	growthGate func(context.Context, string, func() error) error
}

type Workspace struct {
	builder       *Builder
	header        *vmmdv1.BuildImageHeader
	id            string
	root          string
	contextRoot   string
	outputRoot    string
	secretRoot    string
	archive       *os.File
	contextBytes  int64
	contextClosed bool
	secretIndex   int
	secretFile    *os.File
	secretBytes   int64
	totalSecrets  int64
	closed        bool
	activeOnce    sync.Once
}

func NewBuilder(client *containerd.Client, network *guestnetwork.Manager, growthGate func(context.Context, string, func() error) error) *Builder {
	return &Builder{client: client, network: network, semaphore: make(chan struct{}, 2), active: make(map[string]int), namespace: make(map[string]*sync.Mutex), growthGate: growthGate}
}

func (b *Builder) growOCI(ctx context.Context, identity string, operation func() error) error {
	if b.growthGate == nil {
		return errors.New("builder OCI growth gate is unavailable")
	}
	return b.growthGate(ctx, identity, operation)
}

func (b *Builder) Begin(header *vmmdv1.BuildImageHeader) (*Workspace, error) {
	if err := validateHeader(header); err != nil {
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	root := filepath.Join("/state/build-tmp", header.GetActorNamespaceId(), id)
	secretRoot := filepath.Join("/run/agent-vmm-build", id)
	for _, path := range []string{filepath.Join(root, "context"), filepath.Join(root, "output", "tmp"), filepath.Join(root, "output", "home"), filepath.Join(root, "output", "buildkit"), secretRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			_ = os.RemoveAll(root)
			_ = os.RemoveAll(secretRoot)
			return nil, err
		}
	}
	for _, path := range []string{filepath.Join(root, "output"), filepath.Join(root, "output", "tmp"), filepath.Join(root, "output", "home"), filepath.Join(root, "output", "buildkit"), secretRoot} {
		if err := os.Chown(path, 1000, 1000); err != nil {
			_ = os.RemoveAll(root)
			_ = os.RemoveAll(secretRoot)
			return nil, err
		}
	}
	archive, err := os.OpenFile(filepath.Join(root, "context.tar"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		_ = os.RemoveAll(root)
		_ = os.RemoveAll(secretRoot)
		return nil, err
	}
	b.mu.Lock()
	b.active[header.GetActorNamespaceId()]++
	b.mu.Unlock()
	return &Workspace{
		builder: b, header: header, id: id, root: root,
		contextRoot: filepath.Join(root, "context"), outputRoot: filepath.Join(root, "output"),
		secretRoot: secretRoot, archive: archive,
	}, nil
}

func (b *Builder) CanDeleteNamespace(namespaceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[namespaceID] != 0 {
		return errors.New("namespace has active builds")
	}
	return nil
}

func (b *Builder) CleanupNamespace(ctx context.Context, namespaceID string) error {
	if err := b.CanDeleteNamespace(namespaceID); err != nil {
		return err
	}
	lock := b.namespaceLock(namespaceID)
	lock.Lock()
	defer lock.Unlock()
	nsctx := namespaces.WithNamespace(ctx, namespaceID)
	containers, err := b.client.Containers(nsctx)
	if err != nil {
		return err
	}
	for _, container := range containers {
		labels, err := container.Labels(nsctx)
		if err != nil {
			return err
		}
		if labels[InternalContainerLabel] != "true" {
			continue
		}
		if task, err := container.Task(nsctx, nil); err == nil {
			exit, waitErr := task.Wait(nsctx)
			if waitErr != nil {
				return waitErr
			}
			_ = task.Kill(nsctx, syscall.SIGKILL)
			select {
			case <-exit:
			case <-ctx.Done():
				return ctx.Err()
			}
			if _, err := task.Delete(nsctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
		} else if !errdefs.IsNotFound(err) {
			return err
		}
		if err := deleteBuildContainer(namespaceID, container); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workspace) AppendContext(data []byte, final bool) error {
	if w.contextClosed || w.secretIndex != 0 || w.secretFile != nil {
		return errors.New("context chunks are out of order")
	}
	w.contextBytes += int64(len(data))
	if w.contextBytes > MaxContextBytes {
		return errors.New("context archive exceeds size limit")
	}
	if _, err := w.archive.Write(data); err != nil {
		return err
	}
	if !final {
		return nil
	}
	if _, err := w.archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := ExtractContext(w.archive, w.contextRoot); err != nil {
		return err
	}
	if err := filepath.Walk(w.contextRoot, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Lchown(path, 1000, 1000)
	}); err != nil {
		return err
	}
	w.contextClosed = true
	return w.archive.Close()
}

func (w *Workspace) AppendSecret(id string, data []byte, final bool) error {
	if !w.contextClosed || w.secretIndex >= len(w.header.GetSecrets()) || id != w.header.GetSecrets()[w.secretIndex].GetId() {
		return errors.New("secret chunks are out of order")
	}
	if w.secretFile == nil {
		file, err := os.OpenFile(filepath.Join(w.secretRoot, id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if err := file.Chown(1000, 1000); err != nil {
			_ = file.Close()
			return err
		}
		w.secretFile = file
		w.secretBytes = 0
	}
	w.secretBytes += int64(len(data))
	w.totalSecrets += int64(len(data))
	if w.secretBytes > maxSecretBytes || w.totalSecrets > maxSecretsBytes {
		return errors.New("secret exceeds size limit")
	}
	if _, err := w.secretFile.Write(data); err != nil {
		return err
	}
	if final {
		if err := w.secretFile.Close(); err != nil {
			return err
		}
		w.secretFile = nil
		w.secretIndex++
	}
	return nil
}

// RunArchive holds the build slot and namespace owner until the existing
// output archive has been consumed, including runtime materialization.
func (w *Workspace) RunArchive(ctx context.Context, limits *vmmdv1.NamespaceLimits, progress func(string) error, consume func(*os.File, ArchiveResult) error) error {
	if !w.contextClosed || w.secretFile != nil || w.secretIndex != len(w.header.GetSecrets()) {
		return errors.New("build upload is incomplete")
	}
	dockerfile := filepath.Join(w.contextRoot, filepath.FromSlash(w.header.GetDockerfilePath()))
	if info, err := os.Stat(dockerfile); err != nil || !info.Mode().IsRegular() {
		return errors.New("Dockerfile is not a regular context file")
	}
	defer func() {
		if ctx.Err() != nil {
			w.releaseActive()
		}
	}()
	select {
	case w.builder.semaphore <- struct{}{}:
		defer func() { <-w.builder.semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}
	lock := w.builder.namespaceLock(w.header.GetActorNamespaceId())
	lock.Lock()
	defer lock.Unlock()
	cacheHit, err := w.run(ctx, limits, progress)
	if err != nil {
		return err
	}
	archive, err := os.Open(filepath.Join(w.outputRoot, "image.tar"))
	if err != nil {
		return err
	}
	defer archive.Close()
	result, err := copyOCIArchive(archive, io.Discard)
	if err != nil {
		return err
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	result.CacheHit = cacheHit
	return consume(archive, result)
}

func (w *Workspace) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.archive != nil {
		_ = w.archive.Close()
	}
	if w.secretFile != nil {
		_ = w.secretFile.Close()
	}
	w.releaseActive()
	return errors.Join(os.RemoveAll(w.secretRoot), os.RemoveAll(w.root))
}

func (w *Workspace) releaseActive() {
	w.activeOnce.Do(func() {
		w.builder.mu.Lock()
		w.builder.active[w.header.GetActorNamespaceId()]--
		if w.builder.active[w.header.GetActorNamespaceId()] == 0 {
			delete(w.builder.active, w.header.GetActorNamespaceId())
		}
		w.builder.mu.Unlock()
	})
}

func (w *Workspace) run(ctx context.Context, limits *vmmdv1.NamespaceLimits, progress func(string) error) (cacheHit bool, runErr error) {
	namespaceID := w.header.GetActorNamespaceId()
	nsctx := namespaces.WithNamespace(ctx, namespaceID)
	if err := os.Chown(filepath.Join("/state/build-cache", namespaceID), 1000, 1000); err != nil {
		return false, err
	}
	image, err := w.builder.ensureBuilderImage(nsctx)
	if err != nil {
		return false, err
	}
	netNSPath, detachNetwork, err := w.attachNetwork(ctx)
	if err != nil {
		return false, err
	}
	var detachOnce sync.Once
	detach := func() { detachOnce.Do(detachNetwork) }
	defer detach()
	containerID := "agent-vmm-build-" + w.id
	var container containerd.Container
	var task containerd.Task
	var exit <-chan containerd.ExitStatus
	output := new(buildOutput)
	defer func() {
		if container == nil {
			return
		}
		detach()
		cleanup, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespaceID), 10*time.Second)
		defer cancel()
		if task != nil {
			if exit == nil {
				exit, _ = task.Wait(cleanup)
			}
			_ = task.Kill(cleanup, syscall.SIGKILL)
			if exit != nil {
				select {
				case <-exit:
				case <-cleanup.Done():
				}
			}
		}
		cleanupErr := func() error {
			if task != nil {
				if _, err := task.Delete(cleanup, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
					return err
				}
			}
			return deleteBuildContainer(namespaceID, container)
		}()
		runErr = errors.Join(runErr, cleanupErr)
	}()
	err = func() error {
		var createErr error
		container, createErr = w.builder.client.NewContainer(nsctx, containerID,
			containerd.WithContainerLabels(map[string]string{InternalContainerLabel: "true"}),
			containerd.WithImage(image),
			containerd.WithSnapshotter(builderSnapshotter),
			containerd.WithNewSnapshotView(containerID+"-rootfs", image),
			containerd.WithRuntime(builderRuntime, &runtimeoptions.Options{TypeUrl: "io.containerd.runsc.v1.options", ConfigPath: "/etc/containerd/runsc-build.toml"}),
			containerd.WithNewSpec(w.specOptions(image, netNSPath, limits)...),
		)
		if createErr != nil {
			return createErr
		}
		task, createErr = container.NewTask(nsctx, cio.NewCreator(cio.WithStreams(nil, output, output)))
		if createErr != nil {
			return createErr
		}
		exit, createErr = task.Wait(namespaces.WithNamespace(context.Background(), namespaceID))
		if createErr != nil {
			return createErr
		}
		return task.Start(nsctx)
	}()
	if err != nil {
		return false, err
	}
	if progress != nil {
		if err := progress("builder task started"); err != nil {
			return false, err
		}
	}
	select {
	case status := <-exit:
		code, _, resultErr := status.Result()
		if resultErr != nil || code != 0 {
			return false, errors.New("isolated BuildKit build failed")
		}
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return output.Contains([]byte(" CACHED")), nil
}

func deleteBuildContainer(namespaceID string, container containerd.Container) error {
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespaceID), 10*time.Second)
	defer cancel()
	for {
		err := container.Delete(ctx, containerd.WithSnapshotCleanup)
		if err == nil || errdefs.IsNotFound(err) {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type buildOutput struct {
	mu   sync.Mutex
	data []byte
}

func (w *buildOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	const limit = 1 << 20
	remaining := limit - len(w.data)
	if remaining > 0 {
		w.data = append(w.data, data[:min(len(data), remaining)]...)
	}
	return len(data), nil
}

func (w *buildOutput) Contains(value []byte) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Contains(w.data, value)
}

func (w *Workspace) attachNetwork(ctx context.Context) (string, func(), error) {
	namespaceID, id := w.header.GetActorNamespaceId(), "build-"+w.id
	if w.header.GetNetworkMode() == vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE {
		path, err := w.builder.network.AttachLoopback(ctx, namespaceID, id)
		return path, func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = w.builder.network.DetachLoopback(cleanup, namespaceID, id, path)
		}, err
	}
	endpoint, err := w.builder.network.Attach(ctx, namespaceID, id, id)
	return endpoint.NetNSPath, func() { _ = w.builder.network.Detach(context.Background(), namespaceID, id, id) }, err
}

func (b *Builder) ensureBuilderImage(ctx context.Context) (containerd.Image, error) {
	if image, err := b.client.GetImage(ctx, builderImage); err == nil {
		unpacked, unpackErr := image.IsUnpacked(ctx, builderSnapshotter)
		if unpackErr != nil {
			return nil, unpackErr
		}
		if unpacked {
			return image, nil
		}
		if err := b.growOCI(ctx, "builder-image", func() error { return image.Unpack(ctx, builderSnapshotter) }); err != nil {
			return nil, err
		}
		return image, nil
	} else if !errdefs.IsNotFound(err) {
		return nil, err
	}
	var image containerd.Image
	err := b.growOCI(ctx, "builder-image", func() error {
		archive, err := os.Open(builderArchive)
		if err != nil {
			return err
		}
		defer archive.Close()
		if _, err := b.client.Import(ctx, archive, containerd.WithDigestRef(func(_ digest.Digest) string { return builderImage })); err != nil {
			return err
		}
		image, err = b.client.GetImage(ctx, builderImage)
		if err != nil {
			return err
		}
		return image.Unpack(ctx, builderSnapshotter)
	})
	if err != nil {
		return nil, err
	}
	return image, nil
}

func (b *Builder) namespaceLock(namespaceID string) *sync.Mutex {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.namespace[namespaceID] == nil {
		b.namespace[namespaceID] = new(sync.Mutex)
	}
	return b.namespace[namespaceID]
}

func randomID() (string, error) {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
