//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

func (w *Workspace) run(ctx context.Context, limits *vmmdv1.NamespaceLimits, progress func(string) error) (cacheHit bool, runErr error) {
	if progress != nil {
		send := progress
		var progressMu sync.Mutex
		progress = func(text string) error {
			progressMu.Lock()
			defer progressMu.Unlock()
			return send(text)
		}
	}
	namespaceID := w.header.GetActorNamespaceId()
	nsctx := namespaces.WithNamespace(ctx, namespaceID)
	image, err := w.builder.ensureBuilderImage(nsctx)
	if err != nil {
		return false, err
	}
	if err := w.prepareBase(ctx); err != nil {
		return false, err
	}
	netNSPath, detachNetwork, err := w.attachNetwork(ctx)
	if err != nil {
		return false, err
	}
	w.networkCleanupPending = true
	var detachOnce sync.Once
	var detachErr error
	detach := func() error { detachOnce.Do(func() { detachErr = detachNetwork() }); return detachErr }
	defer func() {
		err := detach()
		if err == nil {
			w.networkCleanupPending = false
		}
		runErr = errors.Join(runErr, err)
	}()
	runErr = w.builder.growOCI(ctx, "cache-write\x00"+w.id, func() error {
		if err := os.MkdirAll(w.cacheRoot(), 0700); err != nil {
			return err
		}
		if err := os.Chown(w.cacheRoot(), 1000, 1000); err != nil {
			return err
		}
		var err error
		cacheHit, err = w.runTask(ctx, limits, progress, image, netNSPath, detach)
		if w.cleanupPending {
			return &WriterUnsettledError{Err: err}
		}
		return err
	})
	return cacheHit, runErr
}

func (w *Workspace) runTask(ctx context.Context, limits *vmmdv1.NamespaceLimits, progress func(string) error, image containerd.Image, netNSPath string, detach func() error) (cacheHit bool, runErr error) {
	namespaceID := w.header.GetActorNamespaceId()
	nsctx := namespaces.WithNamespace(ctx, namespaceID)
	runtimeOptions, startupLog, err := w.startupRuntimeOptions()
	if err != nil {
		return false, err
	}
	containerID := "agent-vmm-build-" + w.id
	var container containerd.Container
	var task containerd.Task
	var exit <-chan containerd.ExitStatus
	output := &buildOutput{progress: progress}
	var cleanup context.Context
	var cleanupCancel context.CancelFunc
	cleanupContext := func() context.Context {
		if cleanup == nil {
			cleanup, cleanupCancel = context.WithTimeout(namespaces.WithNamespace(context.Background(), namespaceID), 10*time.Second)
		}
		return cleanup
	}
	defer func() {
		if cleanupCancel != nil {
			cleanupCancel()
		}
	}()
	outputDrained := false
	taskExited := false
	defer func() {
		if container == nil {
			return
		}
		_ = detach()
		cleanup := cleanupContext()
		if task != nil {
			if exit == nil {
				exit, _ = task.Wait(cleanup)
			}
			if err := task.Kill(cleanup, syscall.SIGKILL, containerd.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
				runErr = errors.Join(runErr, err)
				return
			}
			if exit != nil && !taskExited {
				select {
				case <-exit:
				case <-cleanup.Done():
					runErr = errors.Join(runErr, cleanup.Err())
					return
				}
			}
		}
		if task != nil && !outputDrained {
			if err := waitBuildIO(cleanup, task.IO()); err != nil {
				runErr = errors.Join(runErr, err)
				return
			}
			outputDrained = true
		}
		cleanupErr := func() error {
			if task != nil {
				if _, err := task.Delete(cleanup, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
					return err
				}
			}
			return deleteBuildContainer(namespaceID, container)
		}()
		if cleanupErr == nil {
			w.cleanupPending = false
		}
		runErr = errors.Join(runErr, cleanupErr)
	}()
	err = func() error {
		var createErr error
		container, createErr = w.builder.client.NewContainer(nsctx, containerID,
			containerd.WithContainerLabels(map[string]string{InternalContainerLabel: "true"}),
			containerd.WithImage(image),
			containerd.WithSnapshotter(builderSnapshotter),
			containerd.WithNewSnapshotView(containerID+"-rootfs", image),
			containerd.WithRuntime(builderRuntime, runtimeOptions),
			containerd.WithNewSpec(w.specOptions(image, netNSPath, limits)...),
		)
		if createErr != nil {
			return createErr
		}
		w.cleanupPending = true
		task, createErr = container.NewTask(nsctx, cio.NewCreator(cio.WithStreams(nil, output, output)))
		if createErr != nil {
			return runtimeFailure(startupLog, createErr)
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
		taskExited = true
		code, _, resultErr := status.Result()
		if err := waitBuildIO(cleanupContext(), task.IO()); err != nil {
			return false, errors.Join(resultErr, err)
		}
		outputDrained = true
		if progress != nil {
			if err := progress(fmt.Sprintf("builder exited with code %d", code)); err != nil {
				return false, err
			}
		}
		if resultErr != nil {
			return false, resultErr
		}
		if code != 0 {
			return false, runtimeFailure(startupLog, output.failure(code))
		}
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return output.Cached(), nil
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

func (w *Workspace) attachNetwork(ctx context.Context) (string, func() error, error) {
	namespaceID, id := w.header.GetActorNamespaceId(), "build-"+w.id
	if w.header.GetNetworkMode() == vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE {
		path, err := w.builder.network.AttachLoopback(ctx, namespaceID, id)
		return path, func() error {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return w.builder.network.DetachLoopback(cleanup, namespaceID, id, path)
		}, err
	}
	endpoint, err := w.builder.network.Attach(ctx, namespaceID, id, id)
	return endpoint.NetNSPath, func() error { return w.builder.network.Detach(context.Background(), namespaceID, id, id) }, err
}
