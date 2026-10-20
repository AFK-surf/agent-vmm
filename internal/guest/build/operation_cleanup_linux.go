//go:build linux

package build

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

// CleanupExecution is only used after the accepted handler has stopped or after guest restart.
func (b *Builder) CleanupExecution(ctx context.Context, namespace, instance string) error {
	nsctx := namespaces.WithNamespace(ctx, namespace)
	container, err := b.client.LoadContainer(nsctx, "agent-vmm-build-"+instance)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if err == nil {
		task, err := container.Task(nsctx, nil)
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		if err == nil {
			exit, err := task.Wait(nsctx)
			if err != nil {
				return err
			}
			if err := task.Kill(nsctx, syscall.SIGKILL, containerd.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
			select {
			case <-exit:
			case <-ctx.Done():
				return ctx.Err()
			}
			if _, err := task.Delete(nsctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
		}
		if err := deleteBuildContainer(namespace, container); err != nil {
			return err
		}
	}
	if err := b.network.CleanupBuildEndpoint(ctx, namespace, "build-"+instance); err != nil {
		return err
	}
	return errors.Join(os.RemoveAll(filepath.Join("/state/build-tmp", namespace, instance)), os.RemoveAll(filepath.Join("/run/agent-vmm-build", instance)))
}

func (w *Workspace) RecoverClose(ctx context.Context) error {
	if err := w.builder.CleanupExecution(ctx, w.header.ActorNamespaceId, w.id); err != nil {
		return err
	}
	w.cleanupPending = false
	w.networkCleanupPending = false
	return w.Close()
}
