//go:build linux

package control

import (
	"context"
	"strconv"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
)

func (r *Runtime) recoverTaskMonitors(ctx context.Context) error {
	nsctx := runtimeContext(ctx)
	containers, err := r.client.Containers(nsctx)
	if err != nil {
		return err
	}
	for _, container := range containers {
		info, err := container.Info(nsctx)
		if err != nil {
			return err
		}
		namespace := info.Labels[runtimeOwnerLabel]
		if namespace == "" {
			continue
		}
		task, err := container.Task(nsctx, nil)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		current, err := task.Status(nsctx)
		if err != nil {
			return err
		}
		if current.Status == containerd.Stopped {
			continue
		}
		if err := r.recoverTaskMonitor(nsctx, namespace, container, task, info.Labels, containerGenerationID(info.ID, info.CreatedAt)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) recoverTaskMonitor(ctx context.Context, namespace string, container containerd.Container, task containerd.Task, labels map[string]string, generation string) error {
	containerID := labels[runtimeExternalIDLabel]
	limit, err := strconv.ParseUint(labels[containerLogLimitLabel], 10, 64)
	if err != nil {
		return err
	}
	stdout, stderr, err := newRecordLog(logPath(namespace, containerID, generation), logMetaPath(namespace, containerID, generation), limit)
	if err != nil {
		return err
	}
	accepted := false
	defer func() {
		if !accepted {
			stdout.Close()
			stderr.Close()
		}
	}()
	lifecycle := runtimeContext(context.Background())
	taskExit, err := task.Wait(lifecycle)
	if err != nil {
		return err
	}
	process, err := task.LoadProcess(ctx, mainProcessID, cio.NewAttach(cio.WithStreams(nil, stdout, stderr)))
	if errdefs.IsNotFound(err) {
		cleanup, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := deleteTask(cleanup, task); err != nil {
			return err
		}
		if err := r.network.Detach(cleanup, namespace, containerID, labels[containerInstanceLabel]); err != nil {
			return err
		}
		_, err = container.SetLabels(ctx, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(containerd.UnknownExitStatus, 10), containerInstanceLabel: "", containerFinishedLabel: strconv.FormatInt(time.Now().UnixNano(), 10)})
		return err
	}
	if err != nil {
		return err
	}
	mainExit, err := process.Wait(lifecycle)
	if err != nil {
		return err
	}
	accepted = true
	r.tasks.Add(1)
	go func() {
		defer r.tasks.Done()
		r.monitorTask(namespace, containerID, labels[containerInstanceLabel], container, task, process, mainExit, taskExit, stdout, stderr)
	}()
	return nil
}
