//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
)

func (r *Runtime) cleanupTasks(ctx context.Context) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, namespaceID := range names {
		nsctx := namespaceContext(ctx, namespaceID)
		containers, err := r.client.Containers(nsctx)
		if err != nil {
			return err
		}
		for _, container := range containers {
			labels, err := container.Labels(nsctx)
			if err != nil {
				return err
			}
			task, err := container.Task(nsctx, nil)
			if labels[guestbuild.InternalContainerLabel] == "true" {
				if err == nil {
					_, _ = task.Delete(nsctx, containerd.WithProcessKill)
				} else if !errdefs.IsNotFound(err) {
					return err
				}
				if err := container.Delete(nsctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
					return err
				}
				continue
			}
			if errdefs.IsNotFound(err) {
				if labels[containerStateLabel] == "running" {
					_, _ = container.SetLabels(nsctx, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(containerd.UnknownExitStatus, 10)})
				}
				continue
			}
			if err != nil {
				return err
			}
			status, _ := task.Delete(nsctx, containerd.WithProcessKill)
			code := uint32(containerd.UnknownExitStatus)
			if status != nil && status.Error() == nil {
				code = status.ExitCode()
			}
			_, _ = container.SetLabels(nsctx, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(uint64(code), 10)})
		}
	}
	monitorsDone := make(chan struct{})
	go func() {
		r.tasks.Wait()
		close(monitorsDone)
	}()
	select {
	case <-monitorsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := os.RemoveAll("/run/containerd/fifo"); err != nil {
		return err
	}
	return os.MkdirAll("/run/containerd/fifo", 0o700)
}

type cappedLog struct {
	mu        sync.Mutex
	file      *os.File
	remaining uint64
}

func newCappedLog(path string, limit uint64) (*cappedLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return &cappedLog{file: file, remaining: limit}, nil
}

func (writer *cappedLog) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	original := len(data)
	if uint64(len(data)) > writer.remaining {
		data = data[:writer.remaining]
	}
	if len(data) != 0 {
		if _, err := writer.file.Write(data); err != nil {
			return 0, err
		}
		writer.remaining -= uint64(len(data))
	}
	return original, nil
}

func (writer *cappedLog) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.file.Close()
}

var _ io.WriteCloser = (*cappedLog)(nil)

func (r *Runtime) ReadLogs(namespaceID, containerID, stream string, tail uint64) ([]byte, error) {
	file, err := os.Open(logPath(namespaceID, containerID, stream))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if tail == 0 || tail > 1<<20 {
		tail = 1 << 20
	}
	if info.Size() > int64(tail) {
		if _, err := file.Seek(-int64(tail), io.SeekEnd); err != nil {
			return nil, err
		}
	}
	return io.ReadAll(io.LimitReader(file, int64(tail)))
}

func (r *Runtime) StreamLogs(ctx context.Context, request *vmmdv1.LogsRequest, send func(*vmmdv1.LogsResponse) error) error {
	tail := request.GetTailBytes()
	if tail == 0 || tail > 1<<20 {
		tail = 1 << 20
	}
	type source struct {
		file   *os.File
		stream vmmdv1.LogStream
	}
	var sources []source
	for _, item := range []struct {
		name   string
		stream vmmdv1.LogStream
	}{{"stdout", vmmdv1.LogStream_LOG_STREAM_STDOUT}, {"stderr", vmmdv1.LogStream_LOG_STREAM_STDERR}} {
		file, err := os.Open(logPath(request.GetActorNamespaceId(), request.GetContainerId(), item.name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if info.Size() > int64(tail) {
			if _, err := file.Seek(-int64(tail), io.SeekEnd); err != nil {
				return err
			}
		}
		sources = append(sources, source{file: file, stream: item.stream})
	}
	readAvailable := func() (bool, error) {
		read := false
		for _, source := range sources {
			for {
				buffer := make([]byte, 32<<10)
				count, err := source.file.Read(buffer)
				if count != 0 {
					read = true
					if err := send(&vmmdv1.LogsResponse{Stream: source.stream, Data: buffer[:count]}); err != nil {
						return false, err
					}
				}
				if errors.Is(err, io.EOF) || count == 0 {
					break
				}
				if err != nil {
					return false, err
				}
			}
		}
		return read, nil
	}
	if _, err := readAvailable(); err != nil || !request.GetFollow() {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			read, err := readAvailable()
			if err != nil {
				return err
			}
			container, err := r.InspectContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
			if err != nil {
				return err
			}
			if !read && container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				return nil
			}
		}
	}
}

func (r *Runtime) SignalContainer(ctx context.Context, namespaceID, containerID string, signal syscall.Signal) error {
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return errors.New("container is not running")
	}
	task, err := container.Task(nsctx, nil)
	if err != nil {
		return err
	}
	return task.Kill(nsctx, signal)
}
