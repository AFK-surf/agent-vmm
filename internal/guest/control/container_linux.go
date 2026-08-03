//go:build linux

package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	containerStateLabel    = labelPrefix + "state"
	containerExitLabel     = labelPrefix + "exit-code"
	containerVolumeLabel   = labelPrefix + "volumes"
	containerLogLimitLabel = labelPrefix + "log-limit"
	runscContainerType     = "io.kubernetes.cri.container-type"
	runscSandbox           = "sandbox"
	containerSnapshotter   = "native"
	containerProcess       = labelPrefix + "process"
	mainProcessID          = "agent-vmm-main"
	supervisorHostPath     = "/usr/bin/agent-vmm-supervisor"
	supervisorPath         = "/run/agent-vmm/supervisor"
)

func (r *Runtime) StartContainer(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Container, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("container_id", containerID); err != nil {
		return nil, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return nil, err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return current, nil
	}
	labels, err := container.Labels(nsctx)
	if err != nil {
		return nil, err
	}
	limit, err := strconv.ParseUint(labels[containerLogLimitLabel], 10, 64)
	if err != nil {
		return nil, err
	}
	mainSpec, err := containerMainProcess(nsctx, container)
	if err != nil {
		return nil, err
	}
	endpoint, err := r.network.Attach(ctx, namespaceID, containerID)
	if err != nil {
		return nil, err
	}
	if err := container.Update(nsctx, withNetworkNamespace(endpoint.NetNSPath)); err != nil {
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	stdout, stderr, err := newRecordLog(logPath(namespaceID, containerID), logMetaPath(namespaceID, containerID), limit)
	if err != nil {
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	task, err := container.NewTask(nsctx, cio.NullIO)
	if err != nil {
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	lifecycleContext := namespaceContext(context.Background(), namespaceID)
	taskExit, err := task.Wait(lifecycleContext)
	if err != nil {
		_, _ = task.Delete(nsctx, containerd.WithProcessKill)
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	if err := task.Start(nsctx); err != nil {
		_, _ = task.Delete(nsctx)
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	mainProcess, err := task.Exec(nsctx, mainProcessID, mainSpec, cio.NewCreator(cio.WithStreams(nil, stdout, stderr)))
	if err != nil {
		_, _ = task.Delete(nsctx, containerd.WithProcessKill)
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	mainExit, err := mainProcess.Wait(lifecycleContext)
	if err != nil {
		_, _ = mainProcess.Delete(nsctx, containerd.WithProcessKill)
		_, _ = task.Delete(nsctx, containerd.WithProcessKill)
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	if err := mainProcess.Start(nsctx); err != nil {
		_, _ = mainProcess.Delete(nsctx, containerd.WithProcessKill)
		_, _ = task.Delete(nsctx, containerd.WithProcessKill)
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	if _, err := container.SetLabels(nsctx, map[string]string{containerStateLabel: "running", containerExitLabel: ""}); err != nil {
		_ = mainProcess.Kill(nsctx, syscall.SIGKILL)
		_, _ = task.Delete(nsctx, containerd.WithProcessKill)
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID)
		return nil, err
	}
	r.tasks.Add(1)
	go func() {
		defer r.tasks.Done()
		r.monitorTask(namespaceID, containerID, container, task, mainProcess, mainExit, taskExit, stdout, stderr)
	}()
	return r.inspectContainer(nsctx, namespaceID, container)
}

func (r *Runtime) monitorTask(namespaceID, containerID string, container containerd.Container, task containerd.Task, mainProcess containerd.Process, mainExit, taskExit <-chan containerd.ExitStatus, logs ...io.Closer) {
	status := <-mainExit
	code, _, err := status.Result()
	if err != nil {
		code = containerd.UnknownExitStatus
	}
	processContext, cancelProcesses := context.WithTimeout(namespaceContext(context.Background(), namespaceID), 10*time.Second)
	_ = task.Kill(processContext, syscall.SIGTERM)
	select {
	case <-taskExit:
	case <-time.After(time.Second):
		_ = task.Kill(processContext, syscall.SIGKILL)
		select {
		case <-taskExit:
		case <-processContext.Done():
		}
	}
	_, _ = mainProcess.Delete(processContext, containerd.WithProcessKill)
	_ = deleteTask(processContext, task)
	cancelProcesses()
	_ = r.network.Detach(context.Background(), namespaceID, containerID)
	for _, log := range logs {
		_ = log.Close()
	}
	metadataContext, cancelMetadata := context.WithTimeout(namespaceContext(context.Background(), namespaceID), 5*time.Second)
	_, _ = container.SetLabels(metadataContext, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(uint64(code), 10)})
	cancelMetadata()
	r.notifyWaiters(namespaceID+"/"+containerID, code)
}

func deleteTask(ctx context.Context, task containerd.Task) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if _, lastErr = task.Delete(ctx, containerd.WithProcessKill); lastErr == nil || errdefs.IsNotFound(lastErr) {
			return nil
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-ticker.C:
		}
	}
}

func withNetworkNamespace(path string) containerd.UpdateContainerOpts {
	return func(_ context.Context, _ *containerd.Client, container *containers.Container) error {
		value, err := typeurl.UnmarshalAny(container.Spec)
		if err != nil {
			return err
		}
		spec := value.(*specs.Spec)
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		filtered := spec.Linux.Namespaces[:0]
		for _, namespace := range spec.Linux.Namespaces {
			if namespace.Type != specs.NetworkNamespace {
				filtered = append(filtered, namespace)
			}
		}
		spec.Linux.Namespaces = append(filtered, specs.LinuxNamespace{Type: specs.NetworkNamespace, Path: path})
		container.Spec, err = typeurl.MarshalAny(spec)
		return err
	}
}

func (r *Runtime) StopContainer(ctx context.Context, namespaceID, containerID string, timeout time.Duration) (*vmmdv1.Container, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("container_id", containerID); err != nil {
		return nil, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return nil, err
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return current, nil
	}
	task, err := container.Task(nsctx, nil)
	if err != nil {
		return nil, err
	}
	mainProcess, err := task.LoadProcess(nsctx, mainProcessID, nil)
	if err != nil {
		return nil, err
	}
	mainExit, err := mainProcess.Wait(namespaceContext(context.Background(), namespaceID))
	if err != nil {
		return nil, err
	}
	key := namespaceID + "/" + containerID
	wait := r.addWaiter(key)
	defer r.removeWaiter(key, wait)
	labels, err := container.Labels(nsctx)
	if err != nil {
		return nil, err
	}
	if labels[containerStateLabel] == "stopped" {
		return r.inspectContainer(nsctx, namespaceID, container)
	}
	if err := mainProcess.Kill(nsctx, syscall.SIGTERM); err != nil {
		return nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-mainExit:
	case <-timer.C:
		if err := mainProcess.Kill(nsctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
			return nil, err
		}
		select {
		case <-mainExit:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-wait:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return r.inspectContainer(nsctx, namespaceID, container)
}

func (r *Runtime) WaitContainer(ctx context.Context, namespaceID, containerID string) (uint32, error) {
	key := namespaceID + "/" + containerID
	_, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return 0, err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && current.ExitCode != nil {
		return current.GetExitCode(), nil
	}
	channel := r.addWaiter(key)
	defer r.removeWaiter(key, channel)
	labels, err := container.Labels(namespaceContext(ctx, namespaceID))
	if err == nil && labels[containerStateLabel] == "stopped" {
		code, _ := strconv.ParseUint(labels[containerExitLabel], 10, 32)
		return uint32(code), nil
	}
	select {
	case code := <-channel:
		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *Runtime) addWaiter(key string) chan uint32 {
	channel := make(chan uint32, 1)
	r.mu.Lock()
	r.waiters[key] = append(r.waiters[key], channel)
	r.mu.Unlock()
	return channel
}

func (r *Runtime) DeleteContainer(ctx context.Context, namespaceID, containerID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	if err := validateID("container_id", containerID); err != nil {
		return err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return failedPrecondition("running container cannot be deleted")
	}
	if task, err := container.Task(nsctx, nil); err == nil {
		cleanup, cancel := context.WithTimeout(nsctx, 10*time.Second)
		err = deleteTask(cleanup, task)
		cancel()
		if err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	if err := container.Delete(nsctx, containerd.WithSnapshotCleanup); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(logPath(namespaceID, containerID)))
}

func (r *Runtime) InspectContainer(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Container, error) {
	_, _, container, err := r.loadContainer(ctx, namespaceID, containerID)
	return container, err
}

func (r *Runtime) ListContainers(ctx context.Context, namespaceID string) ([]*vmmdv1.Container, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	nsctx := namespaceContext(ctx, namespaceID)
	containers, err := r.client.Containers(nsctx)
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Container, 0, len(containers))
	for _, container := range containers {
		labels, err := container.Labels(nsctx)
		if err != nil {
			return nil, err
		}
		if labels[guestbuild.InternalContainerLabel] == "true" {
			continue
		}
		item, err := r.inspectContainer(nsctx, namespaceID, container)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func (r *Runtime) loadContainer(ctx context.Context, namespaceID, containerID string) (context.Context, containerd.Container, *vmmdv1.Container, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, nil, nil, err
	}
	if err := validateID("container_id", containerID); err != nil {
		return nil, nil, nil, err
	}
	nsctx := namespaceContext(ctx, namespaceID)
	container, err := r.client.LoadContainer(nsctx, containerID)
	if err != nil {
		return nil, nil, nil, err
	}
	labels, err := container.Labels(nsctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if labels[guestbuild.InternalContainerLabel] == "true" {
		return nil, nil, nil, errdefs.ErrNotFound
	}
	result, err := r.inspectContainer(nsctx, namespaceID, container)
	return nsctx, container, result, err
}

func (r *Runtime) inspectContainer(ctx context.Context, namespaceID string, container containerd.Container) (*vmmdv1.Container, error) {
	info, err := container.Info(ctx)
	if err != nil {
		return nil, err
	}
	state := vmmdv1.ContainerState_CONTAINER_STATE_CREATED
	if info.Labels[containerStateLabel] == "running" {
		state = vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
	} else if info.Labels[containerStateLabel] == "stopped" {
		state = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	}
	result := &vmmdv1.Container{Id: info.ID, NamespaceId: namespaceID, Image: info.Image, State: state}
	process, err := containerMainProcess(ctx, container)
	if err != nil {
		return nil, err
	}
	result.Argv = append([]string(nil), process.Args...)
	result.Env = append([]string(nil), process.Env...)
	if process.Cwd != "" {
		workdir := process.Cwd
		result.Workdir = &workdir
	}
	user := process.User.Username
	if user == "" {
		user = strconv.FormatUint(uint64(process.User.UID), 10) + ":" + strconv.FormatUint(uint64(process.User.GID), 10)
	}
	result.User = &user
	if value := info.Labels[containerExitLabel]; value != "" {
		code, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, err
		}
		exitCode := uint32(code)
		result.ExitCode = &exitCode
	}
	if value := info.Labels[containerVolumeLabel]; value != "" {
		if err := json.Unmarshal([]byte(value), &result.Volumes); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func validateEnvironment(environment []string) error {
	for _, value := range environment {
		key, _, ok := strings.Cut(value, "=")
		if !ok || !environmentKey.MatchString(key) || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("environment must contain KEY=VALUE entries")
		}
	}
	return nil
}

func validateProcessStrings(argv []string, workdir, user string) error {
	for _, value := range argv {
		if value == "" || len(value) > 1<<20 || strings.ContainsRune(value, 0) {
			return errors.New("argv contains an empty or invalid value")
		}
	}
	if workdir != "" && (!filepath.IsAbs(workdir) || filepath.Clean(workdir) != workdir || strings.ContainsRune(workdir, 0)) {
		return errors.New("workdir must be a clean absolute path")
	}
	if strings.ContainsRune(user, 0) {
		return errors.New("user contains an invalid value")
	}
	return nil
}

func (r *Runtime) notifyWaiters(key string, code uint32) {
	r.mu.Lock()
	waiters := r.waiters[key]
	delete(r.waiters, key)
	r.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- code
		close(waiter)
	}
}

func (r *Runtime) removeWaiter(key string, target chan uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters := r.waiters[key]
	for index, waiter := range waiters {
		if waiter == target {
			r.waiters[key] = append(waiters[:index], waiters[index+1:]...)
			break
		}
	}
}
