//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Runtime) StartContainer(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Container, error) {
	return r.startContainer(ctx, namespaceID, containerID, false)
}

func (r *Runtime) startContainer(ctx context.Context, namespaceID, containerID string, system bool, requests ...*vmmdv1.StartContainerRequest) (result *vmmdv1.Container, startErr error) {
	dispatched := false
	defer func() {
		if startErr == nil || dispatched {
			return
		}
		failure := status.Convert(rpcError(startErr))
		startErr = hostv1.NewError(failure.Code(), hostv1.ErrorReason_ERROR_REASON_CONTAINER_START_NOT_ADMITTED, failure.Message(), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
	}()
	var identityErr error
	if system {
		identityErr = validateSystemNamespace(namespaceID)
	} else {
		identityErr = validateID("actor_namespace_id", namespaceID)
	}
	if identityErr != nil {
		return nil, identityErr
	}
	if err := validateID("container_id", containerID); err != nil {
		return nil, err
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nsctx, container, current, err := r.loadContainerForRole(ctx, namespaceID, containerID, system)
	if err != nil {
		return nil, err
	}
	var request *vmmdv1.StartContainerRequest
	if len(requests) != 0 {
		request = requests[0]
	}
	if request != nil && request.GetExpectedGenerationId() != "" && request.GetExpectedGenerationId() != current.GetGenerationId() {
		return nil, status.Error(codes.FailedPrecondition, "container generation changed")
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return current, nil
	}
	for _, mount := range current.GetVolumes() {
		if _, err := r.mountSource(namespaceID, mount); err != nil {
			return nil, err
		}
	}
	unlockContainer, err := r.lockContainer(ctx, namespaceID, containerID)
	if err != nil {
		return nil, err
	}
	defer unlockContainer()
	unlock()
	unlock = nil
	current, err = r.inspectContainer(nsctx, namespaceID, container)
	if err != nil {
		return nil, err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return current, nil
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
		return nil, status.Error(codes.FailedPrecondition, "previous sandbox has not stopped; stop and reconcile it before restarting")
	}
	// A process can have exited while the containerd task is still being
	// reaped by monitorTask.  Do not race a replacement task with that cleanup.
	if err := r.removeExitedTask(nsctx, container); err != nil {
		return nil, err
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
	instanceID, err := newContainerInstanceID()
	if err != nil {
		return nil, err
	}
	endpoint, err := r.network.Attach(ctx, namespaceID, containerID, instanceID)
	if err != nil {
		return nil, err
	}
	if err := func() error { return container.Update(nsctx, withRuntimePlacement(namespaceID, endpoint.NetNSPath)) }(); err != nil {
		_ = r.network.Detach(context.Background(), namespaceID, containerID, instanceID)
		return nil, err
	}
	stdout, stderr, err := newRecordLog(logPath(namespaceID, containerID, current.GetGenerationId()), limit)
	if err != nil {
		_ = r.network.Detach(context.Background(), namespaceID, containerID, instanceID)
		return nil, err
	}
	lifecycleContext := runtimeContext(context.Background())
	requestHash := ""
	if request != nil {
		requestHash = containerRequestHash(request)
	}
	dispatched = true
	started, err := startTaskMetadata(nsctx, lifecycleContext, container, mainSpec, stdout, stderr, instanceID, requestHash)
	if err != nil {
		stdout.Close()
		stderr.Close()
		_ = r.network.Detach(context.Background(), namespaceID, containerID, instanceID)
		return nil, err
	}
	r.tasks.Add(1)
	go func() {
		defer r.tasks.Done()
		r.monitorTask(namespaceID, containerID, instanceID, container, started.task, started.mainProcess, started.mainExit, started.taskExit, stdout, stderr)
	}()
	return r.inspectContainer(nsctx, namespaceID, container)
}

type startedTask struct {
	task        containerd.Task
	mainProcess containerd.Process
	taskExit    <-chan containerd.ExitStatus
	mainExit    <-chan containerd.ExitStatus
}

func startTaskMetadata(nsctx, lifecycleContext context.Context, container containerd.Container, mainSpec *specs.Process, stdout, stderr io.Writer, instanceID string, requestHashes ...string) (started startedTask, runErr error) {
	runErr = func() error {
		var err error
		started.task, err = container.NewTask(nsctx, cio.NullIO)
		if err != nil {
			return err
		}
		started.taskExit, err = started.task.Wait(lifecycleContext)
		if err != nil {
			return err
		}
		if err = started.task.Start(nsctx); err != nil {
			return err
		}
		started.mainProcess, err = started.task.Exec(nsctx, mainProcessID, mainSpec, cio.NewCreator(cio.WithStreams(nil, stdout, stderr)))
		if err != nil {
			return err
		}
		started.mainExit, err = started.mainProcess.Wait(lifecycleContext)
		if err != nil {
			return err
		}
		if err = started.mainProcess.Start(nsctx); err != nil {
			return err
		}
		labels := map[string]string{containerStateLabel: "running", containerExitLabel: "", containerInstanceLabel: instanceID, containerStartedLabel: strconv.FormatInt(time.Now().UnixNano(), 10), containerFinishedLabel: ""}
		if len(requestHashes) != 0 && requestHashes[0] != "" {
			labels[containerStartRequestLabel] = requestHashes[0]
			labels[containerStartInstanceLabel] = instanceID
		}
		if _, err = container.SetLabels(nsctx, labels); err != nil {
			return err
		}
		return nil
	}()
	if runErr != nil {
		runErr = errors.Join(runErr, cleanupUnmonitoredTask(nsctx, started))
	}
	return started, runErr
}

func cleanupUnmonitoredTask(nsctx context.Context, started startedTask) error {
	// The request can be cancelled after containerd accepted NewTask. Preserve
	// its namespace identity, but give cleanup an independent bounded lifetime
	// because monitorTask has not taken ownership yet.
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(nsctx), 10*time.Second)
	defer cancel()
	if started.mainProcess != nil {
		_ = started.mainProcess.Kill(cleanupContext, syscall.SIGKILL)
		_, _ = started.mainProcess.Delete(cleanupContext, containerd.WithProcessKill)
	}
	if started.task == nil {
		return nil
	}
	if err := deleteTask(cleanupContext, started.task); err != nil {
		return fmt.Errorf("delete unmonitored container task: %w", err)
	}
	return nil
}

func (r *Runtime) monitorTask(namespaceID, containerID, instanceID string, container containerd.Container, task containerd.Task, mainProcess containerd.Process, mainExit, taskExit <-chan containerd.ExitStatus, logs ...io.Closer) {
	status := <-mainExit
	code, _, err := status.Result()
	if err != nil {
		code = containerd.UnknownExitStatus
	}
	defer func() {
		for _, log := range logs {
			_ = log.Close()
		}
	}()
	unlock, lockErr := r.lockContainer(context.Background(), namespaceID, containerID)
	if lockErr != nil {
		return
	}
	defer unlock()
	labels, labelsErr := container.Labels(runtimeContext(context.Background()))
	if labelsErr != nil || labels[containerInstanceLabel] != instanceID {
		return
	}
	processContext, cancelProcesses := context.WithTimeout(runtimeContext(context.Background()), 10*time.Second)
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
	cleanupErr := deleteTask(processContext, task)
	cancelProcesses()
	detachContext, cancelDetach := context.WithTimeout(runtimeContext(context.Background()), 5*time.Second)
	cleanupErr = errors.Join(cleanupErr, r.network.Detach(detachContext, namespaceID, containerID, instanceID))
	cancelDetach()
	if cleanupErr == nil {
		metadataContext, cancelMetadata := context.WithTimeout(runtimeContext(context.Background()), 5*time.Second)
		_, _ = container.SetLabels(metadataContext, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(uint64(code), 10), containerInstanceLabel: "", containerFinishedLabel: strconv.FormatInt(time.Now().UnixNano(), 10)})
		cancelMetadata()
	}
	r.notifyWaiters(namespaceID+"/"+containerID+"/"+instanceID, code)
}

func (r *Runtime) removeExitedTask(ctx context.Context, container containerd.Container) error {
	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	cleanup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return deleteTask(cleanup, task)
}

func deleteTask(ctx context.Context, task containerd.Task) error {
	if err := task.Kill(ctx, syscall.SIGKILL, containerd.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
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

func withRuntimePlacement(namespaceID, path string) containerd.UpdateContainerOpts {
	return func(_ context.Context, _ *containerd.Client, container *containers.Container) error {
		value, err := typeurl.UnmarshalAny(container.Spec)
		if err != nil {
			return err
		}
		spec := value.(*specs.Spec)
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		if !strings.HasPrefix(namespaceID, "system.") {
			if spec.Linux.Resources == nil {
				spec.Linux.Resources = &specs.LinuxResources{}
			}
			resources := spec.Linux.Resources
			resources.CPU, resources.Memory = nil, nil
			if resources.Unified == nil {
				resources.Unified = make(map[string]string)
			}
			for key, value := range containerResources(namespaceID, nil).Unified {
				resources.Unified[key] = value
			}
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
