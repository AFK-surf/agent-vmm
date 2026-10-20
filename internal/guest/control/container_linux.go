//go:build linux

package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInvalidEnvironment = errors.New("environment must contain valid KEY=VALUE entries; values may contain newlines but not NUL")

const (
	containerCreateRequestLabel = labelPrefix + "create-request-sha256"
	containerConfigurationLabel = labelPrefix + "configuration-fingerprint"
	containerStartRequestLabel  = labelPrefix + "start-request-sha256"
	containerStartInstanceLabel = labelPrefix + "start-request-instance"
	containerStateLabel         = labelPrefix + "state"
	containerExitLabel          = labelPrefix + "exit-code"
	containerVolumeLabel        = labelPrefix + "volumes"
	containerLogLimitLabel      = labelPrefix + "log-limit"
	runscContainerType          = "io.kubernetes.cri.container-type"
	runscSandbox                = "sandbox"
	containerSnapshotter        = "overlayfs"
	containerProcess            = labelPrefix + "process"
	containerInstanceLabel      = labelPrefix + "instance-id"
	containerStartedLabel       = labelPrefix + "started-at-unix-nano"
	containerFinishedLabel      = labelPrefix + "finished-at-unix-nano"
	mainProcessID               = "agent-vmm-main"
	supervisorHostPath          = "/usr/bin/agent-vmm-supervisor"
	supervisorPath              = "/.agent-vmm-supervisor"
)

func (r *Runtime) StopContainer(ctx context.Context, namespaceID, containerID string, timeout time.Duration) (*vmmdv1.Container, error) {
	return r.stopContainer(ctx, namespaceID, containerID, timeout, false)
}

func (r *Runtime) stopContainer(ctx context.Context, namespaceID, containerID string, timeout time.Duration, system bool) (*vmmdv1.Container, error) {
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
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	nsctx, container, current, err := r.loadContainerForRole(ctx, namespaceID, containerID, system)
	if err != nil {
		return nil, err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
		if err := r.removeExitedTask(nsctx, container); err != nil {
			return nil, err
		}
		if err := r.network.Detach(nsctx, namespaceID, containerID, current.GetInstanceId()); err != nil {
			return nil, err
		}
		return r.inspectContainer(nsctx, namespaceID, container)
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return current, nil
	}
	task, err := container.Task(nsctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			// The monitor may have reaped the task between inspect and stop.
			// Re-read the durable container record before exposing a transient
			// containerd NOT_FOUND to callers.
			if latest, inspectErr := r.inspectContainer(nsctx, namespaceID, container); inspectErr == nil && latest.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				return latest, nil
			}
		}
		return nil, err
	}
	mainProcess, err := task.LoadProcess(nsctx, mainProcessID, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			if latest, inspectErr := r.inspectContainer(nsctx, namespaceID, container); inspectErr == nil && latest.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				return latest, nil
			}
		}
		return nil, err
	}
	mainExit, err := mainProcess.Wait(runtimeContext(context.Background()))
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
	return r.waitContainerForRole(ctx, namespaceID, containerID, false)
}

func (r *Runtime) waitContainerForRole(ctx context.Context, namespaceID, containerID string, system bool) (uint32, error) {
	key := namespaceID + "/" + containerID
	_, container, current, err := r.loadContainerForRole(ctx, namespaceID, containerID, system)
	if err != nil {
		return 0, err
	}
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && current.ExitCode != nil {
		return current.GetExitCode(), nil
	}
	channel := r.addWaiter(key)
	defer r.removeWaiter(key, channel)
	labels, err := container.Labels(runtimeContext(ctx))
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

func (r *Runtime) InspectContainer(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Container, error) {
	_, _, container, err := r.loadContainer(ctx, namespaceID, containerID)
	return container, err
}

func (r *Runtime) ListContainers(ctx context.Context, namespaceID string) ([]*vmmdv1.Container, error) {
	return r.ListContainersBounded(ctx, namespaceID, 0)
}

func (r *Runtime) ListContainersBounded(ctx context.Context, namespaceID string, limit uint32) ([]*vmmdv1.Container, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return nil, err
	}
	containerIDs, err := inventory.containerIDs(namespaceID, limit)
	if err != nil {
		return nil, err
	}
	return r.loadIndexedContainers(ctx, namespaceID, containerIDs)
}

func (r *Runtime) ListContainersPage(ctx context.Context, namespaceID, after string, size uint32) ([]*vmmdv1.Container, string, error) {
	if size == 0 || size > vmmdv1.MaxContainerPageSize || len(after) > 200 {
		return nil, "", status.Error(codes.InvalidArgument, "container page exceeds its read budget")
	}
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, "", err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return nil, "", err
	}
	ids, err := inventory.containerPage(namespaceID, after, int(size)+1)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(ids) > int(size) {
		ids = ids[:size]
		next = ids[len(ids)-1]
	}
	containers, err := r.loadIndexedContainers(ctx, namespaceID, ids)
	return containers, next, err
}

func (r *Runtime) loadIndexedContainers(ctx context.Context, namespaceID string, containerIDs []string) ([]*vmmdv1.Container, error) {
	nsctx := runtimeContext(ctx)
	return indexedInventoryLoad(containerIDs, func(containerID string) (*vmmdv1.Container, error) {
		container, err := ownedRuntimeContainer(ctx, r.client, namespaceID, containerID)
		if err != nil {
			return nil, fmt.Errorf("load indexed runtime container %s/%s: %w", namespaceID, containerID, err)
		}
		item, err := r.inspectContainer(nsctx, namespaceID, container)
		if err != nil {
			return nil, fmt.Errorf("inspect indexed runtime container %s/%s: %w", namespaceID, containerID, err)
		}
		return item, nil
	})
}

func (r *Runtime) loadContainer(ctx context.Context, namespaceID, containerID string) (context.Context, containerd.Container, *vmmdv1.Container, error) {
	return r.loadContainerForRole(ctx, namespaceID, containerID, false)
}

func (r *Runtime) loadContainerForRole(ctx context.Context, namespaceID, containerID string, system bool) (context.Context, containerd.Container, *vmmdv1.Container, error) {
	if err := validateContainerIdentityForRole(namespaceID, containerID, system); err != nil {
		return nil, nil, nil, err
	}
	nsctx := runtimeContext(ctx)
	container, err := ownedRuntimeContainer(ctx, r.client, namespaceID, containerID)
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

func validateContainerIdentityForRole(namespaceID, containerID string, system bool) error {
	if system {
		if err := validateSystemNamespace(namespaceID); err != nil {
			return err
		}
	} else if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	return validateID("container_id", containerID)
}

func (r *Runtime) inspectContainer(ctx context.Context, namespaceID string, container containerd.Container) (*vmmdv1.Container, error) {
	info, err := container.Info(ctx)
	if err != nil {
		return nil, err
	}
	state := observedSandboxState(ctx, container, info.Labels, func() (bool, error) { return sandboxCgroupEmpty(namespaceID, info.Labels[runtimeExternalIDLabel]) })
	instanceID := info.Labels[containerInstanceLabel]
	if state == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED || state == vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
		instanceID = ""
	}
	externalID := info.Labels[runtimeExternalIDLabel]
	if externalID == "" || info.Labels[runtimeOwnerLabel] != namespaceID {
		return nil, errdefs.ErrNotFound
	}
	logicalImage := info.Labels[runtimeImageLabel]
	if logicalImage == "" {
		return nil, errdefs.ErrNotFound
	}
	result := &vmmdv1.Container{Id: externalID, NamespaceId: namespaceID, Image: logicalImage, State: state, InstanceId: instanceID, GenerationId: containerGenerationID(info.ID, info.CreatedAt)}
	result.CreateRequestSha256 = info.Labels[containerCreateRequestLabel]
	if fingerprint := info.Labels[containerConfigurationLabel]; fingerprint != "" {
		result.ConfigurationFingerprint = &fingerprint
	}
	result.StartRequestSha256 = info.Labels[containerStartRequestLabel]
	result.StartRequestInstanceId = info.Labels[containerStartInstanceLabel]
	result.CreatedAtUnixNano = info.CreatedAt.UnixNano()
	result.StartedAtUnixNano, err = parseTimestampLabel(info.Labels[containerStartedLabel])
	if err != nil {
		return nil, err
	}
	result.FinishedAtUnixNano, err = parseTimestampLabel(info.Labels[containerFinishedLabel])
	if err != nil {
		return nil, err
	}
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
		var err error
		result.Volumes, err = decodeContainerMounts(value)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func containerGenerationID(id string, createdAt time.Time) string {
	value := sha256.Sum256([]byte(id + "\x00" + strconv.FormatInt(createdAt.UnixNano(), 10)))
	return hex.EncodeToString(value[:])
}

func newContainerInstanceID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func parseTimestampLabel(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func validateEnvironment(environment []string) error {
	for _, value := range environment {
		key, _, ok := strings.Cut(value, "=")
		if !ok || !environmentKey.MatchString(key) || strings.ContainsRune(value, 0) {
			return errInvalidEnvironment
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
