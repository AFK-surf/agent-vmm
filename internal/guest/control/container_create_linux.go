//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	runtimeoptions "github.com/containerd/containerd/api/types/runtimeoptions/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func containerRequestHash(request proto.Message) string {
	body, _ := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	return fmt.Sprintf("%x", sha256.Sum256(body))
}

func (r *Runtime) CreateContainer(ctx context.Context, request *vmmdv1.CreateContainerRequest) (*vmmdv1.Container, error) {
	return r.createContainer(ctx, request, false, nil)
}

func (r *Runtime) createContainer(ctx context.Context, request *vmmdv1.CreateContainerRequest, system bool, systemOption oci.SpecOpts) (_ *vmmdv1.Container, createErr error) {
	dispatched := false
	defer func() {
		if createErr == nil || dispatched {
			return
		}
		failure := status.Convert(rpcError(createErr))
		createErr = hostv1.NewError(failure.Code(), hostv1.ErrorReason_ERROR_REASON_CONTAINER_CREATE_NOT_ADMITTED, failure.Message(), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
	}()
	namespaceID := request.GetActorNamespaceId()
	if system {
		if err := validateSystemNamespace(namespaceID); err != nil {
			return nil, err
		}
	} else if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("container_id", request.GetContainerId()); err != nil {
		return nil, err
	}
	if len(request.GetEntrypointOverride().GetValues()) > 256 || len(request.GetCommandOverride().GetValues()) > 256 || len(request.GetEnv()) > 256 || request.GetLogLimitBytes() == 0 || request.GetLogLimitBytes() > 64<<20 {
		return nil, status.Error(codes.InvalidArgument, "entrypoint, command, env, or log limit is outside the allowed range")
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	unlockContainer, err := r.lockContainer(ctx, namespaceID, request.GetContainerId())
	if err != nil {
		return nil, err
	}
	defer unlockContainer()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	if request.GetImage() == "" {
		return nil, errors.New("image is required")
	}

	if err := validateEnvironment(request.GetEnv()); err != nil {
		return nil, hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_ENVIRONMENT_INVALID, err.Error(), hostv1.WithResource("container", request.GetContainerId()))
	}
	processValues := append([]string(nil), request.GetEntrypointOverride().GetValues()...)
	processValues = append(processValues, request.GetCommandOverride().GetValues()...)
	if err := validateProcessStrings(processValues, request.GetWorkdir(), request.GetUser()); err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	if err := validateResourceLimits(request.GetLimits(), namespace.GetLimits()); err != nil {
		return nil, err
	}
	for _, mount := range request.GetVolumes() {
		if err := validateDestination(mount.GetDestination()); err != nil {
			return nil, err
		}
		source, err := r.mountSource(namespaceID, mount)
		if err != nil {
			return nil, err
		}
		if info, err := os.Stat(source); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("mount source is unavailable: %s", mount.GetDestination())
		}
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	canonicalImage, err := normalizeImageReference(request.GetImage())
	if err != nil {
		return nil, hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_IMAGE_REFERENCE_INVALID, "image reference is invalid", hostv1.WithResource("image", request.GetImage()))
	}
	logicalImage, err := r.client.GetImage(logicalContext, canonicalImage)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_IMAGE_NOT_FOUND, "image is not available in this environment", hostv1.WithResource("image", canonicalImage))
		}
		return nil, err
	}
	nsctx := runtimeContext(ctx)
	image, err := r.client.GetImage(nsctx, runtimeImageReference(logicalImage.Target().Digest))
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_IMAGE_NOT_FOUND, "image is not available in this environment", hostv1.WithResource("image", canonicalImage))
		}
		return nil, err
	}
	unpacked, err := image.IsUnpacked(nsctx, containerSnapshotter)
	if err != nil {
		return nil, status.Error(codes.Internal, "image unpack state could not be read")
	}
	if !unpacked {
		return nil, status.Error(codes.FailedPrecondition, "image root filesystem is not materialized; import the exact image again")
	}
	volumeJSON, err := encodeContainerMounts(request.GetVolumes())
	if err != nil {
		return nil, err
	}
	specOptions := r.specOptions(namespaceID, request, image)
	if systemOption != nil {
		specOptions = append(specOptions, systemOption)
	}
	snapshot := containerd.WithNewSnapshot(runtimeSnapshotKey(namespaceID, request.GetContainerId()), image)
	if system {
		snapshot = containerd.WithNewSnapshotView(runtimeSnapshotKey(namespaceID, request.GetContainerId()), image)
	}
	dispatched = true
	container, err := r.client.NewContainer(nsctx, runtimeContainerKey(namespaceID, request.GetContainerId()),
		containerd.WithImage(image), containerd.WithSnapshotter(containerSnapshotter),
		atContainerStage("rootfs_snapshot", snapshot),
		containerd.WithRuntime(runscRuntime, &runtimeoptions.Options{TypeUrl: "io.containerd.runsc.v1.options", ConfigPath: "/etc/containerd/runsc.toml"}),
		containerd.WithContainerLabels(map[string]string{containerCreateRequestLabel: containerRequestHash(request), containerConfigurationLabel: request.GetConfigurationFingerprint(), containerStateLabel: "created", containerVolumeLabel: string(volumeJSON), containerLogLimitLabel: strconv.FormatUint(request.GetLogLimitBytes(), 10), runtimeOwnerLabel: namespaceID, runtimeExternalIDLabel: request.GetContainerId(), runtimeImageLabel: canonicalImage}),
		atContainerStage("oci_spec", containerd.WithNewSpec(specOptions...)), atContainerStage("supervisor_spec", withSupervisedProcess()),
	)
	if err != nil {
		return nil, atRuntimeStage("container_record", err)
	}
	if system {
		if err := validateSystemSnapshotMountpoints(nsctx, r.client, container); err != nil {
			cleanupErr := r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
			return nil, errors.Join(err, cleanupErr)
		}
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		_ = r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
		return nil, atRuntimeStage("container_inventory", err)
	}
	if err := inventory.addContainer(namespaceID, request.GetContainerId()); err != nil {
		_ = r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
		return nil, atRuntimeStage("container_inventory", err)
	}
	if !system {
		if err := r.assignSnapshotProject(nsctx, namespaceID, container); err != nil {
			r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
			return nil, atRuntimeStage("snapshot_quota", err)
		}

	}
	current, err := r.inspectContainer(nsctx, namespaceID, container)
	if err != nil {
		r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
		return nil, atRuntimeStage("container_projection", err)
	}
	if err := ensureDirectory(filepath.Dir(logPath(namespaceID, request.GetContainerId(), current.GetGenerationId())), 0o700); err != nil {
		r.rollbackNewContainer(nsctx, namespaceID, request.GetContainerId(), container)
		return nil, atRuntimeStage("container_log_state", err)
	}
	return current, nil
}

func (r *Runtime) rollbackNewContainer(ctx context.Context, namespaceID, containerID string, container containerd.Container) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := container.Delete(cleanupContext, containerd.WithSnapshotCleanup); err != nil {
		return err
	}
	if inventory, inventoryErr := r.requireRuntimeInventory(); inventoryErr == nil {
		return inventory.removeContainer(namespaceID, containerID)
	}
	return nil
}

func atContainerStage(stage string, option containerd.NewContainerOpts) containerd.NewContainerOpts {
	return func(ctx context.Context, client *containerd.Client, container *containers.Container) error {
		return atRuntimeStage(stage, option(ctx, client, container))
	}
}

func withSupervisedProcess() containerd.NewContainerOpts {
	return func(_ context.Context, _ *containerd.Client, container *containers.Container) error {
		value, err := typeurl.UnmarshalAny(container.Spec)
		if err != nil {
			return err
		}
		spec, ok := value.(*specs.Spec)
		if !ok || spec.Process == nil {
			return errors.New("container has no OCI process")
		}
		process, err := typeurl.MarshalAny(spec.Process)
		if err != nil {
			return err
		}
		if container.Extensions == nil {
			container.Extensions = make(map[string]typeurl.Any)
		}
		container.Extensions[containerProcess] = process
		spec.Process = &specs.Process{Args: []string{supervisorPath}, Cwd: "/", User: specs.User{}, NoNewPrivileges: true, Capabilities: &specs.LinuxCapabilities{},
			Rlimits: []specs.POSIXRlimit{{Type: "RLIMIT_CORE", Hard: 0, Soft: 0}}}
		container.Spec, err = typeurl.MarshalAny(spec)
		return err
	}
}

func containerMainProcess(ctx context.Context, container containerd.Container) (*specs.Process, error) {
	extensions, err := container.Extensions(ctx)
	if err != nil {
		return nil, err
	}
	value := extensions[containerProcess]
	if value == nil {
		return nil, errors.New("container has no main process")
	}
	process := new(specs.Process)
	if err := typeurl.UnmarshalTo(value, process); err != nil {
		return nil, err
	}
	return process, nil
}

func (r *Runtime) specOptions(namespaceID string, request *vmmdv1.CreateContainerRequest, image containerd.Image) []oci.SpecOpts {
	options := []oci.SpecOpts{oci.WithDefaultSpec(), oci.WithImageConfig(image), oci.WithCgroup(filepath.Join(resourcev2.NamespaceCgroup(namespaceID), request.GetContainerId()))}
	if request.GetEntrypointOverride() != nil || request.GetCommandOverride() != nil {
		options = append(options, processOverride(image, request.GetEntrypointOverride(), request.GetCommandOverride()))
	}
	if len(request.GetEnv()) != 0 {
		options = append(options, oci.WithEnv(request.GetEnv()))
	}
	if request.Workdir != nil {
		options = append(options, oci.WithProcessCwd(request.GetWorkdir()))
	}
	if request.User != nil {
		options = append(options, oci.WithUser(request.GetUser()))
	}
	if request.GetReadOnlyRoot() {
		options = append(options, oci.WithRootFSReadonly())
	}
	return append(options, func(_ context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
		if spec.Annotations == nil {
			spec.Annotations = make(map[string]string)
		}
		spec.Annotations[runscContainerType] = runscSandbox
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: supervisorPath, Type: "bind", Source: supervisorHostPath, Options: []string{"bind", "ro", "nosuid", "nodev", "exec"}})
		spec.Process.NoNewPrivileges = true
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		spec.Linux.Resources = containerResources(namespaceID, request.GetLimits())
		for _, mount := range request.GetVolumes() {
			mode := "rw"
			if mount.GetReadOnly() {
				mode = "ro"
			}
			source, err := r.mountSource(namespaceID, mount)
			if err != nil {
				return err
			}
			spec.Mounts = append(spec.Mounts, specs.Mount{Destination: mount.GetDestination(), Type: "bind", Source: source, Options: []string{"rbind", mode, "nosuid", "nodev"}})
		}
		if request.GetReadOnlyRoot() && !containerHasVolumeAt(request, "/tmp") {
			spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"rw", "nosuid", "nodev", "noexec", "mode=1777", "size=64m"}})
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/etc/resolv.conf", Type: "bind", Source: r.network.ResolvPath(namespaceID), Options: []string{"bind", "ro", "nosuid", "nodev"}})
		return nil
	})
}

func containerHasVolumeAt(request *vmmdv1.CreateContainerRequest, destination string) bool {
	for _, volume := range request.GetVolumes() {
		if filepath.Clean(volume.GetDestination()) == destination {
			return true
		}
	}
	return false
}

func processOverride(image containerd.Image, entrypointOverride, commandOverride *vmmdv1.StringList) oci.SpecOpts {
	return func(ctx context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
		imageSpec, err := image.Spec(ctx)
		if err != nil {
			return err
		}
		spec.Process.Args = processArguments(imageSpec.Config.Entrypoint, imageSpec.Config.Cmd, entrypointOverride, commandOverride)
		if len(spec.Process.Args) == 0 {
			return errors.New("image and overrides provide no process command")
		}
		return nil
	}
}

func processArguments(imageEntrypoint, imageCommand []string, entrypointOverride, commandOverride *vmmdv1.StringList) []string {
	entrypoint := append([]string(nil), imageEntrypoint...)
	command := append([]string(nil), imageCommand...)
	if entrypointOverride != nil {
		entrypoint = append([]string(nil), entrypointOverride.GetValues()...)
	}
	if commandOverride != nil {
		command = append([]string(nil), commandOverride.GetValues()...)
	}
	return append(entrypoint, command...)
}

func containerResources(namespaceID string, limits *vmmdv1.ResourceLimits) *specs.LinuxResources {
	resources := &specs.LinuxResources{}
	system := strings.HasPrefix(namespaceID, "system.")
	if !system {
		resources.Unified = map[string]string{"cpu.max": "max 100000", "cpu.weight": "100", "memory.max": "max", "memory.high": "max"}
	}
	if limits == nil {
		return resources
	}
	if limits.Pids != nil {
		value := int64(limits.GetPids())
		resources.Pids = &specs.LinuxPids{Limit: &value}
	}
	return resources
}

func (r *Runtime) assignSnapshotProject(ctx context.Context, namespaceID string, container containerd.Container) error {
	labels, err := r.client.NamespaceService().Labels(context.Background(), namespaceID)
	if err != nil {
		return err
	}
	projectID, _, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return err
	}
	info, err := container.Info(ctx)
	if err != nil {
		return err
	}
	mounts, err := r.client.SnapshotService(info.Snapshotter).Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return err
	}
	return assignSnapshotMountProject(mounts, projectID)
}

func assignSnapshotMountProject(mounts []mount.Mount, projectID uint32) error {
	for _, item := range mounts {
		if item.Type == "bind" && item.Source != "" {
			return assignProject(item.Source, projectID)
		}
		var upper, work string
		for _, option := range item.Options {
			if value, ok := strings.CutPrefix(option, "upperdir="); ok {
				upper = value
			}
			if value, ok := strings.CutPrefix(option, "workdir="); ok {
				work = value
			}
		}
		if upper != "" && work != "" {
			// Overlay copy-up creates a temporary inode in workdir and renames
			// it into upperdir. Both must belong to the writable project.
			if err := assignProject(work, projectID); err != nil {
				return err
			}
			return assignProject(upper, projectID)
		}
	}
	return errors.New("writable snapshot has no project directories")
}
