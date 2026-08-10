//go:build linux

package control

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	runtimeoptions "github.com/containerd/containerd/api/types/runtimeoptions/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
)

func (r *Runtime) CreateContainer(ctx context.Context, request *vmmdv1.CreateContainerRequest) (*vmmdv1.Container, error) {
	namespaceID := request.GetActorNamespaceId()
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	if err := validateID("container_id", request.GetContainerId()); err != nil {
		return nil, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	if request.GetImage() == "" {
		return nil, errors.New("image is required")
	}
	if len(request.GetEntrypointOverride().GetValues()) > 256 || len(request.GetCommandOverride().GetValues()) > 256 || len(request.GetEnv()) > 256 || request.GetLogLimitBytes() == 0 || request.GetLogLimitBytes() > 64<<20 {
		return nil, errors.New("entrypoint, command, env, or log limit is outside the allowed range")
	}
	if err := validateEnvironment(request.GetEnv()); err != nil {
		return nil, err
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
		if err := validateID("volume_id", mount.GetVolumeId()); err != nil {
			return nil, err
		}
		if err := validateDestination(mount.GetDestination()); err != nil {
			return nil, err
		}
		if info, err := os.Stat(volumePath(namespaceID, mount.GetVolumeId())); err != nil || !info.IsDir() {
			return nil, errors.New("named volume does not exist")
		}
	}
	nsctx := namespaceContext(ctx, namespaceID)
	canonicalImage, err := normalizeImageReference(request.GetImage())
	if err != nil {
		return nil, imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", request.GetImage())
	}
	image, err := r.client.GetImage(nsctx, canonicalImage)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, imageError(codes.NotFound, "IMAGE_NOT_FOUND", "image is not available in this environment", canonicalImage)
		}
		return nil, err
	}
	volumeJSON, err := json.Marshal(request.GetVolumes())
	if err != nil {
		return nil, err
	}
	container, err := r.client.NewContainer(nsctx, request.GetContainerId(),
		containerd.WithImage(image), containerd.WithSnapshotter(containerSnapshotter), containerd.WithNewSnapshot(request.GetContainerId()+"-rootfs", image),
		containerd.WithRuntime(runscRuntime, &runtimeoptions.Options{TypeUrl: "io.containerd.runsc.v1.options", ConfigPath: "/etc/containerd/runsc.toml"}),
		containerd.WithContainerLabels(map[string]string{containerStateLabel: "created", containerVolumeLabel: string(volumeJSON), containerLogLimitLabel: strconv.FormatUint(request.GetLogLimitBytes(), 10)}),
		containerd.WithNewSpec(r.specOptions(namespaceID, request, image)...), withSupervisedProcess(),
	)
	if err != nil {
		return nil, err
	}
	if err := r.assignSnapshotProject(nsctx, namespaceID, container); err != nil {
		_ = container.Delete(nsctx, containerd.WithSnapshotCleanup)
		return nil, err
	}
	current, err := r.inspectContainer(nsctx, namespaceID, container)
	if err != nil {
		_ = container.Delete(nsctx, containerd.WithSnapshotCleanup)
		return nil, err
	}
	if err := ensureDirectory(filepath.Dir(logPath(namespaceID, request.GetContainerId(), current.GetGenerationId())), 0o700); err != nil {
		_ = container.Delete(nsctx, containerd.WithSnapshotCleanup)
		return nil, err
	}
	return current, nil
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
		spec.Process = &specs.Process{Args: []string{supervisorPath}, Cwd: "/", User: specs.User{}, NoNewPrivileges: true, Capabilities: &specs.LinuxCapabilities{}}
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: supervisorPath, Type: "bind", Source: supervisorHostPath, Options: []string{"bind", "ro", "nosuid", "nodev"}})
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
	options := []oci.SpecOpts{oci.WithDefaultSpec(), oci.WithImageConfig(image), oci.WithCgroup(filepath.Join("/agent-vmm", namespaceID, request.GetContainerId()))}
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
		spec.Process.NoNewPrivileges = true
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		spec.Linux.Resources = containerResources(request.GetLimits())
		for _, mount := range request.GetVolumes() {
			mode := "rw"
			if mount.GetReadOnly() {
				mode = "ro"
			}
			spec.Mounts = append(spec.Mounts, specs.Mount{Destination: mount.GetDestination(), Type: "bind", Source: volumePath(namespaceID, mount.GetVolumeId()), Options: []string{"rbind", mode, "nosuid", "nodev"}})
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/etc/resolv.conf", Type: "bind", Source: r.network.ResolvPath(namespaceID), Options: []string{"bind", "ro", "nosuid", "nodev"}})
		return nil
	})
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

func containerResources(limits *vmmdv1.ResourceLimits) *specs.LinuxResources {
	resources := &specs.LinuxResources{}
	if limits == nil {
		return resources
	}
	if limits.CpuMillis != nil {
		period, quota := uint64(100000), int64(limits.GetCpuMillis())*100
		resources.CPU = &specs.LinuxCPU{Period: &period, Quota: &quota}
	}
	if limits.MemoryBytes != nil {
		value := int64(limits.GetMemoryBytes())
		resources.Memory = &specs.LinuxMemory{Limit: &value}
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
	for _, mount := range mounts {
		if mount.Type == "bind" && mount.Source != "" {
			return assignProject(mount.Source, projectID)
		}
		for _, option := range mount.Options {
			if upper, ok := strings.CutPrefix(option, "upperdir="); ok {
				return assignProject(upper, projectID)
			}
		}
	}
	return errors.New("writable snapshot has no project directory")
}
