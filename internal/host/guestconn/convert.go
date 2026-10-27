package guestconn

import (
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Container(container *vmmdv1.Container) *hostv1.Container {
	if container == nil {
		return nil
	}
	result := &hostv1.Container{Id: container.GetId(), InstanceId: container.GetInstanceId(), GenerationId: container.GetGenerationId(), Image: container.GetImage(), State: hostv1.ContainerState(container.GetState()), ExitCode: container.ExitCode, Argv: container.GetArgv(), Workdir: container.Workdir, User: container.User}
	result.StartRequestInstanceId = container.GetStartRequestInstanceId()
	result.CreatedAt = Timestamp(container.GetCreatedAtUnixNano())
	result.StartedAt = Timestamp(container.GetStartedAtUnixNano())
	result.FinishedAt = Timestamp(container.GetFinishedAtUnixNano())
	for _, variable := range container.GetEnv() {
		key, _, _ := strings.Cut(variable, "=")
		result.EnvironmentKeys = append(result.EnvironmentKeys, key)
	}
	for _, volume := range container.GetVolumes() {
		mount := &hostv1.VolumeMount{Destination: volume.GetDestination(), ReadOnly: volume.GetReadOnly()}
		switch source := volume.Source.(type) {
		case *vmmdv1.VolumeMount_OwnedVolume:
			mount.Source = &hostv1.VolumeMount_OwnedVolume{OwnedVolume: source.OwnedVolume}
		case *vmmdv1.VolumeMount_HostShare:
			mount.Source = &hostv1.VolumeMount_HostShare{HostShare: source.HostShare}
		}
		result.Volumes = append(result.Volumes, mount)
	}
	return result
}

func Timestamp(value int64) *timestamppb.Timestamp {
	if value == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(0, value))
}

func Image(image *vmmdv1.Image) *hostv1.Image {
	if image == nil {
		return nil
	}
	return &hostv1.Image{Reference: image.GetReference(), Digest: image.GetDigest(), LogicalBytes: image.GetLogicalBytes(), Platforms: append([]string(nil), image.GetPlatforms()...), GenerationId: image.GetGenerationId(), BuildReceipt: ImageReceipt(image.GetBuildReceipt()), CopyReceipt: ImageReceipt(image.GetCopyReceipt())}
}

func Volume(volume *vmmdv1.Volume) *hostv1.Volume {
	if volume == nil {
		return nil
	}
	usage := &hostv1.Usage{DiskBytes: volume.GetCurrentBytes(), Stale: volume.GetUsageStale(), UnavailableReason: volume.GetUsageUnavailableReason()}
	if observedAt := volume.GetUsageObservedAtUnixNano(); observedAt > 0 {
		usage.SampledAt = timestamppb.New(time.Unix(0, observedAt))
	}
	return &hostv1.Volume{Id: volume.GetId(), Usage: usage, LimitBytes: volume.GetLimitBytes(), MountedBy: volume.GetMountedBy()}
}

func Usage(usage *vmmdv1.Usage) *hostv1.Usage {
	if usage == nil {
		return nil
	}
	return &hostv1.Usage{
		CpuNanos: usage.GetCpuNanos(), MemoryBytes: usage.GetMemoryBytes(), Pids: usage.GetPids(), ReadBytes: usage.GetReadBytes(),
		WriteBytes: usage.GetWriteBytes(), DiskBytes: usage.GetDiskBytes(), NetworkRxBytes: usage.GetNetworkRxBytes(), NetworkTxBytes: usage.GetNetworkTxBytes(),
		SampledAt: timestamppb.New(time.Unix(0, usage.GetSampledAtUnixNano())), Stale: usage.GetUnavailable(), UnavailableReason: usage.GetUnavailableReason(),
	}
}

func ImageReceipt(receipt *vmmdv1.ImageOperationReceipt) *hostv1.ImageOperationReceipt {
	if receipt == nil {
		return nil
	}
	return &hostv1.ImageOperationReceipt{RequestId: receipt.RequestId, InputSha256: receipt.InputSha256, Kind: hostv1.ImageOperationKind(receipt.Kind), State: hostv1.ImageOperationState(receipt.State), WriterSettled: receipt.WriterSettled, ExecutionId: receipt.ExecutionId, ContainerId: receipt.ContainerId, ExpectedInstanceId: receipt.ExpectedInstanceId, ImageReference: receipt.ImageReference, ManifestDigest: receipt.ManifestDigest, Status: receipt.Status, RuntimeInstanceId: receipt.RuntimeInstanceId}
}
