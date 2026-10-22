package host

import (
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func environmentFromStore(environment store.Environment) *hostv1.AgentEnvironment {
	origin := hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL
	if environment.OwnerKind == store.OwnerRemoteRegistration {
		origin = hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE
	}
	return &hostv1.AgentEnvironment{
		Id:          environment.ID,
		DisplayName: environment.DisplayName,
		NamespaceId: environment.NamespaceID,
		Limits: &hostv1.ResourceLimits{
			CpuMillis:   optional(environment.CPUMillis),
			MemoryBytes: optional(environment.MemoryBytes),
			Pids:        optional(environment.PIDs),
			DiskBytes:   optional(environment.DiskBytes),
		},
		EgressMode:     hostv1.EgressMode(environment.EgressMode),
		PolicyRevision: environment.PolicyRevision,
		Origin:         origin,
		Owner: &hostv1.EnvironmentOwner{
			Kind:           hostv1.EnvironmentOwnerKind(environment.OwnerKind),
			RegistrationId: environment.RegistrationID,
			AllocationId:   environment.AllocationID,
		},
	}
}

func sessionEnvironmentFromStore(environment store.Environment) *hostv1.AgentEnvironment {
	result := environmentFromStore(environment)
	result.Id = ""
	result.NamespaceId = ""
	result.Owner = nil
	return result
}

func containerFromGuest(container *vmmdv1.Container) *hostv1.Container {
	if container == nil {
		return nil
	}
	result := &hostv1.Container{Id: container.GetId(), InstanceId: container.GetInstanceId(), GenerationId: container.GetGenerationId(), Image: container.GetImage(), State: hostv1.ContainerState(container.GetState()), ExitCode: container.ExitCode, Argv: container.GetArgv(), Workdir: container.Workdir, User: container.User}
	result.StartRequestInstanceId = container.GetStartRequestInstanceId()
	result.CreatedAt = timestampFromUnixNano(container.GetCreatedAtUnixNano())
	result.StartedAt = timestampFromUnixNano(container.GetStartedAtUnixNano())
	result.FinishedAt = timestampFromUnixNano(container.GetFinishedAtUnixNano())
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

func containerConfigurationObservation(container *vmmdv1.Container, expected *hostv1.CreateContainerRequest) *hostv1.Container {
	result := containerFromGuest(container)
	if expected != nil && result != nil {
		matches := container.GetConfigurationFingerprint() == containerConfigurationFingerprint(expected)
		result.ConfigurationMatches = &matches
	}
	return result
}

func timestampFromUnixNano(value int64) *timestamppb.Timestamp {
	if value == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(0, value))
}

func imageFromGuest(image *vmmdv1.Image) *hostv1.Image {
	if image == nil {
		return nil
	}
	return &hostv1.Image{Reference: image.GetReference(), Digest: image.GetDigest(), LogicalBytes: image.GetLogicalBytes(), Platforms: append([]string(nil), image.GetPlatforms()...), GenerationId: image.GetGenerationId(), BuildReceipt: imageReceiptFromGuest(image.GetBuildReceipt()), CopyReceipt: imageReceiptFromGuest(image.GetCopyReceipt())}
}

func volumeFromGuest(volume *vmmdv1.Volume) *hostv1.Volume {
	if volume == nil {
		return nil
	}
	usage := &hostv1.Usage{DiskBytes: volume.GetCurrentBytes(), Stale: volume.GetUsageStale(), UnavailableReason: volume.GetUsageUnavailableReason()}
	if observedAt := volume.GetUsageObservedAtUnixNano(); observedAt > 0 {
		usage.SampledAt = timestamppb.New(time.Unix(0, observedAt))
	}
	return &hostv1.Volume{Id: volume.GetId(), Usage: usage, LimitBytes: volume.GetLimitBytes(), MountedBy: volume.GetMountedBy()}
}

func usageFromGuest(usage *vmmdv1.Usage) *hostv1.Usage {
	if usage == nil {
		return nil
	}
	return &hostv1.Usage{
		CpuNanos: usage.GetCpuNanos(), MemoryBytes: usage.GetMemoryBytes(), Pids: usage.GetPids(), ReadBytes: usage.GetReadBytes(),
		WriteBytes: usage.GetWriteBytes(), DiskBytes: usage.GetDiskBytes(), NetworkRxBytes: usage.GetNetworkRxBytes(), NetworkTxBytes: usage.GetNetworkTxBytes(),
		SampledAt: timestamppb.New(time.Unix(0, usage.GetSampledAtUnixNano())), Stale: usage.GetUnavailable(), UnavailableReason: usage.GetUnavailableReason(),
	}
}

func operationFromStore(operation store.Operation) *hostv1.Operation {
	resource := &hostv1.ResourceReference{Kind: "environment", EnvironmentId: operation.EnvironmentID}
	if strings.HasPrefix(operation.Kind, "vm-") {
		resource = &hostv1.ResourceReference{Kind: "vm", ResourceId: "local"}
	} else if operation.EnvironmentID == "" || operation.EnvironmentID == "system" {
		resource = &hostv1.ResourceReference{Kind: "system", ResourceId: "local"}
	}
	result := &hostv1.Operation{
		Id: operation.ID, Kind: operation.Kind, State: hostv1.OperationState(operation.State), Stage: operation.Stage,
		RequestId: operation.RequestID, StartedAt: timestamppb.New(operation.StartedAt), UpdatedAt: timestamppb.New(operation.UpdatedAt),
		Resource:   resource,
		OutputPath: operation.OutputPath,
	}
	if operation.ErrorReason != "" {
		result.Error = &hostv1.OperationError{Reason: operation.ErrorReason, Message: operation.ErrorMessage, Resource: result.Resource}
	}
	return result
}

func optional[T any](value T) *T { return &value }

func imageReceiptFromGuest(receipt *vmmdv1.ImageOperationReceipt) *hostv1.ImageOperationReceipt {
	if receipt == nil {
		return nil
	}
	return &hostv1.ImageOperationReceipt{RequestId: receipt.RequestId, InputSha256: receipt.InputSha256, Kind: hostv1.ImageOperationKind(receipt.Kind), State: hostv1.ImageOperationState(receipt.State), WriterSettled: receipt.WriterSettled, ExecutionId: receipt.ExecutionId, ContainerId: receipt.ContainerId, ExpectedInstanceId: receipt.ExpectedInstanceId, ImageReference: receipt.ImageReference, ManifestDigest: receipt.ManifestDigest, Status: receipt.Status, RuntimeInstanceId: receipt.RuntimeInstanceId}
}
