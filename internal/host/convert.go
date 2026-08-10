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
	result := &hostv1.Container{Id: container.GetId(), Image: container.GetImage(), State: hostv1.ContainerState(container.GetState()), ExitCode: container.ExitCode, Argv: container.GetArgv(), Workdir: container.Workdir, User: container.User}
	for _, variable := range container.GetEnv() {
		key, _, _ := strings.Cut(variable, "=")
		result.EnvironmentKeys = append(result.EnvironmentKeys, key)
	}
	for _, volume := range container.GetVolumes() {
		result.Volumes = append(result.Volumes, &hostv1.VolumeMount{VolumeId: volume.GetVolumeId(), Destination: volume.GetDestination(), ReadOnly: volume.GetReadOnly()})
	}
	return result
}

func volumeFromGuest(volume *vmmdv1.Volume) *hostv1.Volume {
	if volume == nil {
		return nil
	}
	return &hostv1.Volume{Id: volume.GetId(), Usage: &hostv1.Usage{DiskBytes: volume.GetCurrentBytes()}, LimitBytes: volume.GetLimitBytes(), MountedBy: volume.GetMountedBy()}
}

func usageFromGuest(usage *vmmdv1.Usage) *hostv1.Usage {
	if usage == nil {
		return nil
	}
	return &hostv1.Usage{
		CpuNanos: usage.GetCpuNanos(), MemoryBytes: usage.GetMemoryBytes(), Pids: usage.GetPids(), ReadBytes: usage.GetReadBytes(),
		WriteBytes: usage.GetWriteBytes(), DiskBytes: usage.GetDiskBytes(), NetworkRxBytes: usage.GetNetworkRxBytes(), NetworkTxBytes: usage.GetNetworkTxBytes(),
		SampledAt: timestamppb.New(time.Unix(0, usage.GetSampledAtUnixNano())), Stale: usage.GetUnavailable(),
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
