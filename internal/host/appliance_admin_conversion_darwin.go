//go:build darwin

package host

import (
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func applianceOperationResponse(operation store.ApplianceOperation) *appliancev1.ApplianceOperationResponse {
	if operation.ID == "" {
		return nil
	}
	return &appliancev1.ApplianceOperationResponse{Operation: &appliancev1.ApplianceOperation{OperationId: operation.ID,
		InstallationId: operation.InstallationID, RequestId: operation.RequestID, Kind: appliancev1.ApplianceOperationKind(operation.Kind),
		State: appliancev1.ApplianceOperationState(operation.State), Phase: appliancev1.ApplianceOperationPhase(operation.Phase), ExpectedRevision: operation.ExpectedRevision,
		TerminalReason: appliancev1.ApplianceTerminalReason(operation.TerminalReason), Receipt: append([]byte(nil), operation.Receipt...),
		StartedAt: timestamppb.New(operation.CreatedAt), UpdatedAt: timestamppb.New(operation.UpdatedAt),
		Error: &appliancev1.ApplianceOperationError{Reason: operation.ErrorReason, Message: operation.ErrorMessage}}}
}

func controllerCapacity(controller *Controller) store.ResourceAmount {
	return store.ResourceAmount{CPUMillis: uint64(controller.systemCapacity.CPUMillis), MemoryBytes: controller.systemCapacity.MemoryBytes, PIDs: uint64(controller.systemCapacity.PIDs), DiskBytes: controller.systemCapacity.DiskBytes}
}

func grantFromProto(installationID string, revision uint64, value *appliancev1.ApprovedGrant) store.ApplianceGrant {
	system := amountFromLimits(value.GetSystemResources().GetCeiling())
	return store.ApplianceGrant{InstallationID: installationID, Revision: revision, System: system,
		PerEnvironment:  amountFromLimits(value.GetCapacity().GetPerEnvironmentLimits()),
		MaxEnvironments: value.GetCapacity().GetMaxEnvironments(), MaxLeaseSeconds: value.GetCapacity().GetMaxLeaseSeconds(),
		DisconnectGraceSeconds: value.GetCapacity().GetDisconnectGraceSeconds(), EgressMode: int32(value.GetEgressMode()), MaxEgressMode: int32(value.GetCapacity().GetMaxEgressMode()),
		DynamicHostLoopback: dynamicGrantFromProto(value.GetDynamicHostLoopback())}
}

func dynamicGrantFromProto(value *appliancev1.DynamicHostLoopbackGrant) store.HostLoopbackFrontendCapacity {
	return store.HostLoopbackFrontendCapacity{MaxActiveLeases: value.GetMaxActiveLeases(), MaxActiveLeasesPerAllocation: value.GetMaxActiveLeasesPerAllocation(),
		MaxLeaseSeconds: value.GetMaxLeaseSeconds(), MaxConcurrentConnections: value.GetMaxConcurrentConnections(),
		MaxConcurrentConnectionsPerLease: value.GetMaxConcurrentConnectionsPerLease(), MaxFrameBytes: value.GetConnectionBounds().GetMaxFrameBytes(),
		MaxConnectionBytes: value.GetConnectionBounds().GetMaxTotalBytes(), ConnectionDeadlineSeconds: value.GetConnectionBounds().GetDeadlineSeconds(),
		ConnectionIdleSeconds: value.GetConnectionBounds().GetIdleSeconds(), MaxUseCount: value.GetConnectionBounds().GetMaxUseCount()}
}

func frontendsFromProto(values []*appliancev1.ControllerFrontend) []store.ApplianceFrontend {
	result := make([]store.ApplianceFrontend, 0, len(values))
	for _, value := range values {
		result = append(result, store.ApplianceFrontend{Name: value.GetName(), Transport: int32(value.GetTransport()),
			Exposure: int32(value.GetExposure()), MaxFrameBytes: value.GetBounds().GetMaxFrameBytes(),
			MaxTotalBytes: value.GetBounds().GetMaxTotalBytes(), DeadlineSeconds: value.GetBounds().GetDeadlineSeconds(),
			IdleSeconds: value.GetBounds().GetIdleSeconds(), MaxConcurrentStreams: value.GetBounds().GetMaxConcurrentStreams(),
			MaxUseCount: value.GetBounds().GetMaxUseCount()})
	}
	return result
}

func frontendsToProto(values []store.ApplianceFrontend) []*appliancev1.ControllerFrontend {
	result := make([]*appliancev1.ControllerFrontend, 0, len(values))
	for _, value := range values {
		result = append(result, &appliancev1.ControllerFrontend{Name: value.Name,
			Transport: appliancev1.FrontendTransport(value.Transport), Exposure: appliancev1.FrontendExposure(value.Exposure),
			Bounds: &appliancev1.StreamBounds{MaxFrameBytes: value.MaxFrameBytes,
				MaxTotalBytes: value.MaxTotalBytes, DeadlineSeconds: value.DeadlineSeconds, IdleSeconds: value.IdleSeconds,
				MaxConcurrentStreams: value.MaxConcurrentStreams, MaxUseCount: value.MaxUseCount}})
	}
	return result
}

func amountFromLimits(value *hostv1.ResourceLimits) store.ResourceAmount {
	return store.ResourceAmount{CPUMillis: uint64(value.GetCpuMillis()), MemoryBytes: value.GetMemoryBytes(), PIDs: uint64(value.GetPids()), DiskBytes: value.GetDiskBytes()}
}

func grantToProto(value store.ApplianceGrant) *appliancev1.ApprovedGrant {
	return &appliancev1.ApprovedGrant{SystemResources: &appliancev1.ResourceRange{Floor: limitsFromAmount(value.System), Ceiling: limitsFromAmount(value.System)},
		Capacity:   &appliancev1.CapacityGrant{MaxEnvironments: value.MaxEnvironments, PerEnvironmentLimits: limitsFromAmount(value.PerEnvironment), MaxEgressMode: hostv1.EgressMode(value.MaxEgressMode), MaxLeaseSeconds: value.MaxLeaseSeconds, DisconnectGraceSeconds: value.DisconnectGraceSeconds},
		EgressMode: hostv1.EgressMode(value.EgressMode), Revision: value.Revision,
		DynamicHostLoopback: dynamicGrantToProto(value.DynamicHostLoopback)}
}

func dynamicGrantToProto(value store.HostLoopbackFrontendCapacity) *appliancev1.DynamicHostLoopbackGrant {
	if value.MaxActiveLeases == 0 {
		return nil
	}
	return &appliancev1.DynamicHostLoopbackGrant{MaxActiveLeases: value.MaxActiveLeases,
		MaxActiveLeasesPerAllocation: value.MaxActiveLeasesPerAllocation, MaxLeaseSeconds: value.MaxLeaseSeconds,
		MaxConcurrentConnections: value.MaxConcurrentConnections, MaxConcurrentConnectionsPerLease: value.MaxConcurrentConnectionsPerLease,
		ConnectionBounds: &appliancev1.StreamBounds{MaxFrameBytes: value.MaxFrameBytes, MaxTotalBytes: value.MaxConnectionBytes,
			DeadlineSeconds: value.ConnectionDeadlineSeconds, IdleSeconds: value.ConnectionIdleSeconds,
			MaxConcurrentStreams: value.MaxConcurrentConnectionsPerLease, MaxUseCount: value.MaxUseCount}}
}

func limitsFromAmount(value store.ResourceAmount) *hostv1.ResourceLimits {
	cpu, pids := uint32(value.CPUMillis), uint32(value.PIDs)
	memory, disk := value.MemoryBytes, value.DiskBytes
	return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
}

func remoteRegistrationFromGrant(id string, installation store.ApplianceInstallation, grant store.ApplianceGrant) store.RemoteRegistration {
	return store.RemoteRegistration{ID: id, Endpoint: "private.invalid:443", DisplayName: installation.ProductID, ProtocolVersion: remoteconnector.ProtocolVersion,
		MaxEnvironments: grant.MaxEnvironments,
		PerEnvCPUMillis: uint32(grant.PerEnvironment.CPUMillis), PerEnvMemoryBytes: grant.PerEnvironment.MemoryBytes,
		PerEnvPIDs: uint32(grant.PerEnvironment.PIDs), PerEnvDiskBytes: grant.PerEnvironment.DiskBytes,
		MaxEgressMode: grant.MaxEgressMode, MaxLeaseSeconds: grant.MaxLeaseSeconds, DisconnectGraceSeconds: grant.DisconnectGraceSeconds,
		StopGraceSeconds: 10, State: int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED), EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL),
		ConnectorPhase: int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_STOPPED), OwnerKind: store.RemoteRegistrationPrivate, InstallationID: installation.ID}
}
