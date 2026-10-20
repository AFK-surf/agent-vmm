//go:build darwin

package host

import (
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/netip"
)

func applianceOperationResponse(operation store.ApplianceOperation) *appliancev1.ApplianceOperationResponse {
	if operation.ID == "" {
		return nil
	}
	response := &appliancev1.ApplianceOperationResponse{Operation: &appliancev1.ApplianceOperation{OperationId: operation.ID,
		InstallationId: operation.InstallationID, RequestId: operation.RequestID, Kind: appliancev1.ApplianceOperationKind(operation.Kind),
		State: appliancev1.ApplianceOperationState(operation.State), Phase: appliancev1.ApplianceOperationPhase(operation.Phase), ExpectedRevision: operation.ExpectedRevision,
		TerminalReason: appliancev1.ApplianceTerminalReason(operation.TerminalReason), Receipt: append([]byte(nil), operation.Receipt...),
		StartedAt: timestamppb.New(operation.CreatedAt), UpdatedAt: timestamppb.New(operation.UpdatedAt),
		Error: &appliancev1.ApplianceOperationError{Reason: operation.ErrorReason, Message: operation.ErrorMessage}}}
	response.Operation.DeadlineUnixMillis = operation.DeadlineUnixMillis
	if operation.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE) {
		progress := new(appliancev1.ApplianceUpdateProgress)
		if proto.Unmarshal(operation.Receipt, progress) == nil {
			response.Operation.SourceArtifactId = progress.GetSourceArtifactId()
			response.Operation.TargetArtifactId = progress.GetRequest().GetArtifactId()
		}
	}
	return response
}

func controllerSystemIsolationCapacity(controller *Controller) store.ResourceAmount {
	return store.ResourceAmount{PIDs: uint64(controller.systemCapacity.PIDs), DiskBytes: controller.systemCapacity.DiskBytes}
}

func grantFromProto(installationID string, revision uint64, value *appliancev1.ApprovedGrant) store.ApplianceGrant {
	system := amountFromLimits(value.GetSystemLimits())
	result := store.ApplianceGrant{InstallationID: installationID, Revision: revision, System: system,
		PerEnvironment: amountFromLimits(value.GetCapacity().GetPerEnvironmentLimits()),
		LANEgress:      value.GetLanEgress(), EgressMode: int32(value.GetEgressMode()), MaxEgressMode: int32(value.GetCapacity().GetMaxEgressMode()),
		DynamicHostLoopback: dynamicGrantFromProto(value.GetDynamicHostLoopback())}
	for _, endpoint := range value.GetHostTcpEndpoints() {
		address, _ := netip.ParseAddr(endpoint.GetIp())
		result.HostTCPEndpoints = append(result.HostTCPEndpoints, netip.AddrPortFrom(address, uint16(endpoint.GetPort())))
	}
	return result
}

func dynamicGrantFromProto(value *appliancev1.DynamicHostLoopbackGrant) store.HostLoopbackFrontendCapacity {
	return store.HostLoopbackFrontendCapacity{MaxActiveLeases: value.GetMaxActiveLeases(), MaxActiveLeasesPerAllocation: value.GetMaxActiveLeasesPerAllocation(),
		MaxLeaseSeconds: value.GetMaxLeaseSeconds(), MaxConcurrentConnections: value.GetMaxConcurrentConnections(),
		MaxConcurrentConnectionsPerLease: value.GetMaxConcurrentConnectionsPerLease(), MaxFrameBytes: value.GetConnectionBounds().GetMaxFrameBytes(),
		MaxConnectionBytes: value.GetConnectionBounds().GetMaxTotalBytes(), ConnectionDeadlineSeconds: value.GetConnectionBounds().GetDeadlineSeconds(),
		ConnectionIdleSeconds: value.GetConnectionBounds().GetIdleSeconds(), MaxUseCount: value.GetMaxUseCount()}
}

func frontendsFromProto(values []*appliancev1.ControllerFrontend) []store.ApplianceFrontend {
	result := make([]store.ApplianceFrontend, 0, len(values))
	for _, value := range values {
		result = append(result, store.ApplianceFrontend{Name: value.GetName(), Transport: int32(value.GetTransport()),
			Exposure: int32(value.GetExposure()), MaxFrameBytes: value.GetBounds().GetMaxFrameBytes(),
			MaxTotalBytes: value.GetBounds().GetMaxTotalBytes(), DeadlineSeconds: value.GetBounds().GetDeadlineSeconds(),
			IdleSeconds: value.GetBounds().GetIdleSeconds(), MaxConcurrentStreams: value.GetBounds().GetMaxConcurrentStreams()})
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
				MaxConcurrentStreams: value.MaxConcurrentStreams}})
	}
	return result
}

func amountFromLimits(value *hostv1.ResourceLimits) store.ResourceAmount {
	return store.ResourceAmount{PIDs: uint64(value.GetPids()), DiskBytes: value.GetDiskBytes()}
}

func grantToProto(value store.ApplianceGrant) *appliancev1.ApprovedGrant {
	result := &appliancev1.ApprovedGrant{SystemLimits: limitsFromAmount(value.System),
		Capacity:  &appliancev1.CapacityGrant{PerEnvironmentLimits: limitsFromAmount(value.PerEnvironment), MaxEgressMode: hostv1.EgressMode(value.MaxEgressMode)},
		LanEgress: value.LANEgress, EgressMode: hostv1.EgressMode(value.EgressMode), Revision: value.Revision,
		DynamicHostLoopback: dynamicGrantToProto(value.DynamicHostLoopback)}
	for _, endpoint := range value.HostTCPEndpoints {
		result.HostTcpEndpoints = append(result.HostTcpEndpoints, &appliancev1.HostTcpEndpoint{Ip: endpoint.Addr().String(), Port: uint32(endpoint.Port())})
	}
	return result
}

func dynamicGrantToProto(value store.HostLoopbackFrontendCapacity) *appliancev1.DynamicHostLoopbackGrant {
	if value.MaxActiveLeases == 0 {
		return nil
	}
	return &appliancev1.DynamicHostLoopbackGrant{MaxActiveLeases: value.MaxActiveLeases,
		MaxActiveLeasesPerAllocation: value.MaxActiveLeasesPerAllocation, MaxLeaseSeconds: value.MaxLeaseSeconds,
		MaxConcurrentConnections: value.MaxConcurrentConnections, MaxConcurrentConnectionsPerLease: value.MaxConcurrentConnectionsPerLease, MaxUseCount: value.MaxUseCount,
		ConnectionBounds: &appliancev1.StreamBounds{MaxFrameBytes: value.MaxFrameBytes, MaxTotalBytes: value.MaxConnectionBytes,
			DeadlineSeconds: value.ConnectionDeadlineSeconds, IdleSeconds: value.ConnectionIdleSeconds,
			MaxConcurrentStreams: value.MaxConcurrentConnectionsPerLease}}
}

func limitsFromAmount(value store.ResourceAmount) *hostv1.ResourceLimits {
	pids := uint32(value.PIDs)
	disk := value.DiskBytes
	return &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk}
}

func remoteRegistrationFromGrant(id string, installation store.ApplianceInstallation, grant store.ApplianceGrant) store.RemoteRegistration {
	return store.RemoteRegistration{ID: id, Endpoint: "private.invalid:443", DisplayName: installation.ProductID, ProtocolVersion: remoteconnector.ProtocolVersion,
		PerEnvPIDs: uint32(grant.PerEnvironment.PIDs), PerEnvDiskBytes: grant.PerEnvironment.DiskBytes,
		MaxEgressMode:    grant.MaxEgressMode,
		StopGraceSeconds: 10, State: int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState: int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED), EnrollmentState: int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL),
		ConnectorPhase: int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_STOPPED), OwnerKind: store.RemoteRegistrationPrivate, InstallationID: installation.ID}
}

func controllerWorkloadIsolationCapacity(controller *Controller) store.ResourceAmount {
	return store.ResourceAmount{PIDs: uint64(controller.remoteCapacity.PIDs), DiskBytes: controller.remoteCapacity.DiskBytes}
}
