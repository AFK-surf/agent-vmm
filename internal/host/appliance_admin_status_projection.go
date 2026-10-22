//go:build darwin || linux

package host

import (
	"context"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (server *applianceAdminServer) applianceStatus(ctx context.Context, installation store.ApplianceInstallation, runtime *vmmdv1.SystemAppliance) *appliancev1.ApplianceStatus {
	result := &appliancev1.ApplianceStatus{InstallationId: installation.ID, ProductId: installation.ProductID,
		ArtifactId: installation.ArtifactID, Revision: installation.Revision,
		ApplianceState: appliancev1.ApplianceState(installation.State), RegistrationId: installation.RegistrationID,
		Enabled: installation.DesiredEnabled, BootId: installation.CurrentBootID,
		ContainerInstance: installation.CurrentContainerInstance}
	var registration store.RemoteRegistration
	if value, err := server.controller.store.GetRemoteRegistration(ctx, installation.RegistrationID); err == nil {
		registration = value
		result.InventoryWatermark = registration.InventoryRevision
	}
	if grant, err := server.controller.store.GetApplianceGrant(ctx, installation.ID); err == nil {
		result.ApprovedGrant = grantToProto(grant)
		if frontends, err := server.controller.store.ListApplianceFrontends(ctx, installation.ID); err == nil {
			result.ApprovedGrant.Frontends = frontendsToProto(frontends)
			for _, frontend := range frontends {
				endpoint, available := server.controller.applianceFrontendProjection(installation.ID, frontend.Name, installation.CurrentContainerInstance)
				result.Frontends = append(result.Frontends, &appliancev1.ApplianceFrontendStatus{Name: frontend.Name,
					Transport: appliancev1.FrontendTransport(frontend.Transport), Exposure: appliancev1.FrontendExposure(frontend.Exposure),
					Endpoint: endpoint, ContainerInstance: installation.CurrentContainerInstance, Available: available})
			}
		}
	}
	result.Health = server.controller.applianceHealthProjection(installation.ID)
	if runtime != nil {
		identity := runtime.GetIdentity()
		identityMatches := installation.CurrentBootID == identity.GetBootId() && installation.CurrentContainerInstance == identity.GetContainerInstanceId()
		if !identityMatches {
			if result.Health == nil || result.Health.GetBootId() != identity.GetBootId() ||
				result.Health.GetContainerInstance() != identity.GetContainerInstanceId() {
				result.Health = &appliancev1.ApplianceHealth{InstallationId: installation.ID,
					BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(),
					ConfigRevision: runtime.GetConfigRevision(), Healthy: false, Status: "INSTANCE_RECONCILIATION_PENDING"}
			}
		} else if result.Health == nil || result.Health.GetBootId() != identity.GetBootId() ||
			result.Health.GetContainerInstance() != identity.GetContainerInstanceId() ||
			result.Health.GetConfigRevision() != runtime.GetConfigRevision() {
			result.Health = &appliancev1.ApplianceHealth{InstallationId: installation.ID,
				BootId: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(),
				ConfigRevision: runtime.GetConfigRevision(),
				Healthy:        runtime.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING,
				Status:         runtime.GetStatus()}
		}
		switch runtime.GetState() {
		case vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED:
			result.ApplianceState = appliancev1.ApplianceState_APPLIANCE_STATE_FAILED
		case vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED:
			result.ApplianceState = appliancev1.ApplianceState_APPLIANCE_STATE_SEALED
		}
	}
	if registration.ID != "" {
		switch {
		case registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED):
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_REVOKED
		case registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL) ||
			registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE) ||
			registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING) ||
			registration.ConnectorPhase == int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_ENROLLING):
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ENROLLING
		case registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED):
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_RECONCILING
		case registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) &&
			installation.DesiredEnabled && installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) &&
			runtime != nil && runtime.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING &&
			result.Health != nil && result.Health.GetHealthy() &&
			result.Health.GetBootId() == installation.CurrentBootID && result.Health.GetContainerInstance() == installation.CurrentContainerInstance:
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE
		case registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) ||
			registration.ConnectorPhase == int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTING):
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_CONNECTING
		default:
			result.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_DISCONNECTED
		}
	}
	return result
}
