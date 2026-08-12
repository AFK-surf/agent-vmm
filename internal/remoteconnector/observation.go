package remoteconnector

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

// RegistrationObservation is the host-owned registration truth used by local
// lifecycle surfaces. A running LaunchAgent is not proof of controller access.
type RegistrationObservation struct {
	Ready   bool
	Revoked bool
}

func ObserveRegistration(ctx context.Context, socket string) (RegistrationObservation, error) {
	local, err := newLocalClient(ctx, socket)
	if err != nil {
		return RegistrationObservation{}, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{})
	if err != nil {
		return RegistrationObservation{}, err
	}
	registration := state.GetRegistration()
	revoked := registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED ||
		registration.GetEnrollmentState() == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING
	ready := !revoked && registration.GetId() != "" &&
		registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		registration.GetEnrollmentState() == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY &&
		registration.GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.GetConnectorStatus().GetPhase() == hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED
	return RegistrationObservation{Ready: ready, Revoked: revoked}, nil
}
