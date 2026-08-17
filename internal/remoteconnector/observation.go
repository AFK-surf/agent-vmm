package remoteconnector

import (
	"context"
	"sync"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

type endpointObservationState struct {
	mu    sync.RWMutex
	value *trustv1.EndpointObservation
}

func newEndpointObservationState(value *trustv1.EndpointObservation) *endpointObservationState {
	state := new(endpointObservationState)
	state.Store(value)
	return state
}

func (state *endpointObservationState) Load() *trustv1.EndpointObservation {
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.value == nil {
		return nil
	}
	return proto.Clone(state.value).(*trustv1.EndpointObservation)
}

func (state *endpointObservationState) Store(value *trustv1.EndpointObservation) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if value == nil {
		state.value = nil
		return
	}
	state.value = proto.Clone(value).(*trustv1.EndpointObservation)
}

// RegistrationObservation is the host-owned registration truth used by local
// lifecycle surfaces. A running LaunchAgent is not proof of controller access.
type RegistrationObservation struct {
	Ready   bool
	Revoked bool
}

func ObserveRegistration(ctx context.Context, socket, registrationID string) (RegistrationObservation, error) {
	local, err := newLocalClient(ctx, socket, registrationID)
	if err != nil {
		return RegistrationObservation{}, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: registrationID})
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
