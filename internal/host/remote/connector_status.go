//go:build darwin || linux

package remote

import (
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

type connectorProtocolObservation struct {
	observedAt         time.Time
	connectionInstance string
	protocol           *hostv1.RemoteProtocolStatus
}

func (owner *Owner) recordConnectorProtocolLocked(registration store.RemoteRegistration, protocol *hostv1.RemoteProtocolStatus) {
	if registration.ConnectorFailureCode != "REMOTE_PROTOCOL_UNSUPPORTED" || protocol == nil {
		delete(owner.connectorProtocols, registration.ID)
		return
	}
	if owner.connectorProtocols == nil {
		owner.connectorProtocols = make(map[string]connectorProtocolObservation)
	}
	owner.connectorProtocols[registration.ID] = connectorProtocolObservation{observedAt: registration.ConnectorObservedAt, connectionInstance: registration.ConnectionInstance, protocol: proto.Clone(protocol).(*hostv1.RemoteProtocolStatus)}
}

func (owner *Owner) connectorProtocol(registration store.RemoteRegistration) *hostv1.RemoteProtocolStatus {
	if registration.ConnectorFailureCode != "REMOTE_PROTOCOL_UNSUPPORTED" || registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) || registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return nil
	}
	owner.connectorStatusMu.Lock()
	defer owner.connectorStatusMu.Unlock()
	observation, ok := owner.connectorProtocols[registration.ID]
	if !ok || observation.connectionInstance != registration.ConnectionInstance || !observation.observedAt.Equal(registration.ConnectorObservedAt) {
		return nil
	}
	return proto.Clone(observation.protocol).(*hostv1.RemoteProtocolStatus)
}
