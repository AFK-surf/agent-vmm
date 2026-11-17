//go:build darwin || linux

package remote

import (
	"context"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const remoteInventorySnapshotLimit = 64

type scopedRemoteObservationClient interface {
	ComponentHealth(context.Context, *vmmdv1.ComponentHealthRequest, ...grpc.CallOption) (*vmmdv1.ComponentHealthResponse, error)
}

func (server *Owner) requireCurrentRemoteConnectionLocked(ctx context.Context, registrationID, connectionInstance string) (store.RemoteRegistration, error) {
	registration, err := server.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil {
		return store.RemoteRegistration{}, status.Error(codes.NotFound, "remote registration does not exist")
	}
	if connectionInstance == "" || registration.ConnectionInstance != connectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return store.RemoteRegistration{}, status.Error(codes.Aborted, "connection instance is not current")
	}
	return registration, nil
}

func (server *Owner) GetRemoteState(ctx context.Context, request *connectorv1.GetRemoteStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "active remote registration does not exist")
	}
	if request.GetSummaryOnly() {
		return &connectorv1.GetRemoteStateResponse{Registration: registrationToProto(registration)}, nil
	}
	// The control handshake needs only current workload-bearing allocations.
	// Retained and discarded rows remain addressable by exact allocation ID;
	// they do not enlarge the connection snapshot.
	environments, err := server.store.ListRemoteActiveEnvironmentsBounded(ctx, registration.ID, remoteInventorySnapshotLimit)
	if err != nil {
		return nil, err
	}
	return &connectorv1.GetRemoteStateResponse{Registration: server.remoteRegistrationSnapshot(registration, environments)}, nil
}

func (server *Owner) GetRemoteObservation(ctx context.Context, request *connectorv1.GetRemoteObservationRequest) (*connectorv1.GetRemoteObservationResponse, error) {
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "active remote registration does not exist")
	}
	observedAt := timestamppb.Now()
	vm := server.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return &connectorv1.GetRemoteObservationResponse{
			Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE},
			Usage:  &hostv1.Usage{SampledAt: observedAt, Stale: true, UnavailableReason: "guest runtime is unavailable"}, ObservedAt: observedAt,
		}, nil
	}
	client, err := server.guestConnection.Client()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}

	usage := server.workloads.RemoteWorkloadUsage(ctx, registration.ID, observedAt)
	response, err := collectScopedRemoteObservation(ctx, client, usage, server.services.HostEgressHealth(vm, observedAt), observedAt)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}
	response.ReclaimCandidate = server.workloads.ReclaimCandidate(registration.ID)
	return response, nil
}

func collectScopedRemoteObservation(ctx context.Context, client scopedRemoteObservationClient, usage *hostv1.Usage, hostEgress *hostv1.ComponentHealth, observedAt *timestamppb.Timestamp) (*connectorv1.GetRemoteObservationResponse, error) {
	healthResponse, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return nil, err
	}
	health := &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}
	for _, component := range healthResponse.GetComponents() {
		if len(health.Components) >= 15 {
			break
		}
		state := hostv1.HealthState(component.GetState())
		health.Components = append(health.Components, &hostv1.ComponentHealth{Component: component.GetComponent(), State: state, ObservedAt: timestamppb.New(time.Unix(0, component.GetObservedAtUnixNano()))})
		if state > health.State {
			health.State = state
		}
	}
	if hostEgress != nil {
		hostEgress = &hostv1.ComponentHealth{Component: hostEgress.GetComponent(), State: hostEgress.GetState(), ObservedAt: hostEgress.GetObservedAt()}
		health.Components = append(health.Components, hostEgress)
		if hostEgress.GetState() > health.State {
			health.State = hostEgress.GetState()
		}
	}
	return &connectorv1.GetRemoteObservationResponse{Health: health, Usage: usage, ObservedAt: observedAt}, nil
}

func (server *Owner) SetConnectionState(ctx context.Context, request *connectorv1.SetConnectionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	if request.GetConnectionInstance() == "" || request.GetConnectionEpoch() == 0 || (request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED) {
		return nil, hostv1.InvalidField("connection_instance/state", "connection instance and valid state are required")
	}
	server.remoteAdmissionMu.Lock()

	defer server.remoteAdmissionMu.Unlock()
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "active remote registration does not exist")
	}
	// A private appliance controller must report its transport connection while
	// the VM start commit is still closed to workload admission. Its command
	// path remains fenced by the private route's admission channel. External
	// registrations still require the global admission fence to be open.
	if connectionRequiresRemoteAdmission(registration, request.GetState()) {
		if err := server.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		if registration.ConnectionInstance != request.GetConnectionInstance() || server.remoteConnectionEpoch(registration.ID, request.GetConnectionInstance()) != request.GetConnectionEpoch() {
			return nil, status.Error(codes.Aborted, "connection instance or epoch has been replaced")
		}
	}
	updated, err := server.store.SetRemoteConnection(ctx, registration.ID, request.GetConnectionInstance(), int32(request.GetState()))
	if err != nil {
		return nil, err
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		server.setRemoteConnectionEpoch(registration.ID, request.GetConnectionInstance(), request.GetConnectionEpoch())
		server.recordDomainReconcile(updated.ID, request.GetConnectionInstance(), !registration.LastConnectedAt.IsZero())
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		server.deleteRemoteConnectionEpoch(registration.ID, request.GetConnectionInstance())
	}
	return &connectorv1.GetRemoteStateResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func connectionRequiresRemoteAdmission(registration store.RemoteRegistration, state hostv1.RemoteConnectionState) bool {
	return state == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.OwnerKind != store.RemoteRegistrationPrivate
}

func (server *Owner) SetRemoteAdmissionState(ctx context.Context, request *connectorv1.SetRemoteAdmissionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	if request.GetConnectionInstance() == "" || (request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return nil, hostv1.InvalidField("connection_instance/state", "connection instance and admission state are required")
	}
	if request.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		if err := server.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	server.remoteAdmissionMu.Lock()

	defer server.remoteAdmissionMu.Unlock()
	if request.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		if err := server.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, err
	}
	if registration.ConnectionInstance != request.GetConnectionInstance() || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, status.Error(codes.Aborted, "connection instance is not current")
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.InventoryRevision != request.GetExpectedInventoryRevision() {
		return nil, status.Error(codes.Aborted, "remote inventory changed before admission")
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.State != int32(request.GetState()) {
		return nil, status.Error(codes.Aborted, "remote admission state changed before admission")
	}
	if request.GetExpectedPoolPolicyRevision() != 0 && registration.PoolPolicyRevision != request.GetExpectedPoolPolicyRevision() {
		return nil, status.Error(codes.Aborted, "remote pool policy changed before admission")
	}
	updated, err := server.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(request.GetState()))
	if err != nil {
		return nil, err
	}
	return &connectorv1.GetRemoteStateResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func (server *Owner) ReportConnectorStatus(ctx context.Context, request *connectorv1.ReportConnectorStatusRequest) (*connectorv1.GetRemoteStateResponse, error) {
	statusValue := request.GetStatus()
	if statusValue == nil || !validConnectorPhase(statusValue.GetPhase()) ||
		len(statusValue.GetConnectorRelease()) > 128 || (statusValue.GetPhase() == hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED && request.GetConnectionInstance() == "") {
		return nil, hostv1.InvalidField("status", "connector status is invalid")
	}
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, err
	}
	observed := time.Now()
	nextRetry := time.Unix(0, 0)
	if statusValue.GetNextRetryAt().IsValid() {
		nextRetry = statusValue.GetNextRetryAt().AsTime()
		if nextRetry.Before(observed) || nextRetry.After(observed.Add(10*time.Minute)) {
			nextRetry = time.Unix(0, 0)
		}
	}
	failureCode, failureMessage := connectorFailure(statusValue.GetFailureCode())
	server.connectorStatusMu.Lock()
	updated, err := server.store.ReportConnectorStatus(ctx, registration.ID, request.GetConnectionInstance(), int32(statusValue.GetPhase()),
		statusValue.GetConnectorRelease(), observed, nextRetry, statusValue.GetReconnectAttempt(), failureCode, failureMessage)
	if err == nil {
		server.recordConnectorProtocolLocked(updated, statusValue.GetRemoteProtocol())
	}
	server.connectorStatusMu.Unlock()
	if err != nil {
		return nil, err
	}
	server.recordDomainTransport(updated.ID, request.GetConnectionInstance(), statusValue.GetDomainMetrics())
	return &connectorv1.GetRemoteStateResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func validConnectorPhase(phase hostv1.ConnectorPhase) bool {
	return phase >= hostv1.ConnectorPhase_CONNECTOR_PHASE_STOPPED && phase <= hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR
}

func connectorFailure(code string) (string, string) {
	switch code {
	case "":
		return "", ""
	case "CREDENTIAL_REVOKED":
		return code, "Controller access was revoked."
	case "CONNECTOR_CONFIG_INVALID":
		return code, "Connector configuration must be repaired."
	case "CREDENTIAL_UNAVAILABLE":
		return code, "The controller credential is unavailable. Retry enrollment."
	case "REMOTE_PROTOCOL_UNSUPPORTED":
		return code, "The controller does not support this Host protocol."
	case "CONTROLLER_AUTH_FAILED":
		return code, "Controller authentication failed."
	case "DATA_WORKER_UNAVAILABLE":
		return code, "The local data worker is unavailable; retrying."
	case "CONNECTION_ENDED", "CONNECTION_FAILED":
		return code, "Controller connection ended; retrying."
	default:
		return "CONNECTOR_FAILURE", "The connector reported an error."
	}
}

func (controller *Owner) RegistrationCurrent(ctx context.Context, registration store.RemoteRegistration) *hostv1.RemoteRegistration {
	result := registrationToProto(registration)
	result.ConnectorStatus.RemoteProtocol = controller.connectorProtocol(registration)
	result.DomainMetrics = controller.controllerDomainMetrics(registration.ID)
	environments, _ := controller.store.ListRemoteEnvironments(ctx, registration.ID)
	for _, environment := range environments {
		result.Allocations = append(result.Allocations, controller.Allocation(environment))
	}
	return result
}

func (controller *Owner) remoteRegistrationSnapshot(registration store.RemoteRegistration, environments []store.Environment) *hostv1.RemoteRegistration {
	result := registrationToProto(registration)
	result.ConnectorStatus.RemoteProtocol = controller.connectorProtocol(registration)
	result.DomainMetrics = controller.controllerDomainMetrics(registration.ID)
	for _, environment := range environments {
		result.Allocations = append(result.Allocations, controller.Allocation(environment))
	}
	return result
}

func (controller *Owner) Allocation(environment store.Environment) *hostv1.RemoteAllocation {
	result := &hostv1.RemoteAllocation{
		AllocationId: environment.AllocationID, DisplayLabel: environment.DisplayName, Revision: environment.PolicyRevision, EnvironmentId: environment.ID,
		EgressMode: hostv1.EgressMode(environment.EgressMode), State: hostv1.AllocationState(environment.AllocationState),
	}
	result.Generation = environment.AllocationGeneration
	return result
}
