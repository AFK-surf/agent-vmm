//go:build darwin || linux

package host

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const remoteInventorySnapshotLimit = 64

type remoteComputeServer struct {
	hostv1.UnimplementedRemoteComputeServiceServer
	controller *Controller
}

type scopedRemoteObservationClient interface {
	ComponentHealth(context.Context, *vmmdv1.ComponentHealthRequest, ...grpc.CallOption) (*vmmdv1.ComponentHealthResponse, error)
}

func (controller *Controller) NewRemoteComputeServer() *grpc.Server {
	limiter := newRPCRateLimiter(100, 200)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(64),
		grpc.ChainUnaryInterceptor(auth.RemoteConnectorUnaryInterceptor(), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.RemoteConnectorStreamInterceptor(), auditStream(controller), limiter.stream, deadlineStream, errorDetailsStream),
	)
	hostv1.RegisterRemoteComputeServiceServer(server, &remoteComputeServer{controller: controller})
	return server
}

func authorizeRemoteRegistration(ctx context.Context, registrationID string) error {
	identity, err := auth.RemoteConnectorIdentity(ctx)
	if err != nil {
		return err
	}
	if registrationID == "" || identity.RegistrationID != registrationID {
		return resourceError(codes.PermissionDenied, "REGISTRATION_MISMATCH", "remote_registration", registrationID, "remote registration does not match transport identity")
	}
	return nil
}

func (server *remoteComputeServer) requireCurrentRemoteConnectionLocked(ctx context.Context, registrationID, connectionInstance string) (store.RemoteRegistration, error) {
	registration, err := server.controller.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil {
		return store.RemoteRegistration{}, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", registrationID, "remote registration does not exist")
	}
	if connectionInstance == "" || registration.ConnectionInstance != connectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return store.RemoteRegistration{}, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", connectionInstance, "connection instance is not current")
	}
	return registration, nil
}

func (server *remoteComputeServer) requireFrontendConnectionEpochLocked(registrationID, connectionInstance string, epoch uint64) error {
	if epoch == 0 || server.controller.remoteConnectionEpochs[frontendConnectionKey(registrationID, connectionInstance)] != epoch {
		return resourceError(codes.Aborted, "STALE_CONNECTION_EPOCH", "remote_connection", connectionInstance, "connection epoch is not current")
	}
	return nil
}

func (server *remoteComputeServer) GetRemoteState(ctx context.Context, request *hostv1.GetRemoteStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	if request.GetSummaryOnly() {
		return &hostv1.GetRemoteStateResponse{Registration: registrationToProto(registration)}, nil
	}
	// The control handshake needs only current workload-bearing allocations.
	// Retained and discarded rows remain addressable by exact allocation ID;
	// they do not enlarge the connection snapshot.
	environments, err := server.controller.store.ListRemoteActiveEnvironmentsBounded(ctx, registration.ID, remoteInventorySnapshotLimit)
	if err != nil {
		return nil, err
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationSnapshot(ctx, registration, environments)}, nil
}

func (server *remoteComputeServer) GetRemoteObservation(ctx context.Context, request *hostv1.GetRemoteObservationRequest) (*hostv1.GetRemoteObservationResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	observedAt := timestamppb.Now()
	vm := server.controller.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return &hostv1.GetRemoteObservationResponse{
			Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE},
			Usage:  &hostv1.Usage{SampledAt: observedAt, Stale: true, UnavailableReason: "guest runtime is unavailable"}, ObservedAt: observedAt,
		}, nil
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}
	defer connection.Close()
	usage := server.controller.remoteWorkloadUsage(ctx, registration.ID, observedAt)
	response, err := collectScopedRemoteObservation(ctx, client, usage, server.controller.hostEgressHealth(vm, observedAt), observedAt)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}
	server.controller.mu.Lock()
	if candidate := server.controller.workloadReclaim; candidate != nil {
		run := candidate.run
		now := time.Now()
		if now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() || now.Sub(server.controller.workloadPressureAt) > 2*workloadSampleInterval || len(run.executions) != 0 || run.stopping || run.instanceID != candidate.value.GetContainerInstanceId() {
			server.controller.clearWorkloadReclaimLocked("deadline_expired")
		} else if candidate.value.GetRegistrationId() == registration.ID {
			response.ReclaimCandidate = proto.Clone(candidate.value).(*hostv1.WorkloadReclaimCandidate)
		}
	}
	server.controller.mu.Unlock()
	return response, nil
}

func collectScopedRemoteObservation(ctx context.Context, client scopedRemoteObservationClient, usage *hostv1.Usage, hostEgress *hostv1.ComponentHealth, observedAt *timestamppb.Timestamp) (*hostv1.GetRemoteObservationResponse, error) {
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
	return &hostv1.GetRemoteObservationResponse{Health: health, Usage: usage, ObservedAt: observedAt}, nil
}

func (server *remoteComputeServer) SetConnectionState(ctx context.Context, request *hostv1.SetConnectionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionEpoch() == 0 || (request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED) {
		return nil, invalidField("connection_instance/state", "connection instance and valid state are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	// A private appliance controller must report its transport connection while
	// the VM start commit is still closed to workload admission. Its command
	// path remains fenced by the private route's admission channel. External
	// registrations still require the global admission fence to be open.
	if connectionRequiresRemoteAdmission(registration, request.GetState()) {
		if err := server.controller.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		key := frontendConnectionKey(registration.ID, request.GetConnectionInstance())
		if registration.ConnectionInstance != request.GetConnectionInstance() || server.controller.remoteConnectionEpochs[key] != request.GetConnectionEpoch() {
			return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance or epoch has been replaced")
		}
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) &&
		registration.ConnectionInstance != "" && registration.ConnectionInstance != request.GetConnectionInstance() {
		// A replacement controller connection fences the old transport. It does
		// not revoke the durable allocation or stop work already accepted by the
		// Host/Guest owners.
		server.controller.disconnectRemoteRegistration(registration.ID, registration.ConnectionInstance)
	}
	updated, err := server.controller.store.SetRemoteConnection(ctx, registration.ID, request.GetConnectionInstance(), int32(request.GetState()))
	if err != nil {
		return nil, err
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		if server.controller.remoteConnectionEpochs == nil {
			server.controller.remoteConnectionEpochs = make(map[string]uint64)
		}
		key := frontendConnectionKey(registration.ID, request.GetConnectionInstance())
		if previous := server.controller.remoteConnectionEpochs[key]; previous != 0 && previous != request.GetConnectionEpoch() {
			server.controller.hostFrontends.closeRegistration(registration.ID, request.GetConnectionInstance(), hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
			server.controller.hostFrontends.discardEpoch(registration.ID, request.GetConnectionInstance(), previous)
		}
		server.controller.remoteConnectionEpochs[key] = request.GetConnectionEpoch()
		server.controller.recordDomainReconcile(updated.ID, request.GetConnectionInstance(), !registration.LastConnectedAt.IsZero())
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		delete(server.controller.remoteConnectionEpochs, frontendConnectionKey(registration.ID, request.GetConnectionInstance()))
		server.controller.disconnectRemoteRegistration(updated.ID, request.GetConnectionInstance())
		server.controller.hostFrontends.discardEpoch(registration.ID, request.GetConnectionInstance(), request.GetConnectionEpoch())
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, updated).GetRegistration()}, nil
}

func connectionRequiresRemoteAdmission(registration store.RemoteRegistration, state hostv1.RemoteConnectionState) bool {
	return state == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.OwnerKind != store.RemoteRegistrationPrivate
}

func (server *remoteComputeServer) SetRemoteAdmissionState(ctx context.Context, request *hostv1.SetRemoteAdmissionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if request.GetConnectionInstance() == "" || (request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return nil, invalidField("connection_instance/state", "connection instance and admission state are required")
	}
	if request.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		if err := server.controller.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if request.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		if err := server.controller.requireRemoteAdmission(); err != nil {
			return nil, err
		}
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, err
	}
	if registration.ConnectionInstance != request.GetConnectionInstance() || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance is not current")
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.InventoryRevision != request.GetExpectedInventoryRevision() {
		return nil, resourceError(codes.Aborted, "INVENTORY_REVISION_CONFLICT", "remote_registration", registration.ID, "remote inventory changed before admission")
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.State != int32(request.GetState()) {
		return nil, resourceError(codes.Aborted, "ADMISSION_STATE_CONFLICT", "remote_registration", registration.ID, "remote admission state changed before admission")
	}
	if request.GetExpectedPoolPolicyRevision() != 0 && registration.PoolPolicyRevision != request.GetExpectedPoolPolicyRevision() {
		return nil, resourceError(codes.Aborted, "POOL_REVISION_CONFLICT", "remote_registration", registration.ID, "remote pool policy changed before admission")
	}
	updated, err := server.controller.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(request.GetState()))
	if err != nil {
		return nil, err
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, updated).GetRegistration()}, nil
}

func (server *remoteComputeServer) ReportConnectorStatus(ctx context.Context, request *hostv1.ReportConnectorStatusRequest) (*hostv1.GetRemoteStateResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	statusValue := request.GetStatus()
	if statusValue == nil || !validConnectorPhase(statusValue.GetPhase()) ||
		len(statusValue.GetConnectorRelease()) > 128 || (statusValue.GetPhase() == hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED && request.GetConnectionInstance() == "") {
		return nil, invalidField("status", "connector status is invalid")
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
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
	updated, err := server.controller.store.ReportConnectorStatus(ctx, registration.ID, request.GetConnectionInstance(), int32(statusValue.GetPhase()),
		statusValue.GetConnectorRelease(), observed, nextRetry, statusValue.GetReconnectAttempt(), failureCode, failureMessage)
	if err != nil {
		return nil, err
	}
	server.controller.recordDomainTransport(updated.ID, request.GetConnectionInstance(), statusValue.GetDomainMetrics())
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, updated).GetRegistration()}, nil
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
	case "CONTROLLER_AUTH_FAILED":
		return code, "Controller authentication failed."
	case "CONNECTION_ENDED", "CONNECTION_FAILED":
		return code, "Controller connection ended; retrying."
	default:
		return "CONNECTOR_FAILURE", "The connector reported an error."
	}
}

func (controller *Controller) remoteRegistrationResponse(ctx context.Context, registration store.RemoteRegistration) *hostv1.RemoteRegistrationResponse {
	result := registrationToProto(registration)
	if registration.OwnerKind == store.RemoteRegistrationPrivate {
		if grant, err := controller.store.GetApplianceGrant(ctx, registration.InstallationID); err == nil {
			result.Pool.HostLoopbackFrontend = hostLoopbackCapacityToProto(grant.DynamicHostLoopback)
		}
	}
	result.DomainMetrics = controller.controllerDomainMetrics(registration.ID)
	environments, _ := controller.store.ListRemoteEnvironments(ctx, registration.ID)
	for _, environment := range environments {
		result.Allocations = append(result.Allocations, controller.remoteAllocation(environment))
	}
	return &hostv1.RemoteRegistrationResponse{Registration: result}
}

func (controller *Controller) remoteRegistrationSnapshot(ctx context.Context, registration store.RemoteRegistration, environments []store.Environment) *hostv1.RemoteRegistration {
	result := registrationToProto(registration)
	if registration.OwnerKind == store.RemoteRegistrationPrivate {
		if grant, err := controller.store.GetApplianceGrant(ctx, registration.InstallationID); err == nil {
			result.Pool.HostLoopbackFrontend = hostLoopbackCapacityToProto(grant.DynamicHostLoopback)
		}
	}
	result.DomainMetrics = controller.controllerDomainMetrics(registration.ID)
	for _, environment := range environments {
		result.Allocations = append(result.Allocations, controller.remoteAllocation(environment))
	}
	return result
}

func hostLoopbackCapacityToProto(value store.HostLoopbackFrontendCapacity) *hostv1.HostLoopbackFrontendCapacity {
	if value.MaxActiveLeases == 0 {
		return nil
	}
	return &hostv1.HostLoopbackFrontendCapacity{MaxActiveLeases: value.MaxActiveLeases,
		MaxActiveLeasesPerAllocation: value.MaxActiveLeasesPerAllocation, MaxLeaseSeconds: value.MaxLeaseSeconds,
		MaxConcurrentConnections: value.MaxConcurrentConnections, MaxConcurrentConnectionsPerLease: value.MaxConcurrentConnectionsPerLease,
		MaxFrameBytes: value.MaxFrameBytes, MaxConnectionBytes: value.MaxConnectionBytes,
		ConnectionDeadlineSeconds: value.ConnectionDeadlineSeconds, ConnectionIdleSeconds: value.ConnectionIdleSeconds,
		MaxUseCount: value.MaxUseCount}
}

func (controller *Controller) remoteAllocation(environment store.Environment) *hostv1.RemoteAllocation {
	result := &hostv1.RemoteAllocation{
		AllocationId: environment.AllocationID, DisplayLabel: environment.DisplayName, Revision: environment.PolicyRevision, EnvironmentId: environment.ID,
		EgressMode: hostv1.EgressMode(environment.EgressMode), State: hostv1.AllocationState(environment.AllocationState),
	}
	result.Generation = environment.AllocationGeneration
	resources := environment.Resources()
	result.ResourcesV2 = &hostv1.WorkloadResourcesV2{CpuMaxMillis: uint32(resources.CPUMaxMillis), MemoryMaxBytes: resources.MemoryMaxBytes, PidMax: uint32(resources.PIDMax), WritableQuotaBytes: resources.WritableQuotaBytes}
	return result
}
