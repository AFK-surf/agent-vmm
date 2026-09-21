//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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
		return status.Error(codes.PermissionDenied, "remote registration does not match transport identity")
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

func (server *remoteComputeServer) EnsureAllocation(ctx context.Context, request *hostv1.EnsureAllocationRequest) (*hostv1.AllocationResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionInstance() != registration.ConnectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance is not current")
	}
	resources := contractFromHost(request.GetResourcesV2())
	if request.GetAllocationId() == "" || request.GetGeneration() == 0 || request.GetResourcesV2() == nil || resources.Validate(server.controller.remoteCapacityV2.CPUMaxMillis) != nil {
		return nil, invalidField("allocation_id/generation/resources_v2", "complete resource-v2 allocation identity and resources are required")
	}
	if request.GetPolicyRevision() != registration.PoolPolicyRevision {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", registration.ID, "pool policy revision is stale")
	}
	if existing, err := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, request.GetAllocationId()); err == nil {
		if !sameAllocationRequest(existing, request) {
			return nil, resourceError(codes.AlreadyExists, "ALLOCATION_CONFLICT", "allocation", request.GetAllocationId(), "allocation exists with different limits")
		}
		unlockEnvironment := server.controller.lockEnvironment(existing.ID)
		defer unlockEnvironment()
		if err := server.controller.reconcileEnvironment(ctx, existing); err != nil {
			return nil, allocationProjectionError(err, "Guest namespace projection is incomplete")
		}
		existing, err = server.controller.store.GetEnvironment(ctx, existing.ID)
		if err != nil {
			return nil, err
		}
		return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(existing)}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err := admitAllocationV2(registration, resources, server.controller.remoteCapacityV2, request.GetEgressMode()); err != nil {
		return nil, err
	}
	environmentID, err := randomID()
	if err != nil {
		return nil, err
	}
	namespaceID, err := randomID()
	if err != nil {
		return nil, err
	}
	environmentFact := store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: request.GetDisplayLabel(),
		CPUMillis: uint32(resources.CPUMaxMillis), MemoryBytes: resources.MemoryMaxBytes, PIDs: uint32(resources.PIDMax), DiskBytes: resources.WritableQuotaBytes,
		EgressMode: int32(request.GetEgressMode()), OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetGeneration(), AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING),
	}
	environment, err := server.controller.store.BeginWorkloadAdmission(ctx, environmentFact, server.controller.remoteCapacityV2)
	if err != nil {
		if errors.Is(err, store.ErrResourceCapacityExceeded) {
			return nil, capacityError("RESOURCE_LEDGER_EXHAUSTED", registration.ID, err)
		}
		return nil, err
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	if err := server.controller.reconcileEnvironment(ctx, environment); err != nil {
		return nil, allocationProjectionError(err, "allocation was saved but guest projection failed")
	}
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(environment)}, nil
}

func allocationProjectionError(err error, message string) error {
	if status.Code(err) == codes.ResourceExhausted {
		return err
	}
	return retryable("ALLOCATION_PROJECTION_FAILED", message, time.Second)
}

func contractFromHost(value *hostv1.WorkloadResourcesV2) resourcev2.Contract {
	return resourcev2.Contract{CPUMaxMillis: uint64(value.GetCpuMaxMillis()),
		MemoryMaxBytes: value.GetMemoryMaxBytes(), PIDMax: uint64(value.GetPidMax()), WritableQuotaBytes: value.GetWritableQuotaBytes()}
}

func admitAllocationV2(registration store.RemoteRegistration, resources resourcev2.Contract, hostCapacity resourcev2.Contract, egress hostv1.EgressMode) error {
	if int32(egress) > registration.MaxEgressMode {
		return resourceError(codes.PermissionDenied, "POOL_EGRESS_LIMIT", "remote_registration", registration.ID, "allocation egress exceeds pool policy")
	}
	perEnvironment := resourcev2.Contract{
		PIDMax:             uint64(registration.PerEnvPIDs),
		WritableQuotaBytes: registration.PerEnvDiskBytes,
	}
	if err := resources.Fits(perEnvironment); err != nil {
		return capacityError("POOL_PER_ENVIRONMENT_LIMIT", registration.ID, err)
	}
	if err := resources.Fits(hostCapacity); err != nil {
		return capacityError("POOL_PER_ENVIRONMENT_LIMIT", registration.ID, err)
	}
	return nil
}

func capacityError(reason, registrationID string, err error) error {
	metadata := map[string]string{}
	if dimension := resourcev2.ExhaustedDimension(err); dimension != "" {
		metadata["resource_dimension"] = string(dimension)
	}
	return resourceErrorWithMetadata(codes.ResourceExhausted, reason, "remote_registration", registrationID, err.Error(), metadata)
}

func sameAllocationRequest(environment store.Environment, request *hostv1.EnsureAllocationRequest) bool {
	return environment.AllocationGeneration == request.GetGeneration() && environment.Resources() == contractFromHost(request.GetResourcesV2()) && environment.EgressMode == int32(request.GetEgressMode())
}

func (server *remoteComputeServer) ReleaseAllocation(ctx context.Context, request *hostv1.ReleaseAllocationRequest) (*hostv1.AllocationResponse, error) {
	if request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 || request.GetExpectedGeneration() == 0 {
		return nil, invalidField("allocation_id/expected_policy_revision/expected_generation", "complete allocation release fence is required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.AllocationGeneration != request.GetExpectedGeneration() {
		return nil, resourceError(codes.Aborted, "STALE_GENERATION", "allocation", request.GetAllocationId(), "release generation is not current")
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	environment, err = server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "environment", environment.ID, "allocation policy revision is stale")
	}
	if err := server.controller.closeAllocationRuntime(ctx, request.GetRegistrationId(), environment.AllocationID, environment.AllocationGeneration); err != nil {
		return nil, err
	}
	if err := server.controller.stopRemoteWorkload(ctx, environment.ID); err != nil {
		return nil, err
	}
	current, err := server.controller.store.GetEnvironment(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	if current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		current, err = server.controller.store.SetAllocationState(ctx, environment.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	} else {
		current, err = server.controller.store.RequeueAllocationRetirement(ctx, environment.ID, current.PolicyRevision)
	}
	if err != nil {
		return nil, err
	}
	server.controller.wakeAllocationRetirement()
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(current)}, nil
}

func (server *remoteComputeServer) PlanCleanupAllocation(ctx context.Context, request *hostv1.PlanCleanupAllocationRequest) (*hostv1.AllocationCleanupPlanResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	if err := validateCleanupKind(request.GetKind()); err != nil {
		return nil, err
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	unlockEnvironment, err := server.controller.lockEnvironmentContext(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	defer unlockEnvironment()
	plan, err := (&operatorServer{controller: server.controller}).planCleanupWithRemoteOwner(ctx, environment.ID, request.GetKind(), true)
	if err != nil {
		return nil, err
	}
	return &hostv1.AllocationCleanupPlanResponse{Plan: allocationCleanupPlan(request.GetAllocationId(), plan)}, nil
}

func allocationCleanupPlan(allocationID string, plan *hostv1.CleanupPlan) *hostv1.AllocationCleanupPlan {
	result := &hostv1.AllocationCleanupPlan{AllocationId: allocationID, Revision: plan.GetRevision(), Kind: plan.GetKind()}
	for _, impact := range plan.GetImpacts() {
		copy := &hostv1.CleanupImpact{Effect: impact.GetEffect(), Irreversible: impact.GetIrreversible(), Blocker: impact.GetBlocker()}
		if resource := impact.GetResource(); resource != nil {
			copy.Resource = &hostv1.ResourceReference{Kind: resource.GetKind(), ResourceId: resource.GetResourceId()}
			if resource.GetKind() == "volume" {
				result.VolumeIds = append(result.VolumeIds, resource.GetResourceId())
			}
		}
		result.Impacts = append(result.Impacts, copy)
	}
	sort.Strings(result.VolumeIds)
	return result
}

func (server *remoteComputeServer) ExecuteCleanupAllocation(ctx context.Context, request *hostv1.ExecuteCleanupAllocationRequest) (*hostv1.AllocationResponse, error) {
	ctx, cancelAdmission := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancelAdmission()
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return nil, cleanupAdmissionError(err, request.GetAllocationId())
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, cleanupAdmissionError(err, request.GetAllocationId())
	}
	defer releaseAdmission()
	if err := validateCleanupKind(request.GetKind()); err != nil {
		return nil, err
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	confirmed := append([]string(nil), request.GetConfirmedVolumeIds()...)
	sort.Strings(confirmed)
	requestID, err := randomID()
	if err != nil {
		return nil, err
	}
	requested := &hostv1.CleanupPlan{
		EnvironmentId: environment.ID,
		Kind:          request.GetKind(),
		Revision:      request.GetPlanRevision(),
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmed, "", false, true, nil)
	if err != nil {
		return nil, err
	}
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, resourceError(codes.FailedPrecondition, "CLEANUP_FAILED", "allocation", request.GetAllocationId(), operation.ErrorMessage)
	}
	if request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		responseCtx, cancelResponse := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
		defer cancelResponse()
		current, err := server.remoteEnvironment(responseCtx, request.GetRegistrationId(), request.GetAllocationId())
		if err != nil {
			return nil, err
		}
		return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(current)}, nil
	}
	return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (server *remoteComputeServer) OpenRuntimeSession(ctx context.Context, request *hostv1.OpenRuntimeSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	if request.GetExecutionOwnerId() == "" || len(request.GetExecutionOwnerId()) > 256 ||
		request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 {
		return nil, invalidField("execution_owner_id/allocation_id/allocation_generation", "exact runtime owner and allocation identity are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	registration, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance())
	if err != nil {
		return nil, err
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) ||
		registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", registration.ID, "remote registration is not enabled")
	}
	environment, err := server.remoteEnvironment(ctx, registration.ID, request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.AllocationGeneration != request.GetAllocationGeneration() ||
		environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		return nil, resourceError(codes.Aborted, "STALE_GENERATION", "allocation", request.GetAllocationId(), "allocation generation is not current and ready")
	}
	for _, previousSessionID := range server.controller.allocationSessionIDs(registration.ID, environment.AllocationID, environment.AllocationGeneration) {
		if err := server.controller.revokeSession(ctx, previousSessionID); err != nil {
			return nil, err
		}
	}
	sessionIdentity := auth.Identity{RegistrationID: registration.ID, AllocationID: environment.AllocationID,
		AllocationGeneration: environment.AllocationGeneration, ExecutionOwnerID: request.GetExecutionOwnerId()}
	session, current, err := server.controller.createSession(ctx, environment.ID, "", store.OwnerRemoteRegistration, sessionIdentity)
	if err != nil {
		return nil, err
	}
	if current.AllocationGeneration != request.GetAllocationGeneration() ||
		current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		_ = server.controller.revokeSession(ctx, session.ID)
		return nil, resourceError(codes.Aborted, "STALE_GENERATION", "allocation", request.GetAllocationId(), "allocation changed while opening the runtime session")
	}
	return &hostv1.CreateAgentSessionResponse{SessionId: session.ID, SocketPath: session.SocketPath, Environment: sessionEnvironmentFromStore(current), ExpiresAt: timestamppb.New(session.ExpiresAt)}, nil
}

func (server *remoteComputeServer) RevokeRuntimeSession(ctx context.Context, request *hostv1.RevokeRuntimeSessionRequest) (*emptypb.Empty, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if request.GetSessionId() == "" || request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 {
		return nil, invalidField("session_id/allocation_id/allocation_generation", "exact runtime session identity is required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	session := server.controller.sessions[request.GetSessionId()]
	exact := session != nil && session.registrationID == request.GetRegistrationId() &&
		session.allocationID == request.GetAllocationId() && session.allocationGeneration == request.GetAllocationGeneration()
	server.controller.mu.Unlock()
	if !exact {
		return nil, resourceError(codes.Aborted, "STALE_RUNTIME_SESSION", "runtime_session", request.GetSessionId(), "runtime session has been replaced or revoked")
	}
	if err := server.controller.revokeSession(ctx, request.GetSessionId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (server *remoteComputeServer) remoteEnvironment(ctx context.Context, registrationID, allocationID string) (store.Environment, error) {
	if err := authorizeRemoteRegistration(ctx, registrationID); err != nil {
		return store.Environment{}, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil {
		return store.Environment{}, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	environment, err := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, allocationID)
	if err != nil {
		return store.Environment{}, resourceError(codes.NotFound, "ALLOCATION_NOT_FOUND", "allocation", allocationID, "allocation does not exist")
	}
	return environment, nil
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
