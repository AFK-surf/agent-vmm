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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type remoteComputeServer struct {
	hostv1.UnimplementedRemoteComputeServiceServer
	controller *Controller
}

const maxRemoteEnvironments = 64

type scopedRemoteObservationClient interface {
	ComponentHealth(context.Context, *vmmdv1.ComponentHealthRequest, ...grpc.CallOption) (*vmmdv1.ComponentHealthResponse, error)
	GetNamespaceUsage(context.Context, *vmmdv1.GetNamespaceUsageRequest, ...grpc.CallOption) (*vmmdv1.GetUsageResponse, error)
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
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, registration).GetRegistration()}, nil
}

func (server *remoteComputeServer) GetRemoteObservation(ctx context.Context, request *hostv1.GetRemoteObservationRequest) (*hostv1.GetRemoteObservationResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	if registration.MaxEnvironments > maxRemoteEnvironments {
		return nil, status.Error(codes.Unavailable, "host observation exceeds the supported remote environment bound")
	}
	environments, err := server.controller.store.ListRemoteEnvironmentsBounded(ctx, registration.ID, maxRemoteEnvironments+1)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}
	if len(environments) > maxRemoteEnvironments {
		return nil, status.Error(codes.Unavailable, "host observation exceeds the supported remote environment bound")
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
	response, err := collectScopedRemoteObservation(ctx, client, environments, server.controller.hostEgressHealth(vm, observedAt), observedAt)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host observation is unavailable")
	}
	return response, nil
}

func collectScopedRemoteObservation(ctx context.Context, client scopedRemoteObservationClient, environments []store.Environment, hostEgress *hostv1.ComponentHealth, observedAt *timestamppb.Timestamp) (*hostv1.GetRemoteObservationResponse, error) {
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
	usage := &hostv1.Usage{SampledAt: observedAt}
	usageAvailable := true
	for _, environment := range environments {
		response, usageErr := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
		if usageErr != nil || response.GetUsage() == nil || response.GetUsage().GetUnavailable() {
			usageAvailable = false
			continue
		}
		value := response.GetUsage()
		usage.CpuNanos += value.GetCpuNanos()
		usage.MemoryBytes += value.GetMemoryBytes()
		usage.Pids += value.GetPids()
		usage.ReadBytes += value.GetReadBytes()
		usage.WriteBytes += value.GetWriteBytes()
		usage.DiskBytes += value.GetDiskBytes()
		usage.NetworkRxBytes += value.GetNetworkRxBytes()
		usage.NetworkTxBytes += value.GetNetworkTxBytes()
	}
	if !usageAvailable {
		usage = &hostv1.Usage{SampledAt: observedAt, Stale: true, UnavailableReason: "registration metrics are unavailable"}
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
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		key := frontendConnectionKey(registration.ID, request.GetConnectionInstance())
		if registration.ConnectionInstance != request.GetConnectionInstance() || server.controller.remoteConnectionEpochs[key] != request.GetConnectionEpoch() {
			return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance or epoch has been replaced")
		}
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) &&
		registration.ConnectionInstance != "" && registration.ConnectionInstance != request.GetConnectionInstance() {
		if err := server.controller.revokeRemoteLeases(registration.ID); err != nil {
			return nil, resourceError(codes.FailedPrecondition, "REMOTE_LEASE_REVOKE_FAILED", "remote_registration", registration.ID, err.Error())
		}
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
		server.controller.disconnectRemoteLeases(updated.ID, request.GetConnectionInstance(), updated.DisconnectedAt, time.Duration(updated.DisconnectGraceSeconds)*time.Second)
		server.controller.hostFrontends.discardEpoch(registration.ID, request.GetConnectionInstance(), request.GetConnectionEpoch())
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, updated).GetRegistration()}, nil
}

func (server *remoteComputeServer) SetRemoteAdmissionState(ctx context.Context, request *hostv1.SetRemoteAdmissionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if request.GetConnectionInstance() == "" || (request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return nil, invalidField("connection_instance/state", "connection instance and admission state are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
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
	if request.GetAllocationId() == "" || request.GetLimits() == nil {
		return nil, invalidField("allocation_id/limits", "allocation ID and limits are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionInstance() != registration.ConnectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance is not current")
	}
	if request.GetPolicyRevision() != registration.PoolPolicyRevision {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", registration.ID, "pool policy revision is stale")
	}
	if existing, err := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, request.GetAllocationId()); err == nil {
		if !sameAllocationRequest(existing, request) {
			return nil, resourceError(codes.AlreadyExists, "ALLOCATION_CONFLICT", "allocation", request.GetAllocationId(), "allocation exists with different limits")
		}
		if existing.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
			unlockEnvironment := server.controller.lockEnvironment(existing.ID)
			defer unlockEnvironment()
			if err := server.controller.reconcileEnvironment(ctx, existing); err != nil {
				return nil, retryable("ALLOCATION_PROJECTION_FAILED", "retained allocation projection failed", time.Second)
			}
			existing, err = server.controller.store.SetAllocationState(ctx, existing.ID, existing.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_READY))
			if err != nil {
				return nil, err
			}
		}
		return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(existing)}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	environments, err := server.controller.store.ListRemoteEnvironments(ctx, registration.ID)
	if err != nil {
		return nil, err
	}
	if err := admitAllocation(registration, environments, request.GetLimits(), request.GetEgressMode()); err != nil {
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
		CPUMillis: request.GetLimits().GetCpuMillis(), MemoryBytes: request.GetLimits().GetMemoryBytes(), PIDs: request.GetLimits().GetPids(), DiskBytes: request.GetLimits().GetDiskBytes(),
		EgressMode: int32(request.GetEgressMode()), OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: request.GetAllocationId(), AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
	}
	reservationOwner, reservationOwnerKind := registration.ID, store.ReservationOwnerRegistration
	if registration.OwnerKind == store.RemoteRegistrationPrivate {
		reservationOwner, reservationOwnerKind = registration.InstallationID, store.ReservationOwnerAppliance
	}
	amount := store.ResourceAmount{CPUMillis: uint64(request.GetLimits().GetCpuMillis()), MemoryBytes: request.GetLimits().GetMemoryBytes(), PIDs: uint64(request.GetLimits().GetPids()), DiskBytes: request.GetLimits().GetDiskBytes()}
	hostCapacity := store.ResourceAmount{CPUMillis: uint64(server.controller.remoteCapacity.CPUMillis), MemoryBytes: server.controller.remoteCapacity.MemoryBytes, PIDs: uint64(server.controller.remoteCapacity.PIDs), DiskBytes: server.controller.remoteCapacity.DiskBytes}
	domainCeiling := store.ResourceAmount{CPUMillis: uint64(registration.AggregateCPUMillis), MemoryBytes: registration.AggregateMemoryBytes, PIDs: uint64(registration.AggregatePIDs), DiskBytes: registration.AggregateDiskBytes}
	if registration.OwnerKind == store.RemoteRegistrationPrivate {
		if grant, err := server.controller.store.GetApplianceGrant(ctx, registration.InstallationID); err == nil {
			domainCeiling = store.ResourceAmount{
				CPUMillis:   domainCeiling.CPUMillis + grant.System.CPUMillis,
				MemoryBytes: domainCeiling.MemoryBytes + grant.System.MemoryBytes,
				PIDs:        domainCeiling.PIDs + grant.System.PIDs,
				DiskBytes:   domainCeiling.DiskBytes + grant.System.DiskBytes,
			}
		} else {
			return nil, err
		}
	}
	environment, err := server.controller.store.AdmitRemoteEnvironment(ctx, environmentFact, store.ResourceReservation{
		ID: "allocation-" + environmentID, OwnerKind: reservationOwnerKind, OwnerID: reservationOwner,
		ResourceKind: store.ReservationResourceAllocation, ResourceID: environmentID, Amount: amount,
	}, hostCapacity, domainCeiling)
	if err != nil {
		if errors.Is(err, store.ErrResourceCapacityExceeded) || errors.Is(err, store.ErrDomainCapacityExceeded) {
			return nil, resourceError(codes.ResourceExhausted, "RESOURCE_LEDGER_EXHAUSTED", "remote_registration", registration.ID, "resource capacity is exhausted")
		}
		return nil, err
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	if err := server.controller.reconcileEnvironment(ctx, environment); err != nil {
		return nil, retryable("ALLOCATION_PROJECTION_FAILED", "allocation was saved but guest projection failed", time.Second)
	}
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(environment)}, nil
}

func admitAllocation(registration store.RemoteRegistration, environments []store.Environment, limits *hostv1.ResourceLimits, egress hostv1.EgressMode) error {
	if validatePolicy(limits, egress) != nil {
		return invalidField("limits/egress_mode", "allocation policy is invalid")
	}
	if uint32(len(environments)) >= registration.MaxEnvironments {
		return resourceError(codes.ResourceExhausted, "POOL_ENVIRONMENT_LIMIT", "remote_registration", registration.ID, "remote environment count limit reached")
	}
	if limits.GetCpuMillis() > registration.PerEnvCPUMillis || limits.GetMemoryBytes() > registration.PerEnvMemoryBytes || limits.GetPids() > registration.PerEnvPIDs || limits.GetDiskBytes() > registration.PerEnvDiskBytes {
		return resourceError(codes.ResourceExhausted, "POOL_PER_ENVIRONMENT_LIMIT", "remote_registration", registration.ID, "allocation exceeds per-environment pool limit")
	}
	if int32(egress) > registration.MaxEgressMode {
		return resourceError(codes.PermissionDenied, "POOL_EGRESS_LIMIT", "remote_registration", registration.ID, "allocation egress exceeds pool policy")
	}
	var cpu uint64
	var memory, disk uint64
	var pids uint64
	for _, environment := range environments {
		cpu += uint64(environment.CPUMillis)
		memory += environment.MemoryBytes
		pids += uint64(environment.PIDs)
		disk += environment.DiskBytes
	}
	if cpu+uint64(limits.GetCpuMillis()) > uint64(registration.AggregateCPUMillis) || memory+limits.GetMemoryBytes() > registration.AggregateMemoryBytes ||
		pids+uint64(limits.GetPids()) > uint64(registration.AggregatePIDs) || disk+limits.GetDiskBytes() > registration.AggregateDiskBytes {
		return resourceError(codes.ResourceExhausted, "POOL_AGGREGATE_LIMIT", "remote_registration", registration.ID, "allocation exceeds aggregate pool limit")
	}
	return nil
}

func sameAllocationRequest(environment store.Environment, request *hostv1.EnsureAllocationRequest) bool {
	limits := request.GetLimits()
	return environment.CPUMillis == limits.GetCpuMillis() && environment.MemoryBytes == limits.GetMemoryBytes() && environment.PIDs == limits.GetPids() &&
		environment.DiskBytes == limits.GetDiskBytes() && environment.EgressMode == int32(request.GetEgressMode())
}

func (server *remoteComputeServer) ReleaseAllocation(ctx context.Context, request *hostv1.ReleaseAllocationRequest) (*hostv1.AllocationResponse, error) {
	if request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 {
		return nil, invalidField("allocation_id/expected_policy_revision", "complete allocation release fence is required")
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
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	environment, err = server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "environment", environment.ID, "allocation policy revision is stale")
	}
	server.controller.revokeAllocationLease(ctx, environment, hostv1.LeaseState_LEASE_STATE_REVOKED, true)
	current, err := server.controller.store.GetEnvironment(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	if current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		current, err = server.controller.store.SetAllocationState(ctx, current.ID, current.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
		if err != nil {
			return nil, err
		}
	}
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(current)}, nil
}

func (server *remoteComputeServer) PlanCleanupAllocation(ctx context.Context, request *hostv1.PlanCleanupAllocationRequest) (*hostv1.AllocationCleanupPlanResponse, error) {
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
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
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
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
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := validateCleanupKind(request.GetKind()); err != nil {
		return nil, err
	}
	if (request.GetExpectedLeaseId() == "") != (request.GetExpectedLeaseGeneration() == 0) {
		return nil, invalidField("expected_lease_id/expected_lease_generation", "allocation lease fence must be complete")
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	activeLeaseID := server.controller.allocationLeases[environment.ID]
	leaseFence := server.controller.remoteLeases[activeLeaseID]
	activeLease := leaseFence != nil && leaseFence.state == hostv1.LeaseState_LEASE_STATE_ACTIVE
	leaseMatches := activeLease && leaseFence.id == request.GetExpectedLeaseId() && leaseFence.registrationID == request.GetRegistrationId() && leaseFence.allocationID == request.GetAllocationId() && leaseFence.environmentID == environment.ID && leaseFence.generation == request.GetExpectedLeaseGeneration()
	server.controller.mu.Unlock()
	if activeLease && !leaseMatches || !activeLease && request.GetExpectedLeaseId() != "" {
		return nil, resourceError(codes.Aborted, "STALE_LEASE", "allocation", request.GetAllocationId(), "allocation lease fence is stale")
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
	var revokeLease func() error
	if activeLease {
		revokeLease = func() error {
			lease, finishErr := server.controller.finishLease(request.GetExpectedLeaseId(), request.GetExpectedLeaseGeneration(), hostv1.LeaseState_LEASE_STATE_REVOKED, true)
			if finishErr != nil {
				return finishErr
			}
			if lease == nil || lease.registrationID != request.GetRegistrationId() || lease.allocationID != request.GetAllocationId() || lease.environmentID != environment.ID {
				return resourceError(codes.Aborted, "STALE_LEASE", "allocation", request.GetAllocationId(), "allocation lease fence is stale")
			}
			return nil
		}
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmed, "", true, revokeLease)
	if err != nil {
		return nil, err
	}
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, resourceError(codes.FailedPrecondition, "CLEANUP_FAILED", "allocation", request.GetAllocationId(), operation.ErrorMessage)
	}
	if request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		current, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
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

func (server *remoteComputeServer) AcquireLease(ctx context.Context, request *hostv1.AcquireLeaseRequest) (*hostv1.LeaseResponse, error) {
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	environment, err = server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "ALLOCATION_NOT_READY", "allocation", request.GetAllocationId(), "allocation must be ready before acquiring a lease")
	}
	if request.GetExpectedPolicyRevision() == 0 || request.GetExpectedPolicyRevision() != environment.PolicyRevision {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "allocation", request.GetAllocationId(), "allocation policy revision is stale")
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionInstance() != registration.ConnectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance is not current")
	}
	duration := request.GetDuration().AsDuration()
	if duration <= 0 || duration > time.Duration(registration.MaxLeaseSeconds)*time.Second {
		return nil, invalidField("duration", "lease duration exceeds pool policy")
	}
	server.controller.mu.Lock()
	if _, exists := server.controller.allocationLeases[environment.ID]; exists {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.AlreadyExists, "ACTIVE_LEASE_EXISTS", "allocation", request.GetAllocationId(), "allocation already has an active lease")
	}
	leaseID, err := randomID()
	if err != nil {
		server.controller.mu.Unlock()
		return nil, err
	}
	lease := &remoteLease{id: leaseID, registrationID: registration.ID, allocationID: request.GetAllocationId(), environmentID: environment.ID, workloadRef: request.GetWorkloadRef(), generation: server.controller.remoteLeaseGeneration.Add(1), expiresAt: time.Now().Add(duration), state: hostv1.LeaseState_LEASE_STATE_ACTIVE}
	server.controller.remoteLeases[lease.id] = lease
	server.controller.allocationLeases[environment.ID] = lease.id
	result := *lease
	server.controller.mu.Unlock()
	updated, err := server.controller.store.AcquireRemoteLeaseProjection(ctx, store.RemoteLeaseProjection{LeaseID: result.id,
		RegistrationID: result.registrationID, AllocationID: result.allocationID, EnvironmentID: result.environmentID,
		WorkloadRef: result.workloadRef, Generation: result.generation, ExpiresAt: result.expiresAt, State: int32(result.state)}, request.GetExpectedPolicyRevision())
	if err != nil {
		server.controller.finishLeaseMemory(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED)
		return nil, err
	}
	server.controller.scheduleLease(result.id, result.generation, result.expiresAt)
	return &hostv1.LeaseResponse{Lease: leaseToProto(&result), AllocationRevision: updated.PolicyRevision}, nil
}

func (server *remoteComputeServer) RenewLease(ctx context.Context, request *hostv1.RenewLeaseRequest) (*hostv1.LeaseResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	registration, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance())
	if err != nil {
		return nil, err
	}
	duration := request.GetDuration().AsDuration()
	if duration <= 0 || duration > time.Duration(registration.MaxLeaseSeconds)*time.Second {
		return nil, invalidField("duration", "lease duration exceeds pool policy")
	}
	server.controller.mu.Lock()
	lease := server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.registrationID != request.GetRegistrationId() || lease.generation != request.GetGeneration() || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease is no longer active")
	}
	previousExpiry := lease.expiresAt
	lease.expiresAt = time.Now().Add(duration)
	result := *lease
	server.controller.mu.Unlock()
	if err := server.controller.store.PutRemoteLeaseProjection(ctx, store.RemoteLeaseProjection{LeaseID: result.id,
		RegistrationID: result.registrationID, AllocationID: result.allocationID, EnvironmentID: result.environmentID,
		WorkloadRef: result.workloadRef, Generation: result.generation, ExpiresAt: result.expiresAt, State: int32(result.state)}); err != nil {
		server.controller.mu.Lock()
		if current := server.controller.remoteLeases[result.id]; current != nil && current.generation == result.generation {
			current.expiresAt = previousExpiry
		}
		server.controller.mu.Unlock()
		return nil, err
	}
	server.controller.scheduleLease(result.id, result.generation, result.expiresAt)
	environment, err := server.controller.store.GetEnvironment(ctx, result.environmentID)
	if err != nil {
		return nil, err
	}
	return &hostv1.LeaseResponse{Lease: leaseToProto(&result), AllocationRevision: environment.PolicyRevision}, nil
}

func (server *remoteComputeServer) ReleaseLease(ctx context.Context, request *hostv1.ReleaseLeaseRequest) (*hostv1.LeaseResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	currentLease := server.controller.remoteLeases[request.GetLeaseId()]
	if currentLease == nil || currentLease.registrationID != request.GetRegistrationId() || currentLease.generation != request.GetGeneration() || currentLease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease is no longer active")
	}
	environmentID := currentLease.environmentID
	server.controller.mu.Unlock()
	unlockEnvironment := server.controller.lockEnvironment(environmentID)
	defer unlockEnvironment()
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	environment, err = server.controller.store.FinishRemoteLeaseProjection(ctx, request.GetRegistrationId(), request.GetLeaseId(), environmentID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_READY))
	if err != nil {
		return nil, err
	}
	lease := server.controller.finishLeaseMemory(request.GetLeaseId(), request.GetGeneration(), hostv1.LeaseState_LEASE_STATE_RELEASED)
	if lease == nil {
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease is no longer active")
	}
	return &hostv1.LeaseResponse{Lease: leaseToProto(lease), AllocationRevision: environment.PolicyRevision}, nil
}

func (server *remoteComputeServer) OpenRuntimeSession(ctx context.Context, request *hostv1.OpenRuntimeSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	lease := server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.registrationID != request.GetRegistrationId() || lease.allocationID != request.GetAllocationId() || lease.generation != request.GetGeneration() || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE || !time.Now().Before(lease.expiresAt) {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease cannot open a runtime session")
	}
	environmentID := lease.environmentID
	previousSessionID := lease.sessionID
	server.controller.mu.Unlock()
	if previousSessionID != "" {
		_ = server.controller.revokeSession(ctx, previousSessionID)
		server.controller.hostFrontends.closeAllocationLease(request.GetLeaseId(), request.GetGeneration(), hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "remote registration is unavailable")
	}
	sessionIdentity := auth.Identity{RegistrationID: registration.ID, AllocationID: request.GetAllocationId(), LeaseID: request.GetLeaseId()}
	session, environment, err := server.controller.createSession(ctx, environmentID, "", store.OwnerRemoteRegistration, sessionIdentity)
	if err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	lease = server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.generation != request.GetGeneration() || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		server.controller.mu.Unlock()
		_ = server.controller.revokeSession(ctx, session.ID)
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease was revoked while opening the session")
	}
	lease.sessionID = session.ID
	leaseExpiresAt := lease.expiresAt
	server.controller.mu.Unlock()
	expiresAt := session.ExpiresAt
	if leaseExpiresAt.Before(expiresAt) {
		expiresAt = leaseExpiresAt
	}
	return &hostv1.CreateAgentSessionResponse{SessionId: session.ID, SocketPath: session.SocketPath, Environment: sessionEnvironmentFromStore(environment), ExpiresAt: timestamppb.New(expiresAt)}, nil
}

func (server *remoteComputeServer) RevokeRuntimeSession(ctx context.Context, request *hostv1.RevokeRuntimeSessionRequest) (*emptypb.Empty, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if request.GetSessionId() == "" || request.GetLeaseId() == "" || request.GetGeneration() == 0 {
		return nil, invalidField("session_id/lease_id/generation", "exact runtime session identity is required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	lease := server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.registrationID != request.GetRegistrationId() || lease.generation != request.GetGeneration() || lease.sessionID != request.GetSessionId() {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.Aborted, "STALE_RUNTIME_SESSION", "runtime_session", request.GetSessionId(), "runtime session has been replaced or revoked")
	}
	server.controller.mu.Unlock()
	if err := server.controller.revokeSession(ctx, request.GetSessionId()); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	if current := server.controller.remoteLeases[request.GetLeaseId()]; current != nil && current.registrationID == request.GetRegistrationId() && current.generation == request.GetGeneration() && current.sessionID == request.GetSessionId() {
		current.sessionID = ""
	}
	server.controller.mu.Unlock()
	server.controller.hostFrontends.closeAllocationLease(request.GetLeaseId(), request.GetGeneration(), hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
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
		Limits:     &hostv1.ResourceLimits{CpuMillis: optional(environment.CPUMillis), MemoryBytes: optional(environment.MemoryBytes), Pids: optional(environment.PIDs), DiskBytes: optional(environment.DiskBytes)},
		EgressMode: hostv1.EgressMode(environment.EgressMode), State: hostv1.AllocationState(environment.AllocationState),
	}
	controller.mu.Lock()
	if leaseID := controller.allocationLeases[environment.ID]; leaseID != "" {
		result.Lease = leaseToProto(controller.remoteLeases[leaseID])
	}
	controller.mu.Unlock()
	return result
}
