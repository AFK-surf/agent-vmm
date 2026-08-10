//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type remoteComputeServer struct {
	hostv1.UnimplementedRemoteComputeServiceServer
	controller *Controller
}

func (controller *Controller) NewRemoteComputeServer() *grpc.Server {
	identity := auth.Identity{Role: auth.RemoteConnector}
	limiter := newRPCRateLimiter(100, 200)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes), grpc.MaxConcurrentStreams(64),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), auditUnary(controller), limiter.unary, deadlineUnary, errorDetailsUnary),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), auditStream(controller), limiter.stream, deadlineStream, errorDetailsStream),
	)
	hostv1.RegisterRemoteComputeServiceServer(server, &remoteComputeServer{controller: controller})
	return server
}

func (server *remoteComputeServer) GetRemoteState(ctx context.Context, _ *hostv1.GetRemoteStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, registration).GetRegistration()}, nil
}

func (server *remoteComputeServer) SetConnectionState(ctx context.Context, request *hostv1.SetConnectionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetConnectionInstance() == "" || (request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED && request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DRAINING) {
		return nil, invalidField("connection_instance/state", "connection instance and valid state are required")
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED && registration.ConnectionInstance != "" && registration.ConnectionInstance != request.GetConnectionInstance() {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance has been replaced")
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		registration.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) &&
		registration.ConnectionInstance != "" && registration.ConnectionInstance != request.GetConnectionInstance() {
		server.controller.revokeAllRemoteLeases(ctx)
	}
	updated, err := server.controller.store.SetRemoteConnection(ctx, registration.ID, request.GetConnectionInstance(), int32(request.GetState()))
	if err != nil {
		return nil, err
	}
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		server.controller.disconnectRemoteLeases(updated.ID, request.GetConnectionInstance(), time.Duration(updated.DisconnectGraceSeconds)*time.Second)
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.controller.remoteRegistrationResponse(ctx, updated).GetRegistration()}, nil
}

func (server *remoteComputeServer) EnsureAllocation(ctx context.Context, request *hostv1.EnsureAllocationRequest) (*hostv1.AllocationResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetAllocationId() == "" || request.GetLimits() == nil {
		return nil, invalidField("allocation_id/limits", "allocation ID and limits are required")
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
	}
	if request.GetPolicyRevision() != registration.Revision {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", registration.ID, "pool policy revision is stale")
	}
	if existing, err := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, request.GetAllocationId()); err == nil {
		if !sameAllocationRequest(existing, request) {
			return nil, resourceError(codes.AlreadyExists, "ALLOCATION_CONFLICT", "allocation", request.GetAllocationId(), "allocation exists with different limits")
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
	environment, err := server.controller.store.CreateEnvironment(ctx, store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: request.GetDisplayLabel(),
		CPUMillis: request.GetLimits().GetCpuMillis(), MemoryBytes: request.GetLimits().GetMemoryBytes(), PIDs: request.GetLimits().GetPids(), DiskBytes: request.GetLimits().GetDiskBytes(),
		EgressMode: int32(request.GetEgressMode()), OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: request.GetAllocationId(), AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY),
	})
	if err != nil {
		return nil, err
	}
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
	environment, err := server.remoteEnvironment(ctx, request.GetAllocationId())
	if err != nil {
		return nil, err
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

func (server *remoteComputeServer) PlanDiscardAllocation(ctx context.Context, request *hostv1.PlanDiscardAllocationRequest) (*hostv1.AllocationCleanupPlanResponse, error) {
	environment, err := server.remoteEnvironment(ctx, request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, environment.ID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	return &hostv1.AllocationCleanupPlanResponse{Plan: allocationCleanupPlan(request.GetAllocationId(), plan)}, nil
}

func allocationCleanupPlan(allocationID string, plan *hostv1.CleanupPlan) *hostv1.AllocationCleanupPlan {
	result := &hostv1.AllocationCleanupPlan{AllocationId: allocationID, Revision: plan.GetRevision()}
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

func (server *remoteComputeServer) ExecuteDiscardAllocation(ctx context.Context, request *hostv1.ExecuteDiscardAllocationRequest) (*hostv1.AllocationResponse, error) {
	environment, err := server.remoteEnvironment(ctx, request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, environment.ID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	expected := allocationCleanupPlan(request.GetAllocationId(), plan).GetVolumeIds()
	confirmed := append([]string(nil), request.GetConfirmedVolumeIds()...)
	sort.Strings(confirmed)
	if request.GetPlanRevision() != plan.GetRevision() || !equalStrings(expected, confirmed) {
		return nil, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "allocation", request.GetAllocationId(), "discard requires the current revision and exact volume set")
	}
	server.controller.revokeAllocationLease(ctx, environment, hostv1.LeaseState_LEASE_STATE_REVOKED, true)
	requestID, err := randomID()
	if err != nil {
		return nil, err
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanup(ctx, requestID, plan, confirmed, "")
	if err != nil {
		return nil, err
	}
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, resourceError(codes.FailedPrecondition, "DISCARD_FAILED", "allocation", request.GetAllocationId(), operation.ErrorMessage)
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
	environment, err := server.remoteEnvironment(ctx, request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
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
	lease := &remoteLease{id: leaseID, allocationID: request.GetAllocationId(), environmentID: environment.ID, workloadRef: request.GetWorkloadRef(), generation: server.controller.remoteLeaseGeneration.Add(1), expiresAt: time.Now().Add(duration), state: hostv1.LeaseState_LEASE_STATE_ACTIVE}
	server.controller.remoteLeases[lease.id] = lease
	server.controller.allocationLeases[environment.ID] = lease.id
	result := *lease
	server.controller.mu.Unlock()
	updated, err := server.controller.store.SetAllocationState(ctx, environment.ID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED))
	if err != nil {
		server.controller.finishLease(lease.id, lease.generation, hostv1.LeaseState_LEASE_STATE_REVOKED, false)
		return nil, err
	}
	server.controller.scheduleLease(result.id, result.generation, result.expiresAt)
	_ = updated
	return &hostv1.LeaseResponse{Lease: leaseToProto(&result)}, nil
}

func (server *remoteComputeServer) RenewLease(ctx context.Context, request *hostv1.RenewLeaseRequest) (*hostv1.LeaseResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, err
	}
	duration := request.GetDuration().AsDuration()
	if duration <= 0 || duration > time.Duration(registration.MaxLeaseSeconds)*time.Second {
		return nil, invalidField("duration", "lease duration exceeds pool policy")
	}
	server.controller.mu.Lock()
	lease := server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.generation != request.GetGeneration() || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease is no longer active")
	}
	lease.generation = server.controller.remoteLeaseGeneration.Add(1)
	lease.expiresAt = time.Now().Add(duration)
	result := *lease
	server.controller.mu.Unlock()
	server.controller.scheduleLease(result.id, result.generation, result.expiresAt)
	return &hostv1.LeaseResponse{Lease: leaseToProto(&result)}, nil
}

func (server *remoteComputeServer) ReleaseLease(ctx context.Context, request *hostv1.ReleaseLeaseRequest) (*hostv1.LeaseResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	lease := server.controller.finishLease(request.GetLeaseId(), request.GetGeneration(), hostv1.LeaseState_LEASE_STATE_RELEASED, false)
	if lease == nil {
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease is no longer active")
	}
	environment, err := server.controller.store.GetEnvironment(ctx, lease.environmentID)
	if err == nil && environment.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED) {
		_, _ = server.controller.store.SetAllocationState(ctx, environment.ID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_READY))
	}
	return &hostv1.LeaseResponse{Lease: leaseToProto(lease)}, nil
}

func (server *remoteComputeServer) OpenRuntimeSession(ctx context.Context, request *hostv1.OpenRuntimeSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return nil, err
	}
	server.controller.mu.Lock()
	lease := server.controller.remoteLeases[request.GetLeaseId()]
	if lease == nil || lease.allocationID != request.GetAllocationId() || lease.generation != request.GetGeneration() || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE || !time.Now().Before(lease.expiresAt) {
		server.controller.mu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "LEASE_REVOKED", "lease", request.GetLeaseId(), "lease cannot open a runtime session")
	}
	environmentID := lease.environmentID
	previousSessionID := lease.sessionID
	server.controller.mu.Unlock()
	if previousSessionID != "" {
		_ = server.controller.revokeSession(ctx, previousSessionID)
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "remote registration is unavailable")
	}
	sessionIdentity := auth.Identity{RegistrationID: registration.ID, AllocationID: request.GetAllocationId(), LeaseID: request.GetLeaseId()}
	session, environment, err := server.controller.createSession(ctx, environmentID, store.OwnerRemoteRegistration, sessionIdentity)
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

func (server *remoteComputeServer) remoteEnvironment(ctx context.Context, allocationID string) (store.Environment, error) {
	if _, err := auth.RemoteConnectorIdentity(ctx); err != nil {
		return store.Environment{}, err
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
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
	environments, _ := controller.store.ListRemoteEnvironments(ctx, registration.ID)
	for _, environment := range environments {
		result.Allocations = append(result.Allocations, controller.remoteAllocation(environment))
	}
	return &hostv1.RemoteRegistrationResponse{Registration: result}
}

func (controller *Controller) remoteAllocation(environment store.Environment) *hostv1.RemoteAllocation {
	result := &hostv1.RemoteAllocation{
		AllocationId: environment.AllocationID, DisplayLabel: environment.DisplayName, Revision: environment.PolicyRevision,
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
