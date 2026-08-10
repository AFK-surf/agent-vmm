//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (server *operatorServer) CreateRemoteRegistration(ctx context.Context, request *hostv1.CreateRemoteRegistrationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetEndpoint() == "" || request.GetProtocolVersion() == "" {
		return nil, invalidField("request_id/endpoint/protocol_version", "request ID, endpoint, and protocol version are required")
	}
	registration, err := registrationFromPool(request.GetPool(), server.controller.remoteCapacity)
	if err != nil {
		return nil, err
	}
	registration.ID, err = randomID()
	if err != nil {
		return nil, err
	}
	registration.Endpoint = request.GetEndpoint()
	registration.DisplayName = request.GetDisplayName()
	registration.ProtocolVersion = request.GetProtocolVersion()
	registration.State = int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED)
	registration.ConnectionState = int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED)
	registration, err = server.controller.store.CreateRemoteRegistration(ctx, registration)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "ACTIVE_REGISTRATION_EXISTS", "remote_registration", "active", "only one active remote registration is allowed")
	}
	return server.controller.remoteRegistrationResponse(ctx, registration), nil
}

func (server *operatorServer) UpdateRemotePool(ctx context.Context, request *hostv1.UpdateRemotePoolRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	current, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	if err != nil {
		return nil, err
	}
	update, err := registrationFromPool(request.GetPool(), server.controller.remoteCapacity)
	if err != nil {
		return nil, err
	}
	update.ID = current.ID
	updated, err := server.controller.store.UpdateRemotePool(ctx, update, request.GetExpectedRevision())
	if err != nil {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", current.ID, err.Error())
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) SetRemoteRegistrationState(ctx context.Context, request *hostv1.SetRemoteRegistrationStateRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		return nil, invalidField("state", "registration state must be enabled, draining, or revoked")
	}
	current, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	updated, err := server.controller.store.SetRemoteRegistrationState(ctx, current.ID, request.GetExpectedRevision(), int32(request.GetState()))
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", current.ID, err.Error())
	}
	if request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		server.controller.revokeAllRemoteLeases(ctx)
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) GetRemoteRegistration(ctx context.Context, _ *hostv1.GetRemoteRegistrationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.ActiveRemoteRegistration(ctx)
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	return server.controller.remoteRegistrationResponse(ctx, registration), nil
}

func registrationFromPool(pool *hostv1.RemotePoolPolicy, capacity store.Environment) (store.RemoteRegistration, error) {
	if pool == nil || pool.GetAggregateLimits() == nil || pool.GetPerEnvironmentLimits() == nil ||
		pool.GetMaxLeaseDuration() == nil || pool.GetDisconnectGrace() == nil || pool.GetStopGrace() == nil {
		return store.RemoteRegistration{}, invalidField("pool", "complete remote pool policy is required")
	}
	aggregate := pool.GetAggregateLimits()
	perEnvironment := pool.GetPerEnvironmentLimits()
	if pool.GetMaxEnvironments() == 0 || validatePolicy(aggregate, pool.GetMaxEgressMode()) != nil || validatePolicy(perEnvironment, pool.GetMaxEgressMode()) != nil {
		return store.RemoteRegistration{}, invalidField("pool", "remote pool limits are invalid")
	}
	if aggregate.GetCpuMillis() > capacity.CPUMillis || aggregate.GetMemoryBytes() > capacity.MemoryBytes ||
		aggregate.GetPids() > capacity.PIDs || aggregate.GetDiskBytes() > capacity.DiskBytes {
		return store.RemoteRegistration{}, invalidField("aggregate_limits", "remote pool exceeds configured VM capacity")
	}
	if perEnvironment.GetCpuMillis() > aggregate.GetCpuMillis() || perEnvironment.GetMemoryBytes() > aggregate.GetMemoryBytes() ||
		perEnvironment.GetPids() > aggregate.GetPids() || perEnvironment.GetDiskBytes() > aggregate.GetDiskBytes() {
		return store.RemoteRegistration{}, invalidField("per_environment_limits", "per-environment limit exceeds aggregate pool")
	}
	lease := pool.GetMaxLeaseDuration().AsDuration()
	disconnect := pool.GetDisconnectGrace().AsDuration()
	stop := pool.GetStopGrace().AsDuration()
	if lease < time.Minute || lease > 24*time.Hour || disconnect < time.Second || disconnect > 10*time.Minute || stop < time.Second || stop > 5*time.Minute {
		return store.RemoteRegistration{}, invalidField("pool duration", "lease or grace duration is outside supported bounds")
	}
	return store.RemoteRegistration{
		MaxEnvironments: pool.GetMaxEnvironments(), AggregateCPUMillis: aggregate.GetCpuMillis(), AggregateMemoryBytes: aggregate.GetMemoryBytes(),
		AggregatePIDs: aggregate.GetPids(), AggregateDiskBytes: aggregate.GetDiskBytes(), PerEnvCPUMillis: perEnvironment.GetCpuMillis(),
		PerEnvMemoryBytes: perEnvironment.GetMemoryBytes(), PerEnvPIDs: perEnvironment.GetPids(), PerEnvDiskBytes: perEnvironment.GetDiskBytes(),
		MaxEgressMode: int32(pool.GetMaxEgressMode()), MaxLeaseSeconds: uint32(lease / time.Second),
		DisconnectGraceSeconds: uint32(disconnect / time.Second), StopGraceSeconds: uint32(stop / time.Second),
	}, nil
}

func registrationToProto(registration store.RemoteRegistration) *hostv1.RemoteRegistration {
	return &hostv1.RemoteRegistration{
		Id: registration.ID, Endpoint: registration.Endpoint, DisplayName: registration.DisplayName, ProtocolVersion: registration.ProtocolVersion,
		Revision: registration.Revision, State: hostv1.RemoteRegistrationState(registration.State), ConnectionState: hostv1.RemoteConnectionState(registration.ConnectionState),
		Pool: &hostv1.RemotePoolPolicy{
			MaxEnvironments:      registration.MaxEnvironments,
			AggregateLimits:      &hostv1.ResourceLimits{CpuMillis: optional(registration.AggregateCPUMillis), MemoryBytes: optional(registration.AggregateMemoryBytes), Pids: optional(registration.AggregatePIDs), DiskBytes: optional(registration.AggregateDiskBytes)},
			PerEnvironmentLimits: &hostv1.ResourceLimits{CpuMillis: optional(registration.PerEnvCPUMillis), MemoryBytes: optional(registration.PerEnvMemoryBytes), Pids: optional(registration.PerEnvPIDs), DiskBytes: optional(registration.PerEnvDiskBytes)},
			MaxEgressMode:        hostv1.EgressMode(registration.MaxEgressMode), MaxLeaseDuration: durationpb.New(time.Duration(registration.MaxLeaseSeconds) * time.Second),
			DisconnectGrace: durationpb.New(time.Duration(registration.DisconnectGraceSeconds) * time.Second), StopGrace: durationpb.New(time.Duration(registration.StopGraceSeconds) * time.Second),
		},
	}
}
