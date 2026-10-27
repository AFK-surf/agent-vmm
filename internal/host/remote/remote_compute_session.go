//go:build darwin || linux

package remote

import (
	"context"
	"database/sql"
	"errors"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *Owner) OpenRuntimeSession(ctx context.Context, request *connectorv1.OpenRuntimeSessionRequest) (*connectorv1.OpenRuntimeSessionResponse, error) {
	if err := server.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	if request.GetExecutionOwnerId() == "" || len(request.GetExecutionOwnerId()) > 256 ||
		request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 {
		return nil, hostv1.InvalidField("execution_owner_id/allocation_id/allocation_generation", "exact runtime owner and allocation identity are required")
	}
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	registration, environment, err := server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	unlock, err := server.workloads.LockEnvironment(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	registration, environment, err = server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if (registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) && registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING)) ||
		registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, status.Error(codes.FailedPrecondition, "remote registration is not enabled")
	}
	if environment.AllocationGeneration != request.GetAllocationGeneration() ||
		environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		return nil, status.Error(codes.Aborted, "allocation generation is not current and ready")
	}
	for _, previousSessionID := range server.sessions.AllocationSessionIDs(registration.ID, environment.AllocationID, environment.AllocationGeneration) {
		if err := server.sessions.Revoke(ctx, previousSessionID); err != nil {
			return nil, err
		}
	}
	sessionIdentity := auth.Identity{RegistrationID: registration.ID, AllocationID: environment.AllocationID,
		AllocationGeneration: environment.AllocationGeneration, ExecutionOwnerID: request.GetExecutionOwnerId()}
	session, current, err := server.sessions.Create(ctx, environment.ID, "", 0, store.OwnerRemoteRegistration, sessionIdentity)
	if err != nil {
		return nil, err
	}
	if current.AllocationGeneration != request.GetAllocationGeneration() ||
		current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		_ = server.sessions.Revoke(ctx, session.ID)
		return nil, status.Error(codes.Aborted, "allocation changed while opening the runtime session")
	}
	return &connectorv1.OpenRuntimeSessionResponse{SessionId: session.ID, SocketPath: session.SocketPath, Environment: sessionEnvironmentFromStore(current), ExpiresAt: timestamppb.New(session.ExpiresAt)}, nil
}

func (server *Owner) RevokeRuntimeSession(ctx context.Context, request *connectorv1.RevokeRuntimeSessionRequest) (*emptypb.Empty, error) {
	if request.GetSessionId() == "" || request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 {
		return nil, hostv1.InvalidField("session_id/allocation_id/allocation_generation", "exact runtime session identity is required")
	}
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	_, environment, err := server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_RUNTIME_SESSION, "runtime session has been replaced or revoked", hostv1.WithResource("runtime_session", request.GetSessionId()))
	}
	if err != nil {
		return nil, err
	}
	unlock, err := server.workloads.LockEnvironment(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, _, err := server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId()); errors.Is(err, sql.ErrNoRows) {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_RUNTIME_SESSION, "runtime session has been replaced or revoked", hostv1.WithResource("runtime_session", request.GetSessionId()))
	} else if err != nil {
		return nil, err
	}
	exact := server.sessions.MatchesAllocation(request.GetSessionId(), request.GetRegistrationId(), request.GetAllocationId(), request.GetAllocationGeneration())
	if !exact {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_RUNTIME_SESSION, "runtime session has been replaced or revoked", hostv1.WithResource("runtime_session", request.GetSessionId()))
	}
	if err := server.sessions.Revoke(ctx, request.GetSessionId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func sessionEnvironmentFromStore(environment store.Environment) *hostv1.AgentEnvironment {
	result := workload.EnvironmentFromStore(environment)
	result.Id = ""
	result.NamespaceId = ""
	result.Owner = nil
	return result
}
