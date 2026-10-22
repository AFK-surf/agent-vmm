//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	if (registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) && registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING)) ||
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
	session, current, err := server.controller.createSession(ctx, environment.ID, "", 0, store.OwnerRemoteRegistration, sessionIdentity)
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
