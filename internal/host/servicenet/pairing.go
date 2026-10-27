//go:build darwin || linux

package servicenet

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type personalMeshPairingRuntime struct {
	invitation *hostv1.PersonalMeshPairingInvitation
	pending    *trustv1.RegistryOperation
}

func (server *Owner) BeginPersonalMeshPairing(ctx context.Context, request *hostv1.BeginPersonalMeshPairingRequest) (*hostv1.BeginPersonalMeshPairingResponse, error) {
	if err := server.pairingAvailable(); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetExpectedRevision() == 0 || !validPairingPIN(request.GetPin()) {
		return nil, hostv1.InvalidField("request_id/mesh_id/expected_revision/pin", "complete pairing invite and an 8 digit PIN are required")
	}
	permissions, err := normalizePairingPermissions(request.GetPermissions())
	if err != nil {
		return nil, hostv1.InvalidField("permissions", err.Error())
	}
	identity, err := server.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	_, snapshot, err := server.store.GetPersonalMesh(ctx, request.GetMeshId())
	if err != nil || snapshot.GetRevision() != request.GetExpectedRevision() || !trust.CanManageMesh(snapshot, identity.Identity) {
		return nil, grpcstatus.Error(codes.PermissionDenied, "local device cannot manage this mesh revision")
	}
	worker, err := dataworker.GetStatus(ctx, server.dataWorkerSocket)
	if err != nil || len(worker.EndpointAddr) == 0 {
		return nil, grpcstatus.Error(codes.Unavailable, "pairing data worker is unavailable")
	}
	sessionID := make([]byte, 16)
	nonce := make([]byte, 32)
	if _, err = rand.Read(sessionID); err != nil {
		return nil, err
	}
	if _, err = rand.Read(nonce); err != nil {
		clear(sessionID)
		return nil, err
	}
	invitation := &hostv1.PersonalMeshPairingInvitation{SessionId: sessionID, MeshId: request.GetMeshId(),
		InviteRevision: request.GetExpectedRevision(), ManagerDeviceId: identity.Identity.GetDeviceId(),
		ManagerRootPublicKey: append([]byte(nil), identity.Identity.GetRootPublicKey()...), ManagerRootKeyRevision: identity.Identity.GetRootKeyRevision(),
		EndpointAddressJson: append([]byte(nil), worker.EndpointAddr...), InviteId: request.GetRequestId(), Nonce: nonce,
		ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute)), Permissions: permissions}
	acceptance, err := proto.MarshalOptions{Deterministic: true}.Marshal(invitation)
	if err != nil {
		return nil, err
	}
	pin := []byte(request.GetPin())
	defer clear(pin)
	status, err := dataworker.PreparePairingManager(ctx, server.dataWorkerSocket, trust.PairingContext(invitation, nil, nil), pin, acceptance, false)
	clear(acceptance)
	if err != nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	server.pairingMu.Lock()
	server.pairings[[16]byte(sessionID)] = &personalMeshPairingRuntime{invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation)}
	server.pairingMu.Unlock()
	return pairingResponse(invitation, status, nil), nil
}

func (server *Owner) JoinPersonalMeshPairing(ctx context.Context, request *hostv1.JoinPersonalMeshPairingRequest) (*hostv1.JoinPersonalMeshPairingResponse, error) {
	if err := server.pairingAvailable(); err != nil {
		return nil, err
	}
	invitation := request.GetInvitation()
	permissions, err := normalizePairingPermissions(invitation.GetPermissions())
	if invitation == nil || !validPairingPIN(request.GetPin()) || err != nil || !validInvitation(invitation, time.Now()) || !slices.Equal(permissions, invitation.GetPermissions()) {
		return nil, hostv1.InvalidField("invitation/pin/permissions", "a live invitation, 8 digit PIN, and supported permissions are required")
	}
	identity, err := server.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	contextValue := trust.PairingContext(invitation, []byte(identity.Identity.GetDeviceId()), identity.Identity.GetRootPublicKey())
	contextDigest, err := contextValue.Digest()
	if err != nil {
		return nil, err
	}
	proof, err := server.trust.SignPairingProof(ctx, &trustv1.PairingProof{SessionId: append([]byte(nil), invitation.GetSessionId()...),
		MeshId: invitation.GetMeshId(), InviteRevision: invitation.GetInviteRevision(), Permissions: permissions,
		ContextDigest: contextDigest[:], ExpiresAt: invitation.GetExpiresAt()})
	if err != nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	proposal, err := proto.MarshalOptions{Deterministic: true}.Marshal(proof)
	if err != nil {
		return nil, err
	}
	pin := []byte(request.GetPin())
	defer clear(pin)
	status, err := dataworker.JoinPairing(ctx, server.dataWorkerSocket, invitation.GetEndpointAddressJson(), contextValue, pin, proposal, false)
	clear(proposal)
	if err != nil {
		return nil, grpcstatus.Error(codes.PermissionDenied, err.Error())
	}
	server.pairingMu.Lock()
	server.pairings[[16]byte(invitation.GetSessionId())] = &personalMeshPairingRuntime{invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation)}
	server.pairingMu.Unlock()
	response := pairingResponse(invitation, status, nil)
	return &hostv1.JoinPersonalMeshPairingResponse{Invitation: response.GetInvitation(), Role: response.GetRole(), State: response.GetState(), AttemptsRemaining: response.GetAttemptsRemaining(), Sas: response.GetSas(), PendingOperation: response.GetPendingOperation(), LastFailure: response.GetLastFailure()}, nil
}

func (server *Owner) GetPersonalMeshPairing(ctx context.Context, request *hostv1.GetPersonalMeshPairingRequest) (*hostv1.GetPersonalMeshPairingResponse, error) {
	response, err := server.currentPairing(ctx, request.GetSessionId())
	return &hostv1.GetPersonalMeshPairingResponse{Invitation: response.GetInvitation(), Role: response.GetRole(), State: response.GetState(), AttemptsRemaining: response.GetAttemptsRemaining(), Sas: response.GetSas(), PendingOperation: response.GetPendingOperation(), LastFailure: response.GetLastFailure()}, err
}

func (server *Owner) ConfirmPersonalMeshPairing(ctx context.Context, request *hostv1.ConfirmPersonalMeshPairingRequest) (*hostv1.ConfirmPersonalMeshPairingResponse, error) {
	if len(request.GetSessionId()) != 16 {
		return nil, hostv1.InvalidField("session_id", "a 16 byte pairing session ID is required")
	}
	if _, err := dataworker.ConfirmPairing(ctx, server.dataWorkerSocket, request.GetSessionId()); err != nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	response, err := server.currentPairing(ctx, request.GetSessionId())
	return &hostv1.ConfirmPersonalMeshPairingResponse{Invitation: response.GetInvitation(), Role: response.GetRole(), State: response.GetState(), AttemptsRemaining: response.GetAttemptsRemaining(), Sas: response.GetSas(), PendingOperation: response.GetPendingOperation(), LastFailure: response.GetLastFailure()}, err
}

func (server *Owner) RejectPersonalMeshPairing(ctx context.Context, request *hostv1.RejectPersonalMeshPairingRequest) (*hostv1.RejectPersonalMeshPairingResponse, error) {
	if len(request.GetSessionId()) != 16 {
		return nil, hostv1.InvalidField("session_id", "a 16 byte pairing session ID is required")
	}
	status, err := dataworker.RejectPairing(ctx, server.dataWorkerSocket, request.GetSessionId())
	if err != nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	runtime := server.pairing(request.GetSessionId())
	if runtime == nil {
		return nil, grpcstatus.Error(codes.NotFound, "pairing session is not active")
	}
	server.forgetPairing(request.GetSessionId())
	response := pairingResponse(runtime.invitation, status, runtime.pending)
	return &hostv1.RejectPersonalMeshPairingResponse{Invitation: response.GetInvitation(), Role: response.GetRole(), State: response.GetState(), AttemptsRemaining: response.GetAttemptsRemaining(), Sas: response.GetSas(), PendingOperation: response.GetPendingOperation(), LastFailure: response.GetLastFailure()}, nil
}

func (server *Owner) currentPairing(ctx context.Context, sessionID []byte) (*hostv1.BeginPersonalMeshPairingResponse, error) {
	if len(sessionID) != 16 {
		return nil, hostv1.InvalidField("session_id", "a 16 byte pairing session ID is required")
	}
	runtime := server.pairing(sessionID)
	if runtime == nil {
		return nil, grpcstatus.Error(codes.NotFound, "pairing session is not active")
	}
	status, err := dataworker.GetPairingStatus(ctx, server.dataWorkerSocket, sessionID)
	if err != nil {
		if errors.Is(err, dataworker.ErrPairingNotFound) {
			server.forgetPairing(sessionID)
		}
		return nil, grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	if status.Role == dataworker.PairingRoleManager && status.State == dataworker.PairingStateReadyToCommit && runtime.pending == nil {
		proof := &trustv1.PairingProof{}
		if len(status.PeerPayload) == 0 || proto.Unmarshal(status.PeerPayload, proof) != nil {
			return server.rejectPairingContext(ctx, sessionID, runtime)
		}
		operation, createErr := server.trust.CreatePersonalMeshJoin(ctx, runtime.invitation.GetInviteId()+"-join", runtime.invitation, status.PeerIdentity, status.PeerRootKey, proof)
		if createErr != nil {
			if errors.Is(createErr, trust.ErrPairingContextMismatch) {
				return server.rejectPairingContext(ctx, sessionID, runtime)
			}
			return nil, grpcstatus.Error(codes.FailedPrecondition, createErr.Error())
		}
		server.pairingMu.Lock()
		if entry := server.pairings[[16]byte(sessionID)]; entry != nil {
			entry.pending = operation
		}
		server.pairingMu.Unlock()
		runtime.pending = operation
	}
	if status.Terminal() {
		server.forgetPairing(sessionID)
	}
	return pairingResponse(runtime.invitation, status, runtime.pending), nil
}

func (server *Owner) pairingAvailable() error {
	if server.personalMeshPairingDisabled {
		return grpcstatus.Error(codes.PermissionDenied, "managed policy disables personal mesh pairing")
	}
	if server.dataWorkerSocket == "" {
		return grpcstatus.Error(codes.FailedPrecondition, "data worker is required")
	}
	return nil
}

func (controller *Owner) pairing(sessionID []byte) *personalMeshPairingRuntime {
	controller.pairingMu.Lock()
	defer controller.pairingMu.Unlock()
	runtime := controller.pairings[[16]byte(sessionID)]
	if runtime == nil {
		return nil
	}
	copyValue := &personalMeshPairingRuntime{invitation: proto.Clone(runtime.invitation).(*hostv1.PersonalMeshPairingInvitation)}
	if runtime.pending != nil {
		copyValue.pending = proto.Clone(runtime.pending).(*trustv1.RegistryOperation)
	}
	return copyValue
}

func (controller *Owner) CommitPairingOperation(ctx context.Context, operationID string) {
	controller.pairingMu.Lock()
	var sessionID []byte
	for key, runtime := range controller.pairings {
		if runtime.pending != nil && runtime.pending.GetOperationId() == operationID {
			sessionID = append([]byte(nil), key[:]...)
			break
		}
	}
	controller.pairingMu.Unlock()
	if len(sessionID) == 16 {
		if status, err := dataworker.CommitPairing(ctx, controller.dataWorkerSocket, sessionID, true); err == nil && status.Terminal() {
			controller.forgetPairing(sessionID)
		}
	}
}

func pairingResponse(invitation *hostv1.PersonalMeshPairingInvitation, status dataworker.PairingStatus, pending *trustv1.RegistryOperation) *hostv1.BeginPersonalMeshPairingResponse {
	return &hostv1.BeginPersonalMeshPairingResponse{Invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation), Role: pairingRole(status.Role),
		State: pairingState(status.State), LastFailure: pairingFailure(status.LastFailure), AttemptsRemaining: uint32(status.AttemptsRemaining), Sas: status.SAS, PendingOperation: pending}
}

func validPairingPIN(pin string) bool {
	if len(pin) != 8 {
		return false
	}
	for index := range pin {
		if pin[index] < '0' || pin[index] > '9' {
			return false
		}
	}
	return true
}

func validInvitation(invitation *hostv1.PersonalMeshPairingInvitation, now time.Time) bool {
	return len(invitation.GetSessionId()) == 16 && invitation.GetMeshId() != "" && invitation.GetInviteRevision() != 0 &&
		invitation.GetManagerDeviceId() != "" && len(invitation.GetManagerRootPublicKey()) == 33 && invitation.GetManagerRootKeyRevision() != 0 &&
		len(invitation.GetEndpointAddressJson()) > 0 && invitation.GetInviteId() != "" && len(invitation.GetNonce()) == 32 &&
		invitation.GetExpiresAt() != nil && invitation.GetExpiresAt().AsTime().After(now) && invitation.GetExpiresAt().AsTime().Sub(now) <= 10*time.Minute
}

func normalizePairingPermissions(input []trustv1.MeshPermission) ([]trustv1.MeshPermission, error) {
	result := append([]trustv1.MeshPermission(nil), input...)
	slices.Sort(result)
	result = slices.Compact(result)
	if len(result) == 0 || len(result) > 2 {
		return nil, errors.New("pairing permissions are outside bounds")
	}
	for _, permission := range result {
		if permission != trustv1.MeshPermission_MESH_PERMISSION_USE_SERVICES && permission != trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS {
			return nil, errors.New("unsupported pairing permission")
		}
	}
	return result, nil
}

func (controller *Owner) forgetPairing(sessionID []byte) {
	controller.pairingMu.Lock()
	delete(controller.pairings, [16]byte(sessionID))
	controller.pairingMu.Unlock()
}

func pairingRole(role dataworker.PairingRole) trustv1.PairingRole {
	if role == dataworker.PairingRoleManager {
		return trustv1.PairingRole_PAIRING_ROLE_MANAGER
	}
	if role == dataworker.PairingRoleJoiner {
		return trustv1.PairingRole_PAIRING_ROLE_JOINER
	}
	return trustv1.PairingRole_PAIRING_ROLE_UNSPECIFIED
}

func pairingState(state dataworker.PairingState) hostv1.PersonalMeshPairingState {
	return hostv1.PersonalMeshPairingState(hostv1.PersonalMeshPairingState_value["PERSONAL_MESH_PAIRING_STATE_"+strings.ToUpper(string(state))])
}

func pairingFailure(failure dataworker.PairingFailure) hostv1.PairingFailure {
	return hostv1.PairingFailure(hostv1.PairingFailure_value["PAIRING_FAILURE_"+strings.ToUpper(string(failure))])
}

func (server *Owner) rejectPairingContext(ctx context.Context, sessionID []byte, runtime *personalMeshPairingRuntime) (*hostv1.BeginPersonalMeshPairingResponse, error) {
	status, err := dataworker.RejectPairingContext(ctx, server.dataWorkerSocket, sessionID)
	if err != nil {
		return nil, err
	}
	server.forgetPairing(sessionID)
	return pairingResponse(runtime.invitation, status, nil), nil
}
