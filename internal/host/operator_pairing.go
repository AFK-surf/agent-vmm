//go:build darwin || linux

package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type personalMeshPairingRuntime struct {
	invitation *hostv1.PersonalMeshPairingInvitation
	pending    *trustv1.RegistryOperation
}

func (server *operatorServer) BeginPersonalMeshPairing(ctx context.Context, request *hostv1.BeginPersonalMeshPairingRequest) (*hostv1.PersonalMeshPairingResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if err := server.pairingAvailable(); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetExpectedRevision() == 0 || !validPairingPIN(request.GetPin()) {
		return nil, invalidField("request_id/mesh_id/expected_revision/pin", "complete pairing invite and an 8 digit PIN are required")
	}
	identity, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	_, snapshot, err := server.controller.store.GetPersonalMesh(ctx, request.GetMeshId())
	if err != nil || snapshot.GetRevision() != request.GetExpectedRevision() || !memberCanManage(snapshot, identity.Identity) {
		return nil, resourceError(codes.PermissionDenied, "PAIRING_NOT_AUTHORIZED", "mesh", request.GetMeshId(), "local device cannot manage this mesh revision")
	}
	worker, err := dataworker.GetStatus(ctx, server.controller.dataWorkerSocket)
	if err != nil || len(worker.EndpointAddr) == 0 {
		return nil, resourceError(codes.Unavailable, "PAIRING_WORKER_UNAVAILABLE", "device", "local", "pairing data worker is unavailable")
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
		ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute))}
	acceptance, err := proto.MarshalOptions{Deterministic: true}.Marshal(invitation)
	if err != nil {
		return nil, err
	}
	pin := []byte(request.GetPin())
	defer clear(pin)
	status, err := dataworker.PreparePairingManager(ctx, server.controller.dataWorkerSocket, pairingContext(invitation, nil, nil), pin, acceptance, false)
	clear(acceptance)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "PAIRING_PREPARE_FAILED", "mesh", request.GetMeshId(), err.Error())
	}
	server.controller.pairingMu.Lock()
	server.controller.pairings[string(sessionID)] = &personalMeshPairingRuntime{invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation)}
	server.controller.pairingMu.Unlock()
	return pairingResponse(invitation, status, nil), nil
}

func (server *operatorServer) JoinPersonalMeshPairing(ctx context.Context, request *hostv1.JoinPersonalMeshPairingRequest) (*hostv1.PersonalMeshPairingResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if err := server.pairingAvailable(); err != nil {
		return nil, err
	}
	invitation := request.GetInvitation()
	permissions, err := normalizePairingPermissions(request.GetPermissions())
	if invitation == nil || !validPairingPIN(request.GetPin()) || err != nil || !validInvitation(invitation, time.Now()) {
		return nil, invalidField("invitation/pin/permissions", "a live invitation, 8 digit PIN, and supported permissions are required")
	}
	identity, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	contextValue := pairingContext(invitation, identity.Identity, permissions)
	contextDigest, err := contextValue.Digest()
	if err != nil {
		return nil, err
	}
	proof, err := server.controller.trust.SignPairingProof(ctx, &trustv1.PairingProof{SessionId: append([]byte(nil), invitation.GetSessionId()...),
		MeshId: invitation.GetMeshId(), InviteRevision: invitation.GetInviteRevision(), Permissions: permissions,
		ContextDigest: contextDigest[:], ExpiresAt: invitation.GetExpiresAt()})
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "PAIRING_PROOF_FAILED", "mesh", invitation.GetMeshId(), err.Error())
	}
	proposal, err := proto.MarshalOptions{Deterministic: true}.Marshal(proof)
	if err != nil {
		return nil, err
	}
	pin := []byte(request.GetPin())
	defer clear(pin)
	status, err := dataworker.JoinPairing(ctx, server.controller.dataWorkerSocket, invitation.GetEndpointAddressJson(), contextValue, pin, proposal, false)
	clear(proposal)
	if err != nil {
		return nil, resourceError(codes.PermissionDenied, "PAIRING_HANDSHAKE_FAILED", "mesh", invitation.GetMeshId(), err.Error())
	}
	server.controller.pairingMu.Lock()
	server.controller.pairings[string(invitation.GetSessionId())] = &personalMeshPairingRuntime{invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation)}
	server.controller.pairingMu.Unlock()
	return pairingResponse(invitation, status, nil), nil
}

func (server *operatorServer) GetPersonalMeshPairing(ctx context.Context, request *hostv1.GetPersonalMeshPairingRequest) (*hostv1.PersonalMeshPairingResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return server.currentPairing(ctx, request.GetSessionId())
}

func (server *operatorServer) ConfirmPersonalMeshPairing(ctx context.Context, request *hostv1.ConfirmPersonalMeshPairingRequest) (*hostv1.PersonalMeshPairingResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if len(request.GetSessionId()) != 16 {
		return nil, invalidField("session_id", "a 16 byte pairing session ID is required")
	}
	if _, err := dataworker.ConfirmPairing(ctx, server.controller.dataWorkerSocket, request.GetSessionId()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "PAIRING_CONFIRM_FAILED", "pairing", hex.EncodeToString(request.GetSessionId()), err.Error())
	}
	return server.currentPairing(ctx, request.GetSessionId())
}

func (server *operatorServer) RejectPersonalMeshPairing(ctx context.Context, request *hostv1.RejectPersonalMeshPairingRequest) (*hostv1.PersonalMeshPairingResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	status, err := dataworker.RejectPairing(ctx, server.controller.dataWorkerSocket, request.GetSessionId())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "PAIRING_REJECT_FAILED", "pairing", hex.EncodeToString(request.GetSessionId()), err.Error())
	}
	runtime := server.controller.pairing(request.GetSessionId())
	if runtime == nil {
		return nil, resourceError(codes.NotFound, "PAIRING_NOT_FOUND", "pairing", hex.EncodeToString(request.GetSessionId()), "pairing session is not active")
	}
	return pairingResponse(runtime.invitation, status, runtime.pending), nil
}

func (server *operatorServer) currentPairing(ctx context.Context, sessionID []byte) (*hostv1.PersonalMeshPairingResponse, error) {
	if len(sessionID) != 16 {
		return nil, invalidField("session_id", "a 16 byte pairing session ID is required")
	}
	runtime := server.controller.pairing(sessionID)
	if runtime == nil {
		return nil, resourceError(codes.NotFound, "PAIRING_NOT_FOUND", "pairing", hex.EncodeToString(sessionID), "pairing session is not active")
	}
	status, err := dataworker.GetPairingStatus(ctx, server.controller.dataWorkerSocket, sessionID)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "PAIRING_STATUS_FAILED", "pairing", hex.EncodeToString(sessionID), err.Error())
	}
	if status.Role == "manager" && status.State == "ready_to_commit" && runtime.pending == nil {
		proof := &trustv1.PairingProof{}
		if len(status.PeerPayload) == 0 || proto.Unmarshal(status.PeerPayload, proof) != nil {
			return nil, resourceError(codes.FailedPrecondition, "PAIRING_PROOF_INVALID", "pairing", hex.EncodeToString(sessionID), "joiner proof is invalid")
		}
		operation, createErr := server.controller.trust.CreatePersonalMeshJoin(ctx, runtime.invitation.GetInviteId()+"-join", runtime.invitation.GetInviteId(), runtime.invitation.GetNonce(), proof)
		if createErr != nil {
			return nil, resourceError(codes.FailedPrecondition, "PAIRING_COMMIT_PREPARE_FAILED", "mesh", runtime.invitation.GetMeshId(), createErr.Error())
		}
		server.controller.pairingMu.Lock()
		server.controller.pairings[string(sessionID)].pending = operation
		server.controller.pairingMu.Unlock()
		runtime.pending = operation
	}
	return pairingResponse(runtime.invitation, status, runtime.pending), nil
}

func (server *operatorServer) pairingAvailable() error {
	if server.controller.personalMeshPairingDisabled {
		return resourceError(codes.PermissionDenied, "MESH_PAIRING_DISABLED", "device", "local", "managed policy disables personal mesh pairing")
	}
	if server.controller.trust == nil || server.controller.dataWorkerSocket == "" {
		return resourceError(codes.FailedPrecondition, "PAIRING_UNAVAILABLE", "device", "local", "device trust and data worker are required")
	}
	return nil
}

func (controller *Controller) pairing(sessionID []byte) *personalMeshPairingRuntime {
	controller.pairingMu.Lock()
	defer controller.pairingMu.Unlock()
	runtime := controller.pairings[string(sessionID)]
	if runtime == nil {
		return nil
	}
	copyValue := &personalMeshPairingRuntime{invitation: proto.Clone(runtime.invitation).(*hostv1.PersonalMeshPairingInvitation)}
	if runtime.pending != nil {
		copyValue.pending = proto.Clone(runtime.pending).(*trustv1.RegistryOperation)
	}
	return copyValue
}

func (controller *Controller) commitPairingOperation(ctx context.Context, operationID string) {
	controller.pairingMu.Lock()
	var sessionID []byte
	for key, runtime := range controller.pairings {
		if runtime.pending != nil && runtime.pending.GetOperationId() == operationID {
			sessionID = []byte(key)
			break
		}
	}
	controller.pairingMu.Unlock()
	if len(sessionID) == 16 {
		_, _ = dataworker.CommitPairing(ctx, controller.dataWorkerSocket, sessionID, true)
	}
}

func pairingContext(invitation *hostv1.PersonalMeshPairingInvitation, joiner *trustv1.DeviceIdentity, permissions []trustv1.MeshPermission) dataworker.PairingContext {
	value := dataworker.PairingContext{SessionID: invitation.GetSessionId(), MeshID: []byte(invitation.GetMeshId()), InviteRevision: invitation.GetInviteRevision(),
		ManagerIdentity: []byte(invitation.GetManagerDeviceId()), ManagerRootKey: invitation.GetManagerRootPublicKey()}
	if joiner != nil {
		value.JoinerIdentity = []byte(joiner.GetDeviceId())
		value.JoinerRootKey = joiner.GetRootPublicKey()
		value.RolesAndFeatures = make([]byte, len(permissions))
		for index, permission := range permissions {
			value.RolesAndFeatures[index] = byte(permission)
		}
	}
	return value
}

func pairingResponse(invitation *hostv1.PersonalMeshPairingInvitation, status dataworker.PairingStatus, pending *trustv1.RegistryOperation) *hostv1.PersonalMeshPairingResponse {
	return &hostv1.PersonalMeshPairingResponse{Invitation: proto.Clone(invitation).(*hostv1.PersonalMeshPairingInvitation), Role: status.Role,
		State: status.State, AttemptsRemaining: uint32(status.AttemptsRemaining), Sas: status.SAS, PendingOperation: pending}
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

func memberCanManage(snapshot *trustv1.MeshSnapshot, identity *trustv1.DeviceIdentity) bool {
	for _, member := range snapshot.GetActiveMembers() {
		if member.GetDeviceId() == identity.GetDeviceId() && member.GetRootKeyRevision() == identity.GetRootKeyRevision() &&
			slices.Equal(member.GetRootPublicKey(), identity.GetRootPublicKey()) {
			for _, permission := range member.GetPermissions() {
				if permission == trustv1.MeshPermission_MESH_PERMISSION_MANAGE_MEMBERS {
					return true
				}
			}
		}
	}
	return false
}
