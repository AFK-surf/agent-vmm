//go:build darwin

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (server *operatorServer) GetDeviceIdentity(ctx context.Context, _ *hostv1.GetDeviceIdentityRequest) (*hostv1.DeviceIdentityResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.FailedPrecondition, "TRUST_UNAVAILABLE", "device", "local", "device trust is not initialized")
	}
	record, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	return deviceIdentityResponse(record), nil
}

func (server *operatorServer) RotateDeviceRoot(ctx context.Context, request *hostv1.RotateDeviceRootRequest) (*hostv1.RotateDeviceRootResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExpectedRootKeyRevision() == 0 {
		return nil, invalidField("request_id/expected_root_key_revision", "request ID and current root revision are required")
	}
	rotation, record, err := server.controller.trust.RotateRoot(ctx, request.GetRequestId(), request.GetExpectedRootKeyRevision())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "ROOT_ROTATION_FAILED", "device", "local", err.Error())
	}
	return &hostv1.RotateDeviceRootResponse{Identity: record.Identity, Rotation: rotation}, nil
}

func (server *operatorServer) ListPersonalMeshes(ctx context.Context, _ *hostv1.ListPersonalMeshesRequest) (*hostv1.ListPersonalMeshesResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.FailedPrecondition, "TRUST_UNAVAILABLE", "device", "local", "device trust is not initialized")
	}
	records, err := server.controller.store.ListPersonalMeshes(ctx)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListPersonalMeshesResponse{}
	for _, record := range records {
		response.Meshes = append(response.Meshes, &hostv1.PersonalMeshResponse{MeshDescriptor: record.Descriptor, Snapshot: record.Snapshot})
	}
	return response, nil
}

func (server *operatorServer) CreatePersonalMesh(ctx context.Context, request *hostv1.CreatePersonalMeshRequest) (*hostv1.PersonalMeshResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetRegistryAudience() == "" {
		return nil, invalidField("request_id/registry_audience", "request ID and registry audience are required")
	}
	if server.controller.personalMeshPairingDisabled {
		return nil, resourceError(codes.PermissionDenied, "MESH_PAIRING_DISABLED", "device", "local", "managed policy disables personal mesh pairing")
	}
	descriptor, snapshot, operation, err := server.controller.trust.CreatePersonalMesh(ctx, request.GetRequestId(), request.GetRegistryAudience())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "MESH_CREATE_FAILED", "device", "local", err.Error())
	}
	return &hostv1.PersonalMeshResponse{MeshDescriptor: descriptor, Snapshot: snapshot, PendingOperation: operation}, nil
}

func (server *operatorServer) ApplyPersonalMeshSnapshot(ctx context.Context, request *hostv1.ApplyPersonalMeshSnapshotRequest) (*hostv1.PersonalMeshResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetSnapshot() == nil || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/snapshot/expected_revision", "complete mesh snapshot update is required")
	}
	if server.controller.personalMeshPairingDisabled {
		_, current, err := server.controller.store.GetPersonalMesh(ctx, request.GetSnapshot().GetMeshId())
		if err != nil || addsActiveMember(current, request.GetSnapshot()) {
			return nil, resourceError(codes.PermissionDenied, "MESH_PAIRING_DISABLED", "mesh", request.GetSnapshot().GetMeshId(), "managed policy denies new personal mesh membership")
		}
	}
	if err := server.controller.trust.ApplyPersonalMeshSnapshot(ctx, request.GetSnapshot(), request.GetExpectedRevision()); err != nil {
		return nil, resourceError(codes.Aborted, "MESH_REVISION_CONFLICT", "mesh", request.GetSnapshot().GetMeshId(), err.Error())
	}
	descriptor, snapshot, err := server.controller.store.GetPersonalMesh(ctx, request.GetSnapshot().GetMeshId())
	if err != nil {
		return nil, err
	}
	return &hostv1.PersonalMeshResponse{MeshDescriptor: descriptor, Snapshot: snapshot}, nil
}

func addsActiveMember(current, next interface {
	GetActiveMembers() []*trustv1.MeshMembership
}) bool {
	existing := make(map[string]uint64)
	for _, member := range current.GetActiveMembers() {
		existing[member.GetDeviceId()] = member.GetRootKeyRevision()
	}
	for _, member := range next.GetActiveMembers() {
		if existing[member.GetDeviceId()] < member.GetRootKeyRevision() {
			return true
		}
	}
	return false
}

func (server *operatorServer) RevokePersonalMeshMember(ctx context.Context, request *hostv1.RevokePersonalMeshMemberRequest) (*hostv1.PersonalMeshResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetDeviceId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/mesh_id/device_id/expected_revision", "complete mesh revoke is required")
	}
	descriptor, snapshot, operation, err := server.controller.trust.RevokePersonalMeshMember(ctx, request.GetRequestId(), request.GetMeshId(), request.GetDeviceId(), request.GetExpectedRevision())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "MESH_REVOKE_FAILED", "mesh", request.GetMeshId(), err.Error())
	}
	return &hostv1.PersonalMeshResponse{MeshDescriptor: descriptor, Snapshot: snapshot, PendingOperation: operation}, nil
}

func (server *operatorServer) RepairDeviceIdentity(ctx context.Context, request *hostv1.RepairDeviceIdentityRequest) (*hostv1.DeviceIdentityResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	record, err := server.controller.trust.Repair(ctx, request.GetRequestId(), request.GetConfirmMembershipReset())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "DEVICE_REPAIR_FAILED", "device", "local", err.Error())
	}
	return deviceIdentityResponse(record), nil
}

func deviceIdentityResponse(record store.DeviceIdentityRecord) *hostv1.DeviceIdentityResponse {
	state := hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY
	if record.State == store.DeviceIdentityNeedsRepair {
		state = hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_NEEDS_REPAIR
	}
	return &hostv1.DeviceIdentityResponse{Identity: record.Identity, State: state}
}
