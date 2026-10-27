//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *operatorServer) GetDeviceIdentity(ctx context.Context, _ *hostv1.GetDeviceIdentityRequest) (*hostv1.GetDeviceIdentityResponse, error) {
	record, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	return deviceIdentityResponse(record), nil
}

func (server *operatorServer) ListPersonalMeshes(ctx context.Context, _ *hostv1.ListPersonalMeshesRequest) (*hostv1.ListPersonalMeshesResponse, error) {
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

func (server *operatorServer) CreatePersonalMesh(ctx context.Context, request *hostv1.CreatePersonalMeshRequest) (*hostv1.CreatePersonalMeshResponse, error) {
	if request.GetRequestId() == "" || request.GetRegistryAudience() == "" {
		return nil, hostv1.InvalidField("request_id/registry_audience", "request ID and registry audience are required")
	}
	if server.controller.personalMeshPairingDisabled {
		return nil, status.Error(codes.PermissionDenied, "managed policy disables personal mesh pairing")
	}
	descriptor, snapshot, operation, err := server.controller.trust.CreatePersonalMesh(ctx, request.GetRequestId(), request.GetRegistryAudience())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &hostv1.CreatePersonalMeshResponse{Mesh: &hostv1.PersonalMeshResponse{MeshDescriptor: descriptor, Snapshot: snapshot, PendingOperation: operation}}, nil
}

func (server *operatorServer) RevokePersonalMeshMember(ctx context.Context, request *hostv1.RevokePersonalMeshMemberRequest) (*hostv1.RevokePersonalMeshMemberResponse, error) {
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetDeviceId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/mesh_id/device_id/expected_revision", "complete mesh revoke is required")
	}
	descriptor, snapshot, operation, err := server.controller.trust.RevokePersonalMeshMember(ctx, request.GetRequestId(), request.GetMeshId(), request.GetDeviceId(), request.GetExpectedRevision())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &hostv1.RevokePersonalMeshMemberResponse{Mesh: &hostv1.PersonalMeshResponse{MeshDescriptor: descriptor, Snapshot: snapshot, PendingOperation: operation}}, nil
}

func (server *operatorServer) RepairDeviceIdentity(ctx context.Context, request *hostv1.RepairDeviceIdentityRequest) (*hostv1.RepairDeviceIdentityResponse, error) {
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	record, err := server.controller.trust.Repair(ctx, request.GetRequestId(), request.GetConfirmMembershipReset())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	response := deviceIdentityResponse(record)
	return &hostv1.RepairDeviceIdentityResponse{Identity: response.GetIdentity(), State: response.GetState()}, nil
}

func deviceIdentityResponse(record store.DeviceIdentityRecord) *hostv1.GetDeviceIdentityResponse {
	state := hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY
	if record.State == store.DeviceIdentityNeedsRepair {
		state = hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_NEEDS_REPAIR
	}
	return &hostv1.GetDeviceIdentityResponse{Identity: record.Identity, State: state}
}
