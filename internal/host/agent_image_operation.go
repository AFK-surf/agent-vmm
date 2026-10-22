//go:build darwin || linux

package host

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *agentServer) ObserveImageOperation(ctx context.Context, request *hostv1.ImageOperationRequest) (*hostv1.ImageOperationReceipt, error) {
	return server.imageOperation(ctx, request, false)
}
func (server *agentServer) CancelImageOperation(ctx context.Context, request *hostv1.ImageOperationRequest) (*hostv1.ImageOperationReceipt, error) {
	return server.imageOperation(ctx, request, true)
}
func (server *agentServer) imageOperation(ctx context.Context, request *hostv1.ImageOperationRequest, cancel bool) (*hostv1.ImageOperationReceipt, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	namespace := identity.NamespaceID
	if request.Kind == hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY {
		if identity.RegistrationID == "" || identity.AllocationID != request.TargetAllocationId || identity.AllocationGeneration != request.TargetAllocationGeneration {
			return nil, status.Error(codes.PermissionDenied, "copy observation target differs from current allocation")
		}
		target, err := server.controller.store.GetRemoteEnvironment(ctx, identity.RegistrationID, request.TargetAllocationId)
		if err != nil {
			return nil, err
		}
		registration, err := server.controller.store.GetRemoteRegistration(ctx, identity.RegistrationID)
		if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
			return nil, status.Error(codes.PermissionDenied, "copy observation controller authorization is not current")
		}
		if _, _, err := server.authorizeImageSource(ctx, identity, request.CopySource, target, request.TargetAllocationGeneration, false); err != nil {
			return nil, err
		}
		namespace = target.NamespaceID
	} else if request.Kind != hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD || request.ContainerId == "" || request.ExpectedInstanceId == "" || request.ExecutionId == "" {
		return nil, status.Error(codes.InvalidArgument, "build observation requires exact execution and container instance")
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	guest := &vmmdv1.ImageOperationRequest{RequestId: request.RequestId, Kind: vmmdv1.ImageOperationKind(request.Kind), InputSha256: request.InputSha256, ExecutionId: request.ExecutionId, ContainerId: request.ContainerId, ExpectedInstanceId: request.ExpectedInstanceId, ActorNamespaceId: namespace}
	var receipt *vmmdv1.ImageOperationReceipt
	if cancel {
		receipt, err = client.CancelImageOperation(ctx, guest)
	} else {
		receipt, err = client.ObserveImageOperation(ctx, guest)
	}
	if err != nil {
		return nil, err
	}
	if receipt.WriterSettled && request.Kind == hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD {
		server.controller.releaseStreamExecution(&workloadExecutionToken{environmentID: identity.EnvironmentID, containerID: request.ContainerId, instanceID: request.ExpectedInstanceId, executionID: request.ExecutionId, authority: executionAuthority(identity), owner: executionOwner(identity)})
	}
	return imageReceiptFromGuest(receipt), nil
}
