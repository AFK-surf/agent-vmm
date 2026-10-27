//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *agentServer) ObserveImageOperation(ctx context.Context, request *hostv1.ObserveImageOperationRequest) (*hostv1.ObserveImageOperationResponse, error) {
	value, err := server.imageOperation(ctx, request, false)
	return &hostv1.ObserveImageOperationResponse{Receipt: value}, err
}
func (server *agentServer) CancelImageOperation(ctx context.Context, request *hostv1.CancelImageOperationRequest) (*hostv1.CancelImageOperationResponse, error) {
	value, err := server.imageOperation(ctx, &hostv1.ObserveImageOperationRequest{RequestId: request.GetRequestId(), Kind: request.GetKind(), InputSha256: request.GetInputSha256(), ExecutionId: request.GetExecutionId(), ContainerId: request.GetContainerId(), ExpectedInstanceId: request.GetExpectedInstanceId(), CopySource: request.GetCopySource(), TargetAllocationId: request.GetTargetAllocationId(), TargetAllocationGeneration: request.GetTargetAllocationGeneration()}, true)
	return &hostv1.CancelImageOperationResponse{Receipt: value}, err
}
func (server *agentServer) imageOperation(ctx context.Context, request *hostv1.ObserveImageOperationRequest, cancel bool) (*hostv1.ImageOperationReceipt, error) {
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
	cached, err := server.matchingImageOperation(ctx, identity, request)
	if err != nil {
		return nil, err
	}
	if cached != nil {
		return cached, nil
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	guest := &vmmdv1.ImageOperationRequest{RequestId: request.RequestId, Kind: vmmdv1.ImageOperationKind(request.Kind), InputSha256: request.InputSha256, ExecutionId: request.ExecutionId, ContainerId: request.ContainerId, ExpectedInstanceId: request.ExpectedInstanceId, ActorNamespaceId: namespace}
	var receipt *vmmdv1.ImageOperationReceipt
	if cancel {
		receipt, err = client.CancelImageOperation(ctx, guest)
	} else {
		receipt, err = client.ObserveImageOperation(ctx, guest)
	}
	if err != nil {
		if status.Code(err) == codes.NotFound {
			cached, lookupErr := server.matchingImageOperation(ctx, identity, request)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if cached != nil {
				return cached, nil
			}
		}
		return nil, err
	}
	if receipt.WriterSettled && request.Kind == hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD {
		server.controller.workloads.ReleaseObservedExecution(identity, request.ContainerId, request.ExpectedInstanceId, request.ExecutionId)
	}
	converted := guestconn.ImageReceipt(receipt)
	if err := server.retainImageOperation(ctx, client, identity, namespace, converted); err != nil {
		return nil, err
	}
	return converted, nil
}

func imageOperationStoreKey(identity auth.Identity, kind hostv1.ImageOperationKind, requestID string) (string, string) {
	name := "image-operation-copy"
	if kind == hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD {
		name = "image-operation-build"
	}
	digest := sha256.Sum256([]byte(name + "\x00" + identity.EnvironmentID + "\x00" + identity.NamespaceID + "\x00" + requestID))
	return "image-" + hex.EncodeToString(digest[:]), name
}

func (server *agentServer) matchingImageOperation(ctx context.Context, identity auth.Identity, request *hostv1.ObserveImageOperationRequest) (*hostv1.ImageOperationReceipt, error) {
	receipt, err := server.cachedImageOperation(ctx, identity, request.Kind, request.RequestId)
	if err != nil || receipt == nil {
		return receipt, err
	}
	if receipt.InputSha256 != request.InputSha256 || receipt.ExecutionId != request.ExecutionId || receipt.ContainerId != request.ContainerId || receipt.ExpectedInstanceId != request.ExpectedInstanceId {
		return nil, status.Error(codes.Aborted, "image operation identity differs from accepted input")
	}
	return receipt, nil
}

func (server *agentServer) cachedImageOperation(ctx context.Context, identity auth.Identity, kind hostv1.ImageOperationKind, requestID string) (*hostv1.ImageOperationReceipt, error) {
	key, _ := imageOperationStoreKey(identity, kind, requestID)
	operation, err := server.controller.store.OperationByRequestID(ctx, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	receipt := new(hostv1.ImageOperationReceipt)
	if err := proto.Unmarshal(operation.Receipt, receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}

func (server *agentServer) retainImageOperation(ctx context.Context, client vmmdv1.VMMDServiceClient, identity auth.Identity, namespace string, receipt *hostv1.ImageOperationReceipt) error {
	if !receipt.GetWriterSettled() {
		return nil
	}
	var state hostv1.OperationState
	switch receipt.State {
	case hostv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED:
		state = hostv1.OperationState_OPERATION_STATE_SUCCEEDED
	case hostv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED:
		state = hostv1.OperationState_OPERATION_STATE_FAILED
	case hostv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED, hostv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN:
		state = hostv1.OperationState_OPERATION_STATE_INTERRUPTED
	default:
		return nil
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
	if err != nil {
		return err
	}
	fingerprint, err := hex.DecodeString(receipt.InputSha256)
	if err != nil {
		return err
	}
	key, kind := imageOperationStoreKey(identity, receipt.Kind, receipt.RequestId)
	release, err := server.controller.workloads.LockImageOperation(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	if _, _, err := server.controller.store.PutOperation(ctx, store.Operation{ID: key, RequestID: key, Kind: kind, EnvironmentID: identity.EnvironmentID, RequestSHA256: fingerprint, State: int32(state), Stage: "settled", Receipt: encoded}); err != nil {
		return err
	}
	_, err = client.ForgetImageOperation(ctx, &vmmdv1.ImageOperationRequest{RequestId: receipt.RequestId, Kind: vmmdv1.ImageOperationKind(receipt.Kind), InputSha256: receipt.InputSha256, ExecutionId: receipt.ExecutionId, ContainerId: receipt.ContainerId, ExpectedInstanceId: receipt.ExpectedInstanceId, ActorNamespaceId: namespace})
	if err != nil {
		slog.Warn("forget settled Guest image operation", "operation_id", key, "error", err)
	}
	return nil
}

func buildInputSHA256(header *hostv1.BuildImageHeader) (string, error) {
	input := &hostv1.BuildInputFingerprint{DockerfilePath: header.DockerfilePath, Image: header.Image, Target: header.Target, BuildArgs: header.BuildArgs, NetworkMode: header.NetworkMode, Secrets: header.Secrets, BaseImageReference: header.BaseImageReference, BaseManifestDigest: header.BaseManifestDigest, ContextSha256: header.ContextSha256}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
