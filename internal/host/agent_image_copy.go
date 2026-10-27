//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"fmt"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *agentServer) CopyImage(ctx context.Context, request *hostv1.CopyImageRequest) (*hostv1.CopyImageResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if identity.RegistrationID == "" || request.GetRequestId() == "" || request.GetImageReference() == "" || digest.Digest(request.GetManifestDigest()).Validate() != nil || digest.Digest(request.GetManifestDigest()).Algorithm() != digest.SHA256 {
		return nil, status.Error(codes.InvalidArgument, "image copy requires current controller and immutable image identity")
	}
	if identity.AllocationID != request.GetTargetAllocationId() || identity.AllocationGeneration != request.GetTargetAllocationGeneration() {
		return nil, status.Error(codes.PermissionDenied, "image target does not match current allocation session")
	}
	ctx, release, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	registration, err := server.controller.store.GetRemoteRegistration(ctx, identity.RegistrationID)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, status.Error(codes.PermissionDenied, "image copy controller authorization is not current")
	}
	target, err := server.controller.store.GetRemoteEnvironment(ctx, identity.RegistrationID, request.GetTargetAllocationId())
	if err != nil {
		return nil, err
	}
	sourceNamespace, scope, err := server.authorizeImageSource(ctx, identity, request.Source, target, request.TargetAllocationGeneration, true)
	if err != nil {
		return nil, err
	}
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, target.ID); err != nil {
		return nil, err
	}
	input, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(input))
	key, _ := imageOperationStoreKey(identity, hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, request.RequestId)
	releaseImage, err := server.controller.workloads.RLockImageOperation(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if releaseImage != nil {
			releaseImage()
		}
	}()
	cached, err := server.cachedImageOperation(ctx, identity, hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, request.RequestId)
	if err != nil {
		return nil, err
	}
	if cached != nil {
		if cached.InputSha256 != fingerprint || cached.State != hostv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED {
			return nil, status.Error(codes.Aborted, "image copy request differs from accepted input or is already terminal")
		}
		return &hostv1.CopyImageResponse{Image: &hostv1.Image{Reference: cached.ImageReference, Digest: cached.ManifestDigest, CopyReceipt: cached}}, nil
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}
	image, err := client.CopyImage(ctx, &vmmdv1.CopyImageRequest{RequestId: request.GetRequestId(), SourceNamespaceId: sourceNamespace, ControllerCacheScope: scope, RetainedBuildRequestId: request.GetSource().GetRetained().GetBuildRequestId(), TargetNamespaceId: target.NamespaceID, ImageReference: request.GetImageReference(), ManifestDigest: request.GetManifestDigest(), ControllerRequestSha256: fingerprint})
	if err != nil {
		return nil, err
	}
	releaseImage()
	releaseImage = nil
	converted := guestconn.Image(image)
	if err := server.retainImageOperation(ctx, client, identity, target.NamespaceID, converted.GetCopyReceipt()); err != nil {
		return nil, err
	}
	return &hostv1.CopyImageResponse{Image: converted}, nil
}

func validateImageCopyAllocations(identity auth.Identity, request *hostv1.CopyImageRequest, source, target store.Environment, requireReady bool) error {
	if source.OwnerKind != store.OwnerRemoteRegistration || target.OwnerKind != store.OwnerRemoteRegistration || source.RegistrationID != identity.RegistrationID || target.RegistrationID != identity.RegistrationID ||
		source.AllocationGeneration != request.GetSource().GetAllocation().GetAllocationGeneration() || target.AllocationGeneration != request.GetTargetAllocationGeneration() || source.AllocationID != request.GetSource().GetAllocation().GetAllocationId() || target.AllocationID != request.GetTargetAllocationId() || target.NamespaceID != identity.NamespaceID ||
		(requireReady && (source.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) || target.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY))) {
		return status.Error(codes.PermissionDenied, "image copy allocation owner, generation or readiness does not match")
	}
	return nil
}

func (server *agentServer) authorizeImageSource(ctx context.Context, identity auth.Identity, source *hostv1.ImageSource, target store.Environment, generation uint64, ready bool) (string, string, error) {
	if target.OwnerKind != store.OwnerRemoteRegistration || target.RegistrationID != identity.RegistrationID || target.AllocationID != identity.AllocationID || target.AllocationGeneration != generation || target.NamespaceID != identity.NamespaceID || (ready && target.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY)) {
		return "", "", status.Error(codes.PermissionDenied, "image target allocation does not match current controller session")
	}
	switch origin := source.GetSource().(type) {
	case *hostv1.ImageSource_Allocation:
		allocation, err := server.controller.store.GetRemoteEnvironment(ctx, identity.RegistrationID, origin.Allocation.GetAllocationId())
		if err != nil {
			return "", "", err
		}
		request := &hostv1.CopyImageRequest{Source: source, TargetAllocationId: identity.AllocationID, TargetAllocationGeneration: generation}
		if err := validateImageCopyAllocations(identity, request, allocation, target, ready); err != nil {
			return "", "", err
		}
		if ready {
			if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, allocation.ID); err != nil {
				return "", "", err
			}
		}
		return allocation.NamespaceID, "", nil
	case *hostv1.ImageSource_Retained:
		if origin.Retained == nil || origin.Retained.BuildRequestId == "" {
			return "", "", status.Error(codes.InvalidArgument, "retained image source is missing")
		}
		return "", identity.RegistrationID, nil
	default:
		return "", "", status.Error(codes.InvalidArgument, "image source is required")
	}
}

func (server *agentServer) controllerImageScope(ctx context.Context, identity auth.Identity, origin hostv1.ImageOrigin) (string, error) {
	switch origin {
	case hostv1.ImageOrigin_IMAGE_ORIGIN_CURRENT_ALLOCATION:
		return "", nil
	case hostv1.ImageOrigin_IMAGE_ORIGIN_CONTROLLER_RETAINED:
		if identity.RegistrationID == "" {
			return "", status.Error(codes.PermissionDenied, "retained images require controller authorization")
		}
		registration, err := server.controller.store.GetRemoteRegistration(ctx, identity.RegistrationID)
		if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
			return "", status.Error(codes.PermissionDenied, "retained image controller authorization is not current")
		}
		return identity.RegistrationID, nil
	default:
		return "", status.Error(codes.InvalidArgument, "unknown image origin")
	}
}
