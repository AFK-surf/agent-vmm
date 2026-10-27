//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/imageref"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PullImage is now an observation/idempotency endpoint only. Cold material is
// accepted exclusively through ImportImage, where immutable bytes, the global
// materializer, and GuestDiskBudget own the physical failure boundary.
func (r *Runtime) PullImage(ctx context.Context, namespaceID, imageReference string) (string, string, uint64, error) {
	return r.pullImage(ctx, namespaceID, imageReference, false)
}

func (r *Runtime) pullImage(ctx context.Context, namespaceID, imageReference string, system bool) (string, string, uint64, error) {
	var identityErr error
	if system {
		identityErr = validateSystemNamespace(namespaceID)
	} else {
		identityErr = validateID("actor_namespace_id", namespaceID)
	}
	if identityErr != nil {
		return "", "", 0, identityErr
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return "", "", 0, err
	}
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return "", "", 0, err
	}
	canonical, err := normalizeImageReference(imageReference)
	if err != nil {
		return "", "", 0, hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_IMAGE_REFERENCE_INVALID, "image reference is invalid", hostv1.WithResource("image", imageReference))
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	logical, err := r.client.GetImage(logicalContext, canonical)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", "", 0, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_IMPORT_REQUIRED, "cold images require the immutable ImportImage path", hostv1.WithResource("image", canonical))
		}
		return "", "", 0, err
	}
	nsctx := runtimeContext(ctx)
	image, err := r.client.GetImage(nsctx, runtimeImageReference(logical.Target().Digest))
	if errdefs.IsNotFound(err) {
		return "", "", 0, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_IMPORT_REQUIRED, "cold images require the immutable ImportImage path", hostv1.WithResource("image", canonical))
	}
	if err != nil {
		return "", "", 0, err
	}
	unpacked, err := image.IsUnpacked(nsctx, containerSnapshotter)
	if err != nil {
		return "", "", 0, status.Error(codes.Internal, "image unpack state could not be read")
	}
	if !unpacked {
		return "", "", 0, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_IMPORT_REQUIRED, "image content must be materialized through ImportImage", hostv1.WithResource("image", canonical))
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return "", "", 0, status.Error(codes.Internal, "image metadata could not be read")
	}
	return canonical, image.Target().Digest.String(), uint64(size), nil
}

func normalizeImageReference(value string) (string, error) {
	return imageref.NormalizeReference(value)
}

func imageGenerationID(image coreimages.Image) string {
	value := image.Name + "\x00" + image.Target.Digest.String() + "\x00" + image.CreatedAt.UTC().Format(time.RFC3339Nano) + "\x00" + image.UpdatedAt.UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func (r *Runtime) DeleteImage(ctx context.Context, namespaceID, reference, expectedGenerationID, expectedDigest string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	if expectedGenerationID == "" || expectedDigest == "" {
		return errors.New("expected_generation_id and expected_digest are required")
	}
	if _, err := digest.Parse(expectedDigest); err != nil {
		return status.Error(codes.InvalidArgument, "expected image digest is invalid")
	}
	canonical, err := normalizeImageReference(reference)
	if err != nil {
		return hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_IMAGE_REFERENCE_INVALID, "image reference is invalid", hostv1.WithResource("image", reference))
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return err
	}
	nsctx := namespaceContext(ctx, namespaceID)
	image, err := r.client.ImageService().Get(nsctx, canonical)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_IMAGE_NOT_FOUND, "image is not available in this environment", hostv1.WithResource("image", canonical))
		}
		return err
	}
	if image.Target.Digest.String() != expectedDigest {
		return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_IMAGE_DIGEST_CONFLICT, "image content changed; refresh and try again", hostv1.WithResource("image", canonical))
	}
	if imageGenerationID(image) != expectedGenerationID {
		return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_IMAGE_GENERATION_CONFLICT, "image record changed; refresh and try again", hostv1.WithResource("image", canonical))
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.GetImage() == canonical {
			return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_IN_USE, "image is used by a container", hostv1.WithResource("image", canonical))
		}
	}
	if err := r.client.ImageService().Delete(nsctx, canonical, coreimages.SynchronousDelete()); err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	return inventory.removeImage(namespaceID, canonical)
}
