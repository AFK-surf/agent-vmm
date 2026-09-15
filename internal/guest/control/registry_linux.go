//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
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
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return "", "", 0, err
	}
	canonical, err := normalizeImageReference(imageReference)
	if err != nil {
		return "", "", 0, imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", imageReference)
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	logical, err := r.client.GetImage(logicalContext, canonical)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", "", 0, imageError(codes.FailedPrecondition, "IMAGE_IMPORT_REQUIRED", "cold images require the immutable ImportImage path", canonical)
		}
		return "", "", 0, err
	}
	nsctx := runtimeContext(ctx)
	image, err := r.client.GetImage(nsctx, runtimeImageReference(logical.Target().Digest))
	if errdefs.IsNotFound(err) {
		return "", "", 0, imageError(codes.FailedPrecondition, "IMAGE_IMPORT_REQUIRED", "cold images require the immutable ImportImage path", canonical)
	}
	if err != nil {
		return "", "", 0, err
	}
	unpacked, err := image.IsUnpacked(nsctx, namespaceSnapshotter(namespaceID))
	if err != nil {
		return "", "", 0, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image unpack state could not be read", canonical)
	}
	if !unpacked {
		return "", "", 0, imageError(codes.FailedPrecondition, "IMAGE_IMPORT_REQUIRED", "image content must be materialized through ImportImage", canonical)
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return "", "", 0, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image metadata could not be read", canonical)
	}
	return canonical, image.Target().Digest.String(), uint64(size), nil
}

func normalizeImageReference(value string) (string, error) {
	return guestimage.NormalizeReference(value)
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
		return imageError(codes.InvalidArgument, "IMAGE_DIGEST_INVALID", "expected image digest is invalid", reference)
	}
	canonical, err := normalizeImageReference(reference)
	if err != nil {
		return imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", reference)
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return err
	}
	nsctx := namespaceContext(ctx, namespaceID)
	image, err := r.client.ImageService().Get(nsctx, canonical)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return imageError(codes.NotFound, "IMAGE_NOT_FOUND", "image is not available in this environment", canonical)
		}
		return err
	}
	if image.Target.Digest.String() != expectedDigest {
		return imageError(codes.Aborted, "IMAGE_DIGEST_CONFLICT", "image content changed; refresh and try again", canonical)
	}
	if imageGenerationID(image) != expectedGenerationID {
		return imageError(codes.Aborted, "IMAGE_GENERATION_CONFLICT", "image record changed; refresh and try again", canonical)
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.GetImage() == canonical {
			return imageError(codes.FailedPrecondition, "IMAGE_IN_USE", "image is used by a container", canonical)
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
