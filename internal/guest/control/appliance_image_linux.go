//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	containerd "github.com/containerd/containerd/v2/client"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bundledMCPImage = "agent-vmm.local/mcp@"
	bundledMCPPath  = "/usr/lib/agent-vmm/mcp-appliance.oci.tar"
)

type bundledSystemImage struct {
	prefix, path string
}

func isBundledSystemImage(imageReference string) bool {
	return strings.HasPrefix(imageReference, bundledMCPImage)
}

func (r *Runtime) importBundledSystemImage(ctx context.Context, namespaceID, imageReference string) (string, string, uint64, error) {
	canonical, err := normalizeImageReference(imageReference)
	if err != nil {
		return "", "", 0, errors.New("bundled system image reference is invalid")
	}
	var bundled bundledSystemImage
	for _, candidate := range []bundledSystemImage{
		{prefix: bundledMCPImage, path: bundledMCPPath},
	} {
		if strings.HasPrefix(canonical, candidate.prefix) {
			bundled = candidate
			break
		}
	}
	if bundled.prefix == "" {
		return "", "", 0, errors.New("bundled system image reference is invalid")
	}
	manifestDigest, err := digest.Parse(strings.TrimPrefix(canonical, bundled.prefix))
	if err != nil || manifestDigest.Algorithm() != digest.SHA256 {
		return "", "", 0, errors.New("bundled system image digest is invalid")
	}
	header := &vmmdv1.ImportImageHeader{RequestId: bundledImportRequestID(canonical), ActorNamespaceId: namespaceID,
		ImageReference: canonical, ManifestDigest: manifestDigest.String(), Platform: "linux/arm64"}
	unlockMaterializer, err := r.lockImportSlot(ctx)
	if err != nil {
		return "", "", 0, err
	}
	defer unlockMaterializer()
	reusable, physicalGrowth, reusableTarget, err := r.imageMaterialized(ctx, namespaceID, canonical, manifestDigest, true)
	if err != nil {
		return "", "", 0, err
	}
	if reusable && !physicalGrowth {
		var image *vmmdv1.Image
		err := func() error {
			var importErr error
			image, importErr = r.importImage(ctx, nil, header, canonical, *reusableTarget, true, true)
			return importErr
		}()
		if err != nil {
			return "", "", 0, err
		}
		return image.GetReference(), image.GetDigest(), image.GetLogicalBytes(), nil
	}
	var archive *os.File
	var manifest *ocispec.Descriptor
	if reusableTarget == nil {
		archive, err = os.Open(bundled.path)
		if err != nil {
			return "", "", 0, status.Error(codes.FailedPrecondition, "installed controller image is absent from cache and the bundled source is unavailable; update this appliance")
		}
		defer archive.Close()
		info, err := archive.Stat()
		if err != nil {
			return "", "", 0, err
		}
		header.ArchiveSize = uint64(info.Size())
		manifest, err = ociarchive.Validate(archive, manifestDigest)
		if err != nil {
			return "", "", 0, status.Error(codes.FailedPrecondition, "bundled archive cannot supply the installed controller image; update this appliance: "+err.Error())
		}
		if err := r.prepareSystemImageImport(ctx, header, canonical, manifestDigest.String()); err != nil {
			return "", "", 0, err
		}
	}
	var image *vmmdv1.Image
	materialize := func() error {
		if reusableTarget != nil {
			if _, createErr := r.client.ImageService().Create(namespaceContext(ctx, namespaceID), coreimages.Image{Name: canonical, Target: *reusableTarget}); createErr != nil && !errdefs.IsAlreadyExists(createErr) {
				return createErr
			}
		}
		var importErr error
		if reusableTarget != nil {
			image, importErr = r.importImage(ctx, nil, header, canonical, *reusableTarget, true, true)
		} else {
			image, importErr = r.importImage(ctx, archive, header, canonical, *manifest, true, false)
		}
		return importErr
	}
	reservation, reserveErr := r.reserveImport(ctx, header)
	if reserveErr != nil {
		return "", "", 0, reserveErr
	}
	defer reservation.Release()
	err = r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return r.guardOCIProjectGrowth(transaction, reservation, materialize)
	})
	if err != nil {
		return "", "", 0, err
	}
	return image.GetReference(), image.GetDigest(), image.GetLogicalBytes(), nil
}

func bundledImportRequestID(imageReference string) string {
	digest := sha256.Sum256([]byte(imageReference))
	return fmt.Sprintf("bundled-mcp-%x", digest[:24])
}

// The caller holds this system namespace's lifecycle lock.
func (r *Runtime) pruneSystemImages(ctx context.Context, namespaceID, currentReference, currentDigest string) (resultErr error) {
	started := time.Now()
	r.logger.Info("system image GC started", "namespace_id", namespaceID, "digest", currentDigest)
	defer func() {
		r.logger.Info("system image GC completed", "namespace_id", namespaceID, "elapsed", time.Since(started).Round(time.Millisecond), "succeeded", resultErr == nil)
	}()
	if err := validateSystemNamespace(namespaceID); err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return err
	}
	var retained systemImageRetention
	if encoded := labels[systemImageRetentionLabel]; encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &retained); err != nil {
			return err
		}
	}
	used := make(map[string]bool)
	for _, container := range containers {
		info, err := container.Info(runtimeContext(ctx))
		if err != nil {
			return err
		}
		if info.Labels[runtimeOwnerLabel] != namespaceID {
			continue
		}
		observed, err := r.inspectContainer(runtimeContext(ctx), namespaceID, container)
		if err != nil {
			return err
		}
		if observed.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			if err := container.Delete(runtimeContext(ctx), containerd.WithSnapshotCleanup); err != nil {
				return err
			}
			if err := inventory.removeContainer(namespaceID, info.Labels[runtimeExternalIDLabel]); err != nil {
				return err
			}
			continue
		}
		used[info.Labels[runtimeImageLabel]] = true
	}
	records, err := r.client.ImageService().List(namespaceContext(ctx, namespaceID))
	if err != nil {
		return err
	}
	for _, image := range records {
		if image.Name == currentReference || image.Target.Digest.String() == currentDigest || slices.Contains(retained.Digests, image.Target.Digest.String()) || used[image.Name] {
			continue
		}
		if err := r.client.ImageService().Delete(namespaceContext(ctx, namespaceID), image.Name, coreimages.SynchronousDelete()); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		if err := inventory.removeImage(namespaceID, image.Name); err != nil {
			return err
		}
	}
	_, err = r.pruneUnreferencedRuntimeImages(ctx, maxGCDeletes)
	return err
}

func (r *Runtime) prepareSystemImageImport(ctx context.Context, header *vmmdv1.ImportImageHeader, reference, targetDigest string) error {
	namespaceID := header.GetActorNamespaceId()
	if err := validateSystemNamespace(namespaceID); err != nil {
		return err
	}
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespaceID, true); err != nil {
		return err
	}
	if err := r.prepareSystemImageImportLocked(ctx, header, reference, targetDigest); err != nil {
		return err
	}
	return r.pruneSystemImages(ctx, namespaceID, reference, targetDigest)
}

func (r *Runtime) prepareSystemImageImportLocked(ctx context.Context, header *vmmdv1.ImportImageHeader, reference, targetDigest string) error {
	namespaceID := header.GetActorNamespaceId()
	fingerprint, err := importFingerprint(header)
	if err != nil {
		return err
	}
	records, err := r.client.ImageService().List(namespaceContext(ctx, namespaceID))
	if err != nil {
		return err
	}
	if err := checkImportIdempotency(records, header.RequestId, fingerprint, reference, digest.Digest(targetDigest)); err != nil {
		return err
	}
	for _, record := range records {
		if record.Name == reference && record.Target.Digest.String() != targetDigest {
			return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_IMAGE_DIGEST_CONFLICT, "image reference already names different content", hostv1.WithResource("image", reference))
		}
	}
	if header.GetSystemRetention() != nil {
		if err := r.projectSystemImageRetention(ctx, namespaceID, header.GetSystemRetention()); err != nil {
			return err
		}
	}
	return nil
}
