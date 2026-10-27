//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Runtime) GetNamespace(ctx context.Context, namespaceID string) (*vmmdv1.Namespace, error) {
	return r.getNamespaceForRole(ctx, namespaceID, false)
}

func (r *Runtime) getNamespaceForRole(ctx context.Context, namespaceID string, system bool) (*vmmdv1.Namespace, error) {
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	namespace.ObservedStatus = "ready"
	return namespace, nil
}

func (r *Runtime) ListNamespaces(ctx context.Context) ([]*vmmdv1.Namespace, error) {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	result := make([]*vmmdv1.Namespace, 0, len(names))
	for _, name := range names {
		if name == runtimeContainerdNamespace || isSystemNamespace(name) {
			continue
		}
		namespace, err := r.GetNamespace(ctx, name)
		if err != nil {
			return nil, err
		}
		result = append(result, namespace)
	}
	return result, nil
}

func (r *Runtime) GetVolume(ctx context.Context, namespaceID, volumeID string) (*vmmdv1.Volume, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	info, err := os.Stat(volumePath(namespaceID, volumeID))
	if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
		return nil, errdefs.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	return r.volumeSnapshot(namespaceID, volumeID, namespace.GetLimits().GetDiskBytes(), containers)
}

func (r *Runtime) ListVolumes(ctx context.Context, namespaceID string) ([]*vmmdv1.Volume, error) {
	return r.ListVolumesBounded(ctx, namespaceID, 0, 0)
}

func (r *Runtime) ListVolumesBounded(ctx context.Context, namespaceID string, limit, relatedContainerLimit uint32) ([]*vmmdv1.Volume, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	containers, err := r.ListContainersBounded(ctx, namespaceID, relatedContainerLimit)
	if err != nil {
		return nil, err
	}
	entries, err := directoryEntriesBounded(volumePath(namespaceID, ""), limit)
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Volume, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			if limit > 0 {
				return nil, errors.New("volume inventory contains an unexpected non-directory entry")
			}
			continue
		}
		usage := r.cachedOrScheduleVolumeUsage(namespaceID, entry.Name())
		generationID, err := volumeGenerationID(volumePath(namespaceID, entry.Name()))
		if err != nil {
			return nil, err
		}
		volume := &vmmdv1.Volume{Id: entry.Name(), NamespaceId: namespaceID, CurrentBytes: usage.bytes, LimitBytes: namespace.GetLimits().GetDiskBytes(), GenerationId: generationID}
		if usage.observedAt.IsZero() {
			volume.UsageStale = true
			volume.UsageUnavailableReason = "volume usage is being sampled"
		} else {
			volume.UsageObservedAtUnixNano = usage.observedAt.UnixNano()
			volume.UsageStale = time.Since(usage.observedAt) >= volumeUsageTTL
			if usage.err != nil {
				volume.UsageStale = true
				volume.UsageUnavailableReason = usageFailureReason(usage.err)
			}
		}
		for _, container := range containers {
			for _, mount := range container.GetVolumes() {
				if mount.GetOwnedVolume() == entry.Name() {
					volume.MountedBy = append(volume.MountedBy, container.GetId())
				}
			}
		}
		result = append(result, volume)
		if limit > 0 && uint32(len(result)) >= limit {
			break
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].GetId() < result[right].GetId() })
	return result, nil
}

func (r *Runtime) ListImages(ctx context.Context, namespaceID string) ([]*vmmdv1.Image, error) {
	return r.ListImagesBounded(ctx, namespaceID, 0)
}

func (r *Runtime) ListImagesBounded(ctx context.Context, namespaceID string, limit uint32) ([]*vmmdv1.Image, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return nil, err
	}
	references, err := inventory.imageReferences(namespaceID, limit)
	if err != nil {
		return nil, err
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	result := make([]*vmmdv1.Image, 0, len(references))
	for _, reference := range references {
		image, err := r.client.GetImage(logicalContext, reference)
		if err != nil {
			return nil, fmt.Errorf("load indexed runtime image %s/%s: %w", namespaceID, reference, err)
		}
		runtimeImage, err := r.client.GetImage(runtimeContext(ctx), runtimeImageReference(image.Target().Digest))
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		unpacked, err := runtimeImage.IsUnpacked(runtimeContext(ctx), containerSnapshotter)
		if err != nil {
			// A cancelled or interrupted import can leave containerd image
			// metadata behind after its content or snapshot has disappeared.
			// Such a record is not a usable image and must not prevent the
			// authoritative importer from observing it as absent and retrying.
			if unmaterializedImage(err) {
				continue
			}
			return nil, err
		}
		if !unpacked {
			continue
		}
		size, err := runtimeImage.Size(runtimeContext(ctx))
		if err != nil {
			return nil, err
		}
		metadata, err := r.client.ImageService().Get(logicalContext, image.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, &vmmdv1.Image{Reference: image.Name(), Digest: image.Target().Digest.String(), LogicalBytes: uint64(size), Platforms: []string{"linux/arm64"}, GenerationId: imageGenerationID(metadata), BuildReceipt: imageOperationReceipt(metadata, "build"), CopyReceipt: imageOperationReceipt(metadata, "copy")})
	}
	return result, nil
}

func directoryEntriesBounded(path string, limit uint32) ([]os.DirEntry, error) {
	if limit == 0 {
		return directoryEntries(path)
	}
	directory, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(int(limit))
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return entries, err
}

func unmaterializedImage(err error) bool {
	return errdefs.IsNotFound(err)
}

func (r *Runtime) GetImage(ctx context.Context, namespaceID, reference string) (*vmmdv1.Image, error) {
	return r.getImageForRole(ctx, namespaceID, reference, false)
}

func (r *Runtime) getImageForRole(ctx context.Context, namespaceID, reference string, system bool) (*vmmdv1.Image, error) {
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	canonical, err := normalizeImageReference(reference)
	if err != nil {
		return nil, hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_IMAGE_REFERENCE_INVALID, "image reference is invalid", hostv1.WithResource("image", reference))
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	logical, err := r.client.GetImage(logicalContext, canonical)
	if err != nil {
		return nil, err
	}
	nsctx := runtimeContext(ctx)
	image, err := r.client.GetImage(nsctx, runtimeImageReference(logical.Target().Digest))
	if err != nil {
		return nil, err
	}
	if logical.Target().Digest != image.Target().Digest {
		return nil, status.Error(codes.FailedPrecondition, "image grant does not match materialized root filesystem")
	}
	unpacked, err := image.IsUnpacked(nsctx, containerSnapshotter)
	if err != nil {
		return nil, status.Error(codes.Internal, "image unpack state could not be read")
	}
	if !unpacked {
		return nil, status.Error(codes.FailedPrecondition, "image root filesystem is not materialized; import the exact image again")
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return nil, err
	}
	metadata, err := r.client.ImageService().Get(logicalContext, canonical)
	if err != nil {
		return nil, err
	}
	return &vmmdv1.Image{Reference: canonical, Digest: image.Target().Digest.String(), LogicalBytes: uint64(size), Platforms: []string{"linux/arm64"}, GenerationId: imageGenerationID(metadata), BuildReceipt: imageOperationReceipt(metadata, "build"), CopyReceipt: imageOperationReceipt(metadata, "copy")}, nil
}
