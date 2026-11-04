//go:build linux

package control

import (
	"context"

	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
)

const maxGCDeletes = 32

func (r *Runtime) reclaimOCI(ctx context.Context, limit int) (int, error) {
	if !r.materializationSlot.TryAcquire(1) {
		if r.builder == nil {
			return 0, nil
		}
		return r.builder.ReclaimCache(ctx, limit, "explicit_oci_reclaim oci_slot_occupied=true")
	}
	defer r.materializationSlot.Release(1)
	return r.reclaimOCIMaterializerHeld(ctx, limit, "explicit_oci_reclaim")
}

func (r *Runtime) reclaimOCIMaterializerHeld(ctx context.Context, limit int, reason string) (int, error) {
	if limit <= 0 || limit > maxGCDeletes {
		limit = maxGCDeletes
	}
	retained, err := r.reclaimRetainedImageCache(ctx, max(1, limit/2))
	if err != nil || retained >= limit {
		return retained, err
	}
	removed, err := r.pruneUnreferencedRuntimeImages(ctx, limit-retained)
	removed += retained
	if err != nil || removed >= limit || r.builder == nil {
		return removed, err
	}
	caches, err := r.builder.ReclaimCache(ctx, limit-removed, reason)
	return removed + caches, err
}

// OCIContentLifecycle.GC: logical grants and accepted-source pins retain material.
// Logical image records are retention grants. The rebuildable index selects a
// bounded page of physical runtime images. Each candidate is checked again
// under the import lock. A synchronous delete lets containerd remove only
// content and snapshots that its current reference graph does not use.
func (r *Runtime) pruneUnreferencedRuntimeImages(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return 0, err
	}
	r.gcMu.Lock()
	references := inventory.runtimeImagePage(r.gcRuntimeImageCursor, limit)
	if len(references) == 0 && r.gcRuntimeImageCursor != "" {
		r.gcRuntimeImageCursor = ""
		references = inventory.runtimeImagePage("", limit)
	}
	if len(references) != 0 {
		r.gcRuntimeImageCursor = references[len(references)-1]
	}
	r.gcMu.Unlock()

	return r.pruneRuntimeImageReferences(ctx, references)
}

func (r *Runtime) pruneRuntimeImageReferences(ctx context.Context, references []string) (int, error) {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return 0, err
	}
	if err := r.ensureSourceImagePins(ctx); err != nil {
		return 0, err
	}
	deleted := 0
	for _, reference := range references {
		image, err := r.client.ImageService().Get(runtimeContext(ctx), reference)
		if errdefs.IsNotFound(err) {
			inventory.removeRuntimeImage(reference)
			continue
		}
		if err != nil {
			return deleted, err
		}
		if image.Name != runtimeImageReference(image.Target.Digest) {
			continue
		}
		retained, err := r.runtimeImageRetained(ctx, image.Target.Digest)
		if err == nil && !retained {
			err = r.client.ImageService().Delete(runtimeContext(ctx), image.Name, coreimages.SynchronousDelete())
			if errdefs.IsNotFound(err) {
				err = nil
			}
			if err == nil {
				inventory.removeRuntimeImage(reference)
				deleted++
			}
		}
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (r *Runtime) runtimeImageRetained(ctx context.Context, wanted digest.Digest) (bool, error) {
	if err := r.ensureSourceImagePins(ctx); err != nil {
		return false, err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return false, err
	}
	return inventory.imageDigestReferenced(wanted.String()), nil
}
