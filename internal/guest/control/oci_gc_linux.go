//go:build linux

package control

import (
	"context"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
)

const maxGCDeletes = 32

func (r *Runtime) reclaimOCI(ctx context.Context, limit int) (int, error) {
	return r.reclaimOCIWithMaterializer(ctx, limit, false)
}

func (r *Runtime) reclaimOCIWithMaterializer(ctx context.Context, limit int, materializerHeld bool) (int, error) {
	return r.reclaimOCIWithMaterializerTransaction(ctx, limit, materializerHeld, nil)
}

func (r *Runtime) reclaimOCIWithMaterializerTransaction(ctx context.Context, limit int, materializerHeld bool, _ *guestdisk.Transaction) (int, error) {
	if limit <= 0 || limit > maxGCDeletes {
		limit = maxGCDeletes
	}
	return r.pruneUnreferencedRuntimeImages(ctx, limit, materializerHeld)
}

// Logical image records are retention grants. The rebuildable index selects a
// bounded page of physical runtime images. Each candidate is checked again
// under the import lock. A synchronous delete lets containerd remove only
// content and snapshots that its current reference graph does not use.
func (r *Runtime) pruneUnreferencedRuntimeImages(ctx context.Context, limit int, materializerHeld bool) (int, error) {
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
		retained, err := r.runtimeImageRetained(image.Target.Digest)
		if err != nil {
			return deleted, err
		}
		if retained {
			continue
		}
		unlock := func() {}
		if !materializerHeld {
			unlock = r.materializer.Lock(image.Target.Digest.String())
		}
		retained, err = r.runtimeImageRetained(image.Target.Digest)
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
		unlock()
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (r *Runtime) runtimeImageRetained(wanted digest.Digest) (bool, error) {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return false, err
	}
	return inventory.imageDigestReferenced(wanted.String()), nil
}
