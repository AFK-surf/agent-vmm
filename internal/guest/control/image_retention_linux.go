//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
)

const builtControllerLabel = labelPrefix + "built-controller"

const retainedScopeLabel = labelPrefix + "retained-controller"
const retainedReferenceLabel = labelPrefix + "retained-reference"

func retainedImageOwner(scope string) string { return "retained:" + scope }
func retainedImageReference(scope, reference, manifest, buildID string) string {
	return fmt.Sprintf("retained-%x", sha256.Sum256([]byte(scope+"\x00"+reference+"\x00"+manifest+"\x00"+buildID)))
}

func (r *Runtime) retainBuiltImage(ctx context.Context, scope, namespace string, receipt *vmmdv1.ImageOperationReceipt) error {
	if scope == "" {
		return nil
	}
	if receipt.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED || !receipt.WriterSettled {
		return errors.New("image retention requires settled successful build")
	}
	unlock, err := r.lockNamespace(ctx, retainedImageOwner(scope))
	if err != nil {
		return err
	}
	defer unlock()
	source, err := r.client.ImageService().Get(namespaceContext(ctx, namespace), receipt.ImageReference)
	if err != nil {
		return err
	}
	if source.Target.Digest.String() != receipt.ManifestDigest {
		return errors.New("retained build output changed")
	}
	if err := setImageOperationReceipt(&source, "build", receipt); err != nil {
		return err
	}
	if _, err := r.client.ImageService().Update(namespaceContext(ctx, namespace), source, "labels"); err != nil {
		return err
	}
	record := coreimages.Image{Name: retainedImageReference(scope, receipt.ImageReference, receipt.ManifestDigest, receipt.RequestId), Target: source.Target,
		Labels: map[string]string{retainedScopeLabel: scope, retainedReferenceLabel: receipt.ImageReference}}
	if err := setImageOperationReceipt(&record, "build", receipt); err != nil {
		return err
	}
	_, err = r.client.ImageService().Create(runtimeContext(ctx), record)
	if errdefs.IsAlreadyExists(err) {
		prior, readErr := r.client.ImageService().Get(runtimeContext(ctx), record.Name)
		if readErr != nil {
			return readErr
		}
		if prior.Target.Digest != record.Target.Digest || prior.Labels[retainedScopeLabel] != scope || prior.Labels[retainedReferenceLabel] != receipt.ImageReference {
			return errors.New("retained image record conflicts with accepted build")
		}
		prior.Labels = record.Labels
		_, err = r.client.ImageService().Update(runtimeContext(ctx), prior, "labels")
	}
	if err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	inventory.ensureNamespace(retainedImageOwner(scope))
	inventory.addRuntimeImage(record.Name)
	return inventory.addImage(retainedImageOwner(scope), record.Name, record.Target.Digest.String())
}

func (r *Runtime) retainedImage(ctx context.Context, scope, reference, manifest, buildID string) (coreimages.Image, error) {
	if scope == "" {
		return coreimages.Image{}, errors.New("retained image controller scope is missing")
	}
	record, err := r.client.ImageService().Get(runtimeContext(ctx), retainedImageReference(scope, reference, manifest, buildID))
	if err != nil {
		return coreimages.Image{}, err
	}
	receipt := imageOperationReceipt(record, "build")
	if record.Labels[retainedScopeLabel] != scope || record.Labels[retainedReferenceLabel] != reference || record.Target.Digest.String() != manifest || receipt == nil || receipt.RequestId != buildID ||
		receipt.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED || !receipt.WriterSettled {
		return coreimages.Image{}, errors.New("retained image has no settled successful receipt")
	}
	return record, nil
}

func (r *Runtime) listRetainedImages(ctx context.Context, scope string, limit uint32) ([]*vmmdv1.Image, error) {
	if scope == "" {
		return nil, errors.New("retained image controller scope is missing")
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return nil, err
	}
	inventory.ensureNamespace(retainedImageOwner(scope))
	references, err := inventory.imageReferences(retainedImageOwner(scope), limit)
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Image, 0, len(references))
	for _, reference := range references {
		record, err := r.client.ImageService().Get(runtimeContext(ctx), reference)
		if err != nil {
			return nil, err
		}
		result = append(result, &vmmdv1.Image{Reference: record.Labels[retainedReferenceLabel], Digest: record.Target.Digest.String(),
			GenerationId: imageGenerationID(record), BuildReceipt: imageOperationReceipt(record, "build"), Platforms: []string{"linux/arm64"}})
	}
	return result, nil
}

func (r *Runtime) deleteRetainedImage(ctx context.Context, scope, reference, generation, manifest string) error {
	canonical, err := normalizeImageReference(reference)
	if err != nil {
		return err
	}
	unlock, err := r.lockNamespace(ctx, retainedImageOwner(scope))
	if err != nil {
		return err
	}
	defer unlock()
	records, err := r.client.ImageService().List(runtimeContext(ctx))
	if err != nil {
		return err
	}
	var record coreimages.Image
	for _, candidate := range records {
		if candidate.Labels[retainedScopeLabel] == scope && candidate.Labels[retainedReferenceLabel] == canonical && candidate.Target.Digest.String() == manifest && imageGenerationID(candidate) == generation {
			record = candidate
			break
		}
	}
	if record.Name == "" {
		return errdefs.ErrNotFound
	}
	if err != nil {
		return err
	}
	if imageGenerationID(record) != generation {
		return errors.New("retained image generation changed")
	}
	if err := r.client.ImageService().Delete(runtimeContext(ctx), record.Name, coreimages.SynchronousDelete()); err != nil {
		return err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	inventory.removeRuntimeImage(record.Name)
	return inventory.removeImage(retainedImageOwner(scope), record.Name)
}

func (r *Runtime) retainSuccessfulBuild(ctx context.Context, namespace string, receipt *vmmdv1.ImageOperationReceipt) error {
	if receipt.Kind != vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD || receipt.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED || !receipt.WriterSettled {
		return nil
	}
	record, err := r.client.ImageService().Get(namespaceContext(ctx, namespace), receipt.ImageReference)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.retainBuiltImage(ctx, record.Labels[builtControllerLabel], namespace, receipt)
}

func (r *Runtime) reclaimRetainedImageCache(ctx context.Context, limit int) (int, error) {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return 0, err
	}
	inventory.mu.RLock()
	hasCache := false
	for owner, images := range inventory.images {
		if strings.HasPrefix(owner, "retained:") && len(images) != 0 {
			hasCache = true
			break
		}
	}
	inventory.mu.RUnlock()
	if !hasCache {
		return 0, nil
	}
	r.gcMu.Lock()
	candidates := inventory.runtimeImagePage(r.gcRetainedImageCursor, limit)
	if len(candidates) == 0 && r.gcRetainedImageCursor != "" {
		r.gcRetainedImageCursor = ""
		candidates = inventory.runtimeImagePage("", limit)
	}
	if len(candidates) != 0 {
		r.gcRetainedImageCursor = candidates[len(candidates)-1]
	}
	r.gcMu.Unlock()
	byScope := make(map[string][]string)
	for _, reference := range candidates {
		record, err := r.client.ImageService().Get(runtimeContext(ctx), reference)
		if errdefs.IsNotFound(err) {
			inventory.removeRuntimeImage(reference)
			continue
		}
		if err != nil {
			return 0, err
		}
		if scope := record.Labels[retainedScopeLabel]; scope != "" {
			byScope[scope] = append(byScope[scope], reference)
		}
	}
	removed := 0
	for scope, candidates := range byScope {
		unlock, available := r.tryLockResource("namespace\x00" + retainedImageOwner(scope))
		if !available {
			continue
		}
		count, err := func() (int, error) {
			defer unlock()
			references, err := r.unsettledImageReferences(ctx)
			if err != nil {
				return 0, err
			}
			pinned := make(map[string]bool, len(references))
			for _, reference := range references {
				pinned[reference.SourceImage] = true
			}
			count := 0
			for _, reference := range candidates {
				if pinned[reference] {
					continue
				}
				record, err := r.client.ImageService().Get(runtimeContext(ctx), reference)
				if errdefs.IsNotFound(err) {
					inventory.removeRuntimeImage(reference)
					continue
				}
				if err != nil {
					return count, err
				}
				err = r.client.ImageService().Delete(runtimeContext(ctx), record.Name, coreimages.SynchronousDelete())
				r.logger.Info("retained image cache reclaimed", "controller_scope", scope, "reference", record.Labels[retainedReferenceLabel], "digest", record.Target.Digest.String(), "request_id", imageOperationReceipt(record, "build").GetRequestId(), "record", record.Name, "succeeded", err == nil, "error", err)
				if err != nil {
					return count, err
				}
				if err := inventory.removeImage(retainedImageOwner(scope), record.Name); err != nil {
					return count, err
				}
				inventory.removeRuntimeImage(record.Name)
				count++
			}
			return count, nil
		}()
		removed += count
		if err != nil {
			return removed, err
		}
	}
	return removed, nil
}
