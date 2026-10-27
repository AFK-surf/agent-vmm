//go:build linux

package control

import (
	"context"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
)

// A namespace-owned materialized image needs no shared writer admission. Keep
// its record protected through receipt and retention projection; GC can follow
// on the existing reconciliation path without blocking lifecycle recovery.
func (s *Server) acknowledgeMaterializedImport(ctx context.Context, header *vmmdv1.ImportImageHeader, reference string, target digest.Digest) (*vmmdv1.Image, bool, error) {
	r := s.runtime
	namespace := header.GetActorNamespaceId()
	unlock, err := r.lockNamespace(ctx, namespace)
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespace, header.GetSystemImport()); err != nil {
		return nil, false, err
	}
	logical, err := r.client.GetImage(namespaceContext(ctx, namespace), reference)
	if errdefs.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	reusable, growth, _, err := r.imageMaterializedLocked(ctx, namespace, reference, target, header.GetSystemImport())
	if err != nil || !reusable || growth {
		return nil, false, err
	}
	if header.GetSystemImport() {
		if err := r.prepareSystemImageImportLocked(ctx, header, reference, target.String()); err != nil {
			return nil, false, err
		}
	}
	fingerprint, err := importFingerprint(header)
	if err != nil {
		return nil, false, err
	}
	records, err := r.client.ImageService().List(namespaceContext(ctx, namespace))
	if err != nil {
		return nil, false, err
	}
	if err := checkImportIdempotency(records, header.RequestId, fingerprint, reference, target); err != nil {
		return nil, false, err
	}
	fingerprint = importRequestValue(header.RequestId, fingerprint)
	if err := r.commitLogicalImageRecord(namespaceContext(ctx, namespace), logical, nil, reference, logical.Target(), fingerprint); err != nil {
		return nil, false, err
	}
	if err := r.indexCommittedImage(namespace, reference, target.String()); err != nil {
		return nil, false, err
	}
	image, err := r.getImageForRole(ctx, namespace, reference, header.GetSystemImport())
	return image, err == nil, err
}
