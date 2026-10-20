//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"fmt"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
)

// A namespace-owned materialized image needs no shared writer admission. Keep
// its record protected through receipt and retention projection; GC can follow
// on the existing reconciliation path without blocking lifecycle recovery.
func (s *Server) acknowledgeMaterializedImport(ctx context.Context, header *vmmdv1.ImportImageHeader, reference string, target digest.Digest) (*vmmdv1.Image, bool, error) {
	r := s.runtime
	namespace := header.GetActorNamespaceId()
	unlock := r.lockNamespace(namespace)
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
	value, mutationErr := s.mutate(ctx, "import-image", header, func() (any, error) {
		if header.GetSystemImport() {
			if err := r.prepareSystemImageImportLocked(ctx, header, reference, target.String()); err != nil {
				return nil, err
			}
		}
		fingerprint, err := importFingerprint(header)
		if err != nil {
			return nil, err
		}
		requestLabel := fmt.Sprintf("%s%x", importRequestLabelBase, sha256.Sum256([]byte(header.GetRequestId())))
		records, err := r.client.ImageService().List(namespaceContext(ctx, namespace))
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if previous, found := record.Labels[requestLabel]; found && (previous != fingerprint || record.Name != reference || record.Target.Digest != target) {
				return nil, imageError(codes.Aborted, "IMPORT_IDEMPOTENCY_CONFLICT", "import request ID was reused with different input", reference)
			}
		}
		if err := r.commitLogicalImageRecord(namespaceContext(ctx, namespace), logical, nil, reference, logical.Target(), requestLabel, fingerprint); err != nil {
			return nil, err
		}
		if err := r.indexCommittedImage(namespace, reference, target.String()); err != nil {
			return nil, err
		}
		image, err := r.getImageForRole(ctx, namespace, reference, header.GetSystemImport())
		return image, err
	})
	if mutationErr != nil {
		return nil, false, mutationErr
	}
	return value.(*vmmdv1.Image), true, nil
}
