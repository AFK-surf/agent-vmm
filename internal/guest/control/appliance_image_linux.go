//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
)

const (
	bundledMCPImage = "agent-vmm.local/mcp@"
	bundledMCPPath  = "/usr/lib/agent-vmm/mcp-appliance.oci.tar"
	bundledMCPLock  = "/usr/lib/agent-vmm/mcp-appliance.digest"
)

type bundledSystemImage struct {
	prefix, path, lock string
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
		{prefix: bundledMCPImage, path: bundledMCPPath, lock: bundledMCPLock},
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
	locked, err := os.ReadFile(bundled.lock)
	if err != nil || strings.TrimSpace(string(locked)) != manifestDigest.String() {
		return "", "", 0, errors.New("bundled system image does not match the guest lock")
	}
	archive, err := os.Open(bundled.path)
	if err != nil {
		return "", "", 0, err
	}
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil {
		return "", "", 0, err
	}
	manifest, err := validateOCIArchive(archive, manifestDigest)
	if err != nil {
		return "", "", 0, err
	}
	header := &vmmdv1.ImportImageHeader{RequestId: bundledImportRequestID(canonical), ActorNamespaceId: namespaceID,
		ImageReference: canonical, ManifestDigest: manifestDigest.String(),
		Platform: "linux/arm64", ArchiveSize: uint64(info.Size())}
	unlockMaterializer, err := r.lockImportSlot(manifestDigest.String())
	if err != nil {
		return "", "", 0, err
	}
	defer unlockMaterializer()
	reusable, physicalGrowth, reusableTarget, err := r.imageMaterialized(ctx, namespaceID, canonical, manifestDigest, true)
	if err != nil {
		return "", "", 0, err
	}
	if reusable && !physicalGrowth {
		image, err := r.getImageForRole(namespaceContext(ctx, namespaceID), namespaceID, canonical, true)
		if err != nil {
			return "", "", 0, err
		}
		return image.GetReference(), image.GetDigest(), image.GetLogicalBytes(), nil
	}
	reservation, err := r.reserveImport(ctx, header, true)
	if err != nil {
		return "", "", 0, err
	}
	defer reservation.Release()
	var image *vmmdv1.Image
	err = r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return r.guardOCIProjectGrowth(transaction, reservation, func() error {
			if reusableTarget != nil {
				if _, createErr := r.client.ImageService().Create(namespaceContext(ctx, namespaceID), coreimages.Image{Name: canonical, Target: *reusableTarget}); createErr != nil && !errdefs.IsAlreadyExists(createErr) {
					return createErr
				}
			}
			var importErr error
			if reusable {
				image, importErr = r.importImage(ctx, nil, header, canonical, *manifest, true)
			} else {
				image, importErr = r.importImage(ctx, archive, header, canonical, *manifest, true)
			}
			return importErr
		})
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
