//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
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
	hash := sha256.New()
	if _, err := io.Copy(hash, archive); err != nil {
		return "", "", 0, err
	}
	manifest, err := validateOCIArchive(archive, manifestDigest, canonical)
	if err != nil {
		return "", "", 0, err
	}
	archiveHash := hash.Sum(nil)
	header := &vmmdv1.ImportImageHeader{RequestId: bundledImportRequestID(canonical, archiveHash), ActorNamespaceId: namespaceID,
		ImageReference: canonical, ArchiveSha256: archiveHash, ManifestDigest: manifestDigest.String(),
		Platform: "linux/arm64", ArchiveSize: uint64(info.Size())}
	image, err := r.importImage(ctx, archive, header, canonical, *manifest, true)
	if err != nil {
		return "", "", 0, err
	}
	return image.GetReference(), image.GetDigest(), image.GetLogicalBytes(), nil
}

func bundledImportRequestID(imageReference string, archiveHash []byte) string {
	input := make([]byte, 0, len(imageReference)+len(archiveHash))
	input = append(input, imageReference...)
	input = append(input, archiveHash...)
	digest := sha256.Sum256(input)
	return fmt.Sprintf("bundled-mcp-%x", digest[:24])
}
