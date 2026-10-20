//go:build linux

package control

import (
	"crypto/sha256"
	"errors"
	"fmt"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/protobuf/proto"
)

func importFingerprint(header *vmmdv1.ImportImageHeader) (string, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(header)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

func validateImportHeader(header *vmmdv1.ImportImageHeader, maxArchiveBytes uint64) (string, digest.Digest, error) {
	if header == nil || validateRequestID(header.GetRequestId()) != nil ||
		len(header.GetArchiveSha256()) != 32 || header.GetPlatform() != "linux/arm64" || header.GetArchiveSize() == 0 || header.GetArchiveSize() > maxArchiveBytes {
		return "", "", errors.New("complete bounded linux/arm64 OCI import metadata is required")
	}
	if header.GetSystemImport() {
		if validateSystemNamespace(header.GetActorNamespaceId()) != nil || validateNamespaceLimits(header.GetActorNamespaceId(), header.GetSystemLimits()) != nil || header.GetSystemEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
			return "", "", errors.New("system image import identity is invalid")
		}
	} else if validateID("actor_namespace_id", header.GetActorNamespaceId()) != nil {
		return "", "", errors.New("actor_namespace_id is invalid")
	}
	reference, err := guestimage.NormalizeReference(header.GetImageReference())
	if err != nil || reference != header.GetImageReference() {
		return "", "", errors.New("image_reference must be canonical and fixed")
	}
	manifestDigest, err := digest.Parse(header.GetManifestDigest())
	if err != nil || manifestDigest.Algorithm() != digest.SHA256 {
		return "", "", errors.New("manifest_digest must be a sha256 digest")
	}
	return reference, manifestDigest, nil
}
