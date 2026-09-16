//go:build linux

package control

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/protobuf/proto"
)

const maxImportMetadataBytes = 16 << 20

type ociLayout struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

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
		if validateSystemNamespace(header.GetActorNamespaceId()) != nil || validateNamespaceLimits(header.GetSystemLimits(), true) != nil || header.GetSystemEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
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

func validateOCIArchive(archive io.ReadSeeker, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	manifestPath := path.Join("blobs", manifestDigest.Algorithm().String(), manifestDigest.Encoded())
	entries, err := readOCIEntries(archive, map[string]uint64{"oci-layout": 4096, "index.json": maxImportMetadataBytes, manifestPath: maxImportMetadataBytes})
	if err != nil {
		return nil, err
	}
	var layout ociLayout
	if err := json.Unmarshal(entries["oci-layout"], &layout); err != nil || layout.ImageLayoutVersion != "1.0.0" {
		return nil, errors.New("archive has an invalid OCI layout")
	}
	var index ocispec.Index
	if err := json.Unmarshal(entries["index.json"], &index); err != nil || index.SchemaVersion != 2 {
		return nil, errors.New("archive has an invalid OCI index")
	}
	var selected *ocispec.Descriptor
	for position := range index.Manifests {
		descriptor := &index.Manifests[position]
		if descriptor.Digest == manifestDigest {
			if selected != nil {
				return nil, errors.New("OCI index repeats the committed manifest")
			}
			selected = descriptor
		}
	}
	if selected == nil || selected.MediaType != ocispec.MediaTypeImageManifest || selected.Platform == nil ||
		selected.Platform.OS != "linux" || selected.Platform.Architecture != "arm64" || selected.Platform.Variant != "" ||
		selected.Size != int64(len(entries[manifestPath])) || manifestDigest.Algorithm().FromBytes(entries[manifestPath]) != manifestDigest {
		return nil, errors.New("OCI index does not bind the committed linux/arm64 manifest")
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(entries[manifestPath], &manifest); err != nil || manifest.SchemaVersion != 2 || manifest.MediaType != ocispec.MediaTypeImageManifest ||
		manifest.Config.MediaType != ocispec.MediaTypeImageConfig || manifest.Config.Digest.Algorithm() != digest.SHA256 || manifest.Config.Size <= 0 {
		return nil, errors.New("archive has an invalid OCI manifest")
	}
	configPath := path.Join("blobs", manifest.Config.Digest.Algorithm().String(), manifest.Config.Digest.Encoded())
	configEntries, err := readOCIEntries(archive, map[string]uint64{configPath: maxImportMetadataBytes})
	if err != nil {
		return nil, err
	}
	configBytes := configEntries[configPath]
	if manifest.Config.Size != int64(len(configBytes)) || manifest.Config.Digest.Algorithm().FromBytes(configBytes) != manifest.Config.Digest {
		return nil, errors.New("OCI manifest config digest does not match the archive")
	}
	var config ocispec.Image
	if err := json.Unmarshal(configBytes, &config); err != nil || config.OS != "linux" || config.Architecture != "arm64" || config.Variant != "" {
		return nil, errors.New("OCI image config is not linux/arm64")
	}
	for _, layer := range manifest.Layers {
		if layer.Digest.Algorithm() != digest.SHA256 || layer.Size <= 0 || !strings.HasPrefix(layer.MediaType, "application/vnd.oci.image.layer.v1.tar") {
			return nil, errors.New("OCI manifest contains an invalid layer descriptor")
		}
	}
	return selected, nil
}

func readOCIEntries(archive io.ReadSeeker, targets map[string]uint64) (map[string][]byte, error) {
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(targets))
	reader := tar.NewReader(archive)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read OCI archive: %w", err)
		}
		entryName := header.Name
		if header.Typeflag == tar.TypeDir {
			entryName = strings.TrimSuffix(entryName, "/")
		}
		clean := path.Clean(entryName)
		if clean != entryName || path.IsAbs(entryName) || clean == "." || strings.HasPrefix(clean, "../") ||
			(header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA && header.Typeflag != tar.TypeDir) {
			return nil, errors.New("OCI archive contains an unsafe entry")
		}
		limit, wanted := targets[header.Name]
		if !wanted || header.Typeflag == tar.TypeDir {
			continue
		}
		if _, duplicate := result[header.Name]; duplicate || header.Size < 0 || uint64(header.Size) > limit {
			return nil, errors.New("OCI archive metadata is duplicate or oversized")
		}
		content, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
		if err != nil || uint64(len(content)) != uint64(header.Size) {
			return nil, errors.New("OCI archive metadata is truncated")
		}
		result[header.Name] = content
	}
	for target := range targets {
		if result[target] == nil {
			return nil, fmt.Errorf("OCI archive is missing %s", target)
		}
	}
	return result, nil
}
