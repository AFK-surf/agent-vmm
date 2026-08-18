//go:build linux

package build

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ArchiveResult is the immutable result of a build-only Dockerfile execution.
// The image manifest digest is the OCI descriptor selected from index.json.
type ArchiveResult struct {
	ArchiveSHA256  []byte
	ManifestDigest string
	Size           uint64
	CacheHit       bool
}

type ociLayout struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

// copyOCIArchive verifies that BuildKit emitted exactly one linux/arm64 image
// descriptor and writes the unchanged archive to output while committing its
// byte digest. It deliberately does not accept an OCI index as the image
// result: appliances are always one pinned linux/arm64 image.
func copyOCIArchive(source io.ReadSeeker, output io.Writer) (ArchiveResult, error) {
	manifest, err := applianceManifestDigest(source)
	if err != nil {
		return ArchiveResult{}, err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return ArchiveResult{}, err
	}
	hash := sha256.New()
	count, err := io.Copy(io.MultiWriter(output, hash), source)
	if err != nil {
		return ArchiveResult{}, fmt.Errorf("write OCI archive: %w", err)
	}
	if count <= 0 {
		return ArchiveResult{}, errors.New("OCI archive is empty")
	}
	return ArchiveResult{ArchiveSHA256: hash.Sum(nil), ManifestDigest: manifest.String(), Size: uint64(count)}, nil
}

func applianceManifestDigest(archive io.ReadSeeker) (digest.Digest, error) {
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	reader := tar.NewReader(archive)
	var layoutBytes, indexBytes []byte
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("read OCI archive: %w", err)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA && header.Typeflag != tar.TypeDir {
			return "", errors.New("OCI archive contains an unsafe entry")
		}
		name := strings.TrimSuffix(header.Name, "/")
		if header.Typeflag == tar.TypeDir {
			if name == "" {
				continue
			}
		}
		if path.Clean(name) != name || path.IsAbs(name) || name == "." || strings.HasPrefix(name, "../") {
			return "", errors.New("OCI archive contains an unsafe entry")
		}
		if name != "oci-layout" && name != "index.json" {
			continue
		}
		if header.Size < 0 || header.Size > 1<<20 {
			return "", errors.New("OCI archive metadata is oversized")
		}
		data, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil || int64(len(data)) != header.Size {
			return "", errors.New("OCI archive metadata is truncated")
		}
		if name == "oci-layout" {
			if layoutBytes != nil {
				return "", errors.New("OCI archive repeats oci-layout")
			}
			layoutBytes = data
		} else {
			if indexBytes != nil {
				return "", errors.New("OCI archive repeats index.json")
			}
			indexBytes = data
		}
	}
	var layout ociLayout
	if json.Unmarshal(layoutBytes, &layout) != nil || layout.ImageLayoutVersion != "1.0.0" {
		return "", errors.New("OCI archive has an invalid layout")
	}
	var index ocispec.Index
	if json.Unmarshal(indexBytes, &index) != nil || index.SchemaVersion != 2 || len(index.Manifests) != 1 {
		return "", errors.New("OCI archive must contain exactly one image manifest")
	}
	manifest := index.Manifests[0]
	if manifest.MediaType != ocispec.MediaTypeImageManifest || manifest.Platform == nil || manifest.Platform.OS != "linux" || manifest.Platform.Architecture != "arm64" || manifest.Platform.Variant != "" || manifest.Digest.Algorithm() != digest.SHA256 {
		return "", errors.New("OCI archive does not select a linux/arm64 image manifest")
	}
	return manifest.Digest, nil
}
