package ociarchive

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"path"
	"strings"
)

const maxImportMetadataBytes = 16 << 20

type ociLayout struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

func Validate(archive io.ReadSeeker, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
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
	if err := validateLayerBlobs(archive, manifest.Layers); err != nil {
		return nil, err
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

func validateLayerBlobs(archive io.ReadSeeker, layers []ocispec.Descriptor) error {
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	expected := make(map[string]ocispec.Descriptor, len(layers))
	for _, layer := range layers {
		expected[path.Join("blobs", layer.Digest.Algorithm().String(), layer.Digest.Encoded())] = layer
	}
	seen := make(map[string]bool, len(expected))
	reader := tar.NewReader(archive)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		layer, wanted := expected[header.Name]
		if !wanted {
			continue
		}
		if seen[header.Name] || header.Size != layer.Size {
			return errors.New("OCI layer size or uniqueness does not match the manifest")
		}
		verifier := layer.Digest.Verifier()
		if _, err := io.Copy(verifier, reader); err != nil {
			return err
		}
		if !verifier.Verified() {
			return errors.New("OCI layer digest does not match the manifest")
		}
		seen[header.Name] = true
	}
	if len(seen) != len(expected) {
		return errors.New("OCI archive is missing a committed layer")
	}
	return nil
}
