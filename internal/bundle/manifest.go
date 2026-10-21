package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const ManifestName = "manifest.json"

type Manifest struct {
	SchemaVersion int                 `json:"schema_version"`
	Architecture  string              `json:"architecture"`
	MinimumMacOS  string              `json:"minimum_macos"`
	Artifacts     map[string]Artifact `json:"artifacts"`
	RootFS        RootFS              `json:"rootfs"`
}

type Artifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type RootFS struct {
	Filesystem string `json:"filesystem"`
	Compressor string `json:"compressor"`
	BlockSize  int64  `json:"block_size"`
	Dictionary int64  `json:"dictionary_size"`
	Filter     string `json:"filter"`
}

type Verified struct {
	Directory      string
	Manifest       Manifest
	ManifestDigest string
}

// Verify checks the manifest against the digest linked into the Host and
// resolves the named artifacts. The signed bundle covers artifact content, so
// Verify does not hash the artifacts again.
func Verify(directory, expectedManifestDigest string, artifacts ...string) (*Verified, error) {
	if len(expectedManifestDigest) != sha256.Size*2 {
		return nil, errors.New("invalid embedded manifest digest")
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	manifestBytes, err := os.ReadFile(filepath.Join(directory, ManifestName))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	digest := sha256.Sum256(manifestBytes)
	manifestDigest := hex.EncodeToString(digest[:])
	if manifestDigest != strings.ToLower(expectedManifestDigest) {
		return nil, errors.New("guest manifest digest does not match Host")
	}

	var manifest Manifest
	decoder := json.NewDecoder(strings.NewReader(string(manifestBytes)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Architecture != "arm64" || manifest.MinimumMacOS != "14.0" {
		return nil, errors.New("unsupported bundle manifest")
	}
	if manifest.RootFS.Filesystem != "squashfs" || manifest.RootFS.BlockSize != 1<<20 || !supportedRootFS(manifest.RootFS) {
		return nil, errors.New("unsupported rootfs format")
	}
	for _, name := range artifacts {
		artifact, ok := manifest.Artifacts[name]
		if !ok {
			return nil, fmt.Errorf("missing artifact %q", name)
		}
		if err := checkArtifact(directory, artifact); err != nil {
			return nil, fmt.Errorf("verify %s: %w", name, err)
		}
	}
	return &Verified{Directory: directory, Manifest: manifest, ManifestDigest: manifestDigest}, nil
}

func supportedRootFS(root RootFS) bool {
	return root.Compressor == "xz" && root.Dictionary == 1<<20 && root.Filter == "arm64-bcj" ||
		root.Compressor == "zstd" && root.Dictionary == 0 && root.Filter == "none"
}

func checkArtifact(directory string, artifact Artifact) error {
	if artifact.Path == "" || !filepath.IsLocal(artifact.Path) || filepath.Clean(artifact.Path) != artifact.Path || artifact.Path == ManifestName {
		return errors.New("invalid artifact path")
	}
	info, err := os.Stat(filepath.Join(directory, artifact.Path))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != artifact.Size {
		return errors.New("artifact size mismatch")
	}
	return nil
}
