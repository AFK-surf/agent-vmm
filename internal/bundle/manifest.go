package bundle

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	ManifestName  = "manifest.json"
	SignatureName = "manifest.json.sig"
)

type Manifest struct {
	SchemaVersion int                  `json:"schema_version"`
	Architecture  string               `json:"architecture"`
	MinimumMacOS  string               `json:"minimum_macos"`
	Signer        string               `json:"signer"`
	Artifacts     map[string]Artifact  `json:"artifacts"`
	Components    map[string]Component `json:"components"`
	RootFS        RootFS               `json:"rootfs"`
}

type Artifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Component struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

type RootFS struct {
	LogicalBytes int64  `json:"logical_bytes"`
	Filesystem   string `json:"filesystem"`
	Compressor   string `json:"compressor"`
	BlockSize    int64  `json:"block_size"`
	Dictionary   int64  `json:"dictionary_size"`
	Filter       string `json:"filter"`
}

type Verified struct {
	Directory      string
	Manifest       Manifest
	ManifestDigest string
}

func Verify(directory string, publicKey ed25519.PublicKey) (*Verified, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("invalid trust root")
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	manifestBytes, err := os.ReadFile(filepath.Join(directory, ManifestName))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	signatureText, err := os.ReadFile(filepath.Join(directory, SignatureName))
	if err != nil {
		return nil, fmt.Errorf("read signature: %w", err)
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureText)))
	if err != nil || !ed25519.Verify(publicKey, manifestBytes, signature) {
		return nil, errors.New("invalid manifest signature")
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
	if manifest.RootFS.Filesystem != "squashfs" || manifest.RootFS.BlockSize != 1<<20 || !supportedRootFS(manifest.Signer, manifest.RootFS) {
		return nil, errors.New("unsupported rootfs format")
	}
	for _, name := range []string{"kernel", "initramfs", "rootfs", "sbom", "provenance", "checksums"} {
		artifact, ok := manifest.Artifacts[name]
		if !ok {
			return nil, fmt.Errorf("missing artifact %q", name)
		}
		if err := verifyArtifact(directory, artifact); err != nil {
			return nil, fmt.Errorf("verify %s: %w", name, err)
		}
	}
	digest := sha256.Sum256(manifestBytes)
	return &Verified{Directory: directory, Manifest: manifest, ManifestDigest: hex.EncodeToString(digest[:])}, nil
}

func supportedRootFS(signer string, root RootFS) bool {
	return signer == "release" && root.Compressor == "xz" && root.Dictionary == 1<<20 && root.Filter == "arm64-bcj" ||
		signer == "development" && root.Compressor == "zstd" && root.Dictionary == 0 && root.Filter == "none"
}

func verifyArtifact(directory string, artifact Artifact) error {
	if artifact.Path == "" || filepath.Base(artifact.Path) != artifact.Path || artifact.Path == ManifestName || artifact.Path == SignatureName {
		return errors.New("invalid artifact path")
	}
	file, err := os.Open(filepath.Join(directory, artifact.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != artifact.Size {
		return errors.New("artifact size mismatch")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
		return errors.New("artifact digest mismatch")
	}
	return nil
}
