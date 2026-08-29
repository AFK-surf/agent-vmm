//go:build darwin

package vmm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

type BundledApplianceMaterial struct {
	Manifest    []byte
	ImageDigest string
}

func (manager *Manager) ReadVerifiedBundledApplianceMaterial() (BundledApplianceMaterial, error) {
	verified, err := bundle.Verify(manager.config.BundleDirectory, manager.config.GuestManifestDigest)
	if err != nil {
		return BundledApplianceMaterial{}, err
	}
	read := func(name string) ([]byte, error) {
		artifact, ok := verified.Manifest.Artifacts[name]
		if !ok {
			return nil, fmt.Errorf("verified bundle artifact %q is unavailable", name)
		}
		file, err := os.Open(filepath.Join(verified.Directory, artifact.Path))
		if err != nil {
			return nil, err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() != artifact.Size {
			return nil, fmt.Errorf("verified bundle artifact %q size changed", name)
		}
		encoded, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(encoded)
		if hex.EncodeToString(digest[:]) != artifact.SHA256 {
			return nil, fmt.Errorf("verified bundle artifact %q digest changed", name)
		}
		return encoded, nil
	}
	manifest, err := read("mcp_appliance_manifest")
	if err != nil {
		return BundledApplianceMaterial{}, err
	}
	digest, err := read("mcp_appliance_digest")
	if err != nil {
		return BundledApplianceMaterial{}, err
	}
	return BundledApplianceMaterial{Manifest: manifest, ImageDigest: strings.TrimSpace(string(digest))}, nil
}
