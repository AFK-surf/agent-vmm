//go:build darwin && !managedonly

package main

import (
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteManifestProducesOnlyCanonicalArtifact(t *testing.T) {
	directory := t.TempDir()
	digest := "sha256:" + strings.Repeat("0", 64)
	if hash := writeManifest(directory, "product", "release", &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ImageManifestDigest: digest, ImageReference: "agent-vmm.local/appliance@" + digest, ArchiveSha256: make([]byte, 32), ArchiveSize: 1, Platform: "linux/arm64"}); len(hash) != 32 {
		t.Fatalf("canonical hash length = %d", len(hash))
	}
	data, err := os.ReadFile(filepath.Join(directory, "manifest.pb"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest appliancev1.ControllerApplianceManifest
	if err := proto.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.GetProductId() != "product" || manifest.GetImageManifestDigest() != digest {
		t.Fatalf("manifest identity = product %q digest %q", manifest.GetProductId(), manifest.GetImageManifestDigest())
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "manifest.pb" {
		t.Fatalf("manifest output entries = %v", entries)
	}
}
