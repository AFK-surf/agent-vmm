//go:build darwin

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
)

func TestWriteManifestProducesOnlyCanonicalArtifact(t *testing.T) {
	directory := t.TempDir()
	digest := "sha256:" + strings.Repeat("0", 64)
	if hash := writeManifest(directory, "product", "release", digest); len(hash) != 32 {
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
