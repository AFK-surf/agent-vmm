package appliancecontent

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
)

func testReference() *appliancev1.ApplianceContentReference {
	sha := sha256.Sum256([]byte("archive"))
	manifest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD, ArchiveSha256: sha[:], ArchiveSize: 7, Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + manifest, ImageManifestDigest: manifest}
}

func TestVerifiedMetadataReuseIsIndependentOfManifestRole(t *testing.T) {
	root := t.TempDir()
	content := testReference()
	file, err := os.CreateTemp(root, "stage-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("archive")); err != nil {
		t.Fatal(err)
	}
	// A partial archive has no committed metadata and cannot be reused.
	if err := os.WriteFile(filepath.Join(root, ArchiveName(content)), []byte("archive"), 0444); err != nil {
		t.Fatal(err)
	}
	if available, err := Available(root, content); err != nil || available {
		t.Fatalf("partial available=%v err=%v", available, err)
	}
	if err := Publish(root, file, content); err != nil {
		t.Fatal(err)
	}
	other := proto.Clone(content).(*appliancev1.ApplianceContentReference)
	other.Role = appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_HARNESS
	if available, err := Available(root, other); err != nil || !available {
		t.Fatalf("shared available=%v err=%v", available, err)
	}
	other.ImageReference = "docker.io/library/other@" + other.ImageManifestDigest
	if _, err := Available(root, other); err == nil {
		t.Fatal("conflicting metadata accepted")
	}
	info, err := os.Stat(filepath.Join(root, ArchiveName(content)))
	if err != nil || info.Mode().Perm() != 0444 {
		t.Fatalf("mode=%v err=%v", info, err)
	}
}

func TestManifestAllowsMultipleDigestsPerRoleAndRejectsDuplicateAuthority(t *testing.T) {
	first := testReference()
	second := proto.Clone(first).(*appliancev1.ApplianceContentReference)
	second.ImageManifestDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	second.ImageReference = "docker.io/library/test@" + second.ImageManifestDigest
	second.ArchiveSha256 = bytes.Repeat([]byte{2}, 32)
	manifest := &appliancev1.ControllerApplianceManifest{ContentRefs: []*appliancev1.ApplianceContentReference{first, second}}
	if err := ValidateManifestReferences(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.ContentRefs = append(manifest.ContentRefs, first)
	if err := ValidateManifestReferences(manifest); err == nil {
		t.Fatal("duplicate authority accepted")
	}
}
