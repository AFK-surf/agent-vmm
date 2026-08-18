//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestValidateOCIArchiveBindsManifestPlatformAndReference(t *testing.T) {
	reference := "example.com/agent/pi:latest"
	archive, manifestDigest := testOCIArchive(t, reference, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	if _, err := validateOCIArchive(bytes.NewReader(archive), manifestDigest, reference); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateImportHeader(&vmmdv1.ImportImageHeader{RequestId: "request", ActorNamespaceId: "namespace", ImageReference: reference,
		ArchiveSha256: make([]byte, 32), ManifestDigest: manifestDigest.String(), Platform: "linux/arm64", ArchiveSize: uint64(len(archive))}, uint64(len(archive))); err != nil {
		t.Fatal(err)
	}
}

func TestValidateOCIArchiveRejectsPlatformReferenceAndUnsafeEntry(t *testing.T) {
	reference := "example.com/agent/pi:latest"
	tests := []struct {
		name     string
		platform ocispec.Platform
		ref      string
		unsafe   bool
	}{
		{name: "platform", platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}, ref: reference},
		{name: "reference", platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, ref: "example.com/other:latest"},
		{name: "unsafe", platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, ref: reference, unsafe: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archive, manifestDigest := testOCIArchive(t, test.ref, test.platform)
			if test.unsafe {
				archive = appendTarEntry(t, archive, "../escape", []byte("bad"))
			}
			if _, err := validateOCIArchive(bytes.NewReader(archive), manifestDigest, reference); err == nil {
				t.Fatal("invalid OCI archive accepted")
			}
		})
	}
}

func TestValidateOCIArchiveRejectsMissingCommittedManifest(t *testing.T) {
	archive, _ := testOCIArchive(t, "example.com/agent/pi:latest", ocispec.Platform{OS: "linux", Architecture: "arm64"})
	if _, err := validateOCIArchive(bytes.NewReader(archive), digest.FromString("missing"), "example.com/agent/pi:latest"); err == nil {
		t.Fatal("missing committed manifest accepted")
	}
}

func testOCIArchive(t *testing.T, reference string, platform ocispec.Platform) ([]byte, digest.Digest) {
	t.Helper()
	config := ocispec.Image{Platform: platform}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configDigest := digest.FromBytes(configBytes)
	manifest := ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: configDigest, Size: int64(len(configBytes))},
		Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: digest.FromString("layer"), Size: 1}}}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifestBytes)
	index := ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: manifestDigest, Size: int64(len(manifestBytes)), Platform: &platform,
			Annotations: map[string]string{ocispec.AnnotationRefName: reference}}}}
	indexBytes, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for name, content := range map[string][]byte{
		"oci-layout": []byte(`{"imageLayoutVersion":"1.0.0"}`),
		"index.json": indexBytes,
		"blobs/sha256/" + manifestDigest.Encoded(): manifestBytes,
		"blobs/sha256/" + configDigest.Encoded():   configBytes,
	} {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), manifestDigest
}

func appendTarEntry(t *testing.T, archive []byte, name string, content []byte) []byte {
	t.Helper()
	trimmed := bytes.TrimSuffix(archive, make([]byte, 1024))
	var output bytes.Buffer
	output.Write(trimmed)
	writer := tar.NewWriter(&output)
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
