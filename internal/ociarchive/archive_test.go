package ociarchive

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"testing"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestSelectSingleManifestCommitsLinuxARM64(t *testing.T) {
	archive, manifest := testApplianceArchive(t, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	got, err := SelectSingleManifest(bytes.NewReader(archive))
	if err != nil || got != manifest {
		t.Fatalf("manifest = %q, %v", got, err)
	}
}

func TestSelectSingleManifestRejectsOtherPlatforms(t *testing.T) {
	for _, platform := range []ocispec.Platform{{OS: "linux", Architecture: "amd64"}, {OS: "linux", Architecture: "arm64", Variant: "v8"}} {
		archive, _ := testApplianceArchive(t, platform)
		if _, err := SelectSingleManifest(bytes.NewReader(archive)); err == nil {
			t.Fatalf("accepted platform %+v", platform)
		}
	}
}

func testApplianceArchive(t *testing.T, platform ocispec.Platform) ([]byte, digest.Digest) {
	t.Helper()
	config := []byte(`{"architecture":"arm64","os":"linux"}`)
	configDigest := digest.FromBytes(config)
	manifest := ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: configDigest, Size: int64(len(config))}}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifestBytes)
	index, err := json.Marshal(ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: manifestDigest, Size: int64(len(manifestBytes)), Platform: &platform}}})
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string][]byte{
		"oci-layout":                               []byte(`{"imageLayoutVersion":"1.0.0"}`),
		"index.json":                               index,
		"blobs/sha256/" + configDigest.Encoded():   config,
		"blobs/sha256/" + manifestDigest.Encoded(): manifestBytes,
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, name := range []string{"oci-layout", "index.json", "blobs/sha256/" + configDigest.Encoded(), "blobs/sha256/" + manifestDigest.Encoded()} {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(entries[name]))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(entries[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes(), manifestDigest
}
