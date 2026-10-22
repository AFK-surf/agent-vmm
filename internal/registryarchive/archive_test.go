package registryarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	digest "github.com/opencontainers/go-digest"
)

func pushImage(t *testing.T, host, repository string, platform v1.Platform) (string, v1.Image) {
	t.Helper()
	image, err := random.Image(1024, 2)
	if err != nil {
		t.Fatal(err)
	}
	config, err := image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config = config.DeepCopy()
	config.OS, config.Architecture, config.Variant = platform.OS, platform.Architecture, platform.Variant
	if image, err = mutate.ConfigFile(image, config); err != nil {
		t.Fatal(err)
	}
	reference := host + "/" + repository + ":test"
	ref, err := name.ParseReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, image); err != nil {
		t.Fatal(err)
	}
	return reference, image
}

func TestExportWritesCommittedArchive(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	directory := t.TempDir()
	for _, test := range []struct {
		name     string
		platform v1.Platform
	}{{"arm64", v1.Platform{OS: "linux", Architecture: "arm64"}}, {"arm64 v8", v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}}} {
		t.Run(test.name, func(t *testing.T) {
			reference, _ := pushImage(t, host, strings.ReplaceAll(test.name, " ", "-"), test.platform)
			archive, err := Export(context.Background(), reference, directory, 8<<30, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(archive.Path)
			raw, err := os.ReadFile(archive.Path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ociarchive.Validate(bytes.NewReader(raw), digest.Digest(archive.ManifestDigest)); err != nil {
				t.Fatalf("exported archive rejected by importer: %v", err)
			}
			sum := sha256.Sum256(raw)
			if uint64(len(raw)) != archive.Size || string(sum[:]) != string(archive.SHA256) || !strings.HasPrefix(archive.ManifestDigest, "sha256:") {
				t.Fatalf("archive identity = %+v, file bytes=%d", archive, len(raw))
			}

		})
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("export left staging files: %v %v", entries, err)
	}
}

func TestExportFailuresLeaveNoFiles(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	amd64, _ := pushImage(t, host, "amd64", v1.Platform{OS: "linux", Architecture: "amd64"})
	arm64, _ := pushImage(t, host, "large", v1.Platform{OS: "linux", Architecture: "arm64"})
	arm64v9, _ := pushImage(t, host, "arm64-v9", v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v9"})
	directory := t.TempDir()
	for _, test := range []struct {
		reference string
		limit     uint64
		want      error
	}{{amd64, 8 << 30, ErrPlatformUnavailable}, {arm64v9, 8 << 30, ErrPlatformUnavailable}, {arm64, 1024, ErrTooLarge}} {
		if _, err := Export(context.Background(), test.reference, directory, test.limit, nil); !errors.Is(err, test.want) {
			t.Fatalf("Export(%s) error = %v, want %v", test.reference, err, test.want)
		}
	}
	if entries, err := filepath.Glob(filepath.Join(directory, "*")); err != nil || len(entries) != 0 {
		t.Fatalf("failed export left files: %v %v", entries, err)
	}
}

func TestExportDockerIndexBindsSelectedConfig(t *testing.T) {
	// Captured from https://quay.io/v2/curl/curl/manifests/latest on 2026-10-22.
	// Replace only the selected descriptor identity with the local image fixture.
	fixture, err := os.ReadFile("testdata/curl-index.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, platform := range []v1.Platform{
		{OS: "linux", Architecture: "arm64", Variant: "v8"},
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64", Variant: "v9"},
	} {
		t.Run(platform.String(), func(t *testing.T) {
			var index v1.IndexManifest
			if err := json.Unmarshal(fixture, &index); err != nil {
				t.Fatal(err)
			}
			var rawIndex []byte
			backend := registry.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/curl/manifests/index" {
					w.Header().Set("Content-Type", string(index.MediaType))
					_, _ = w.Write(rawIndex)
					return
				}
				backend.ServeHTTP(w, r)
			}))
			defer server.Close()
			host := strings.TrimPrefix(server.URL, "http://")
			_, image := pushImage(t, host, "curl", platform)
			hash, err := image.Digest()
			if err != nil {
				t.Fatal(err)
			}
			size, err := image.Size()
			if err != nil {
				t.Fatal(err)
			}
			for i := range index.Manifests {
				if matchesARM64(index.Manifests[i].Platform) {
					index.Manifests[i].Digest = hash
					index.Manifests[i].Size = size
				}
			}
			rawIndex, err = json.Marshal(index)
			if err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			archive, err := Export(context.Background(), host+"/curl:index", directory, 1<<20, nil)
			if platform.Architecture != "arm64" || platform.Variant == "v9" {
				if !errors.Is(err, ErrPlatformUnavailable) {
					t.Fatalf("mismatched index/config error=%v", err)
				}
				entries, readErr := os.ReadDir(directory)
				if readErr != nil || len(entries) != 0 {
					t.Fatalf("rejected image left files: %v, %v", entries, readErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(archive.Path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ociarchive.Validate(bytes.NewReader(data), digest.Digest(archive.ManifestDigest)); err != nil {
				t.Fatalf("Docker arm64/v8 export rejected: %v", err)
			}
		})
	}
}
