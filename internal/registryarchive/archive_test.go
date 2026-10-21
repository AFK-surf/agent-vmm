package registryarchive

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
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
			sum := sha256.Sum256(raw)
			if uint64(len(raw)) != archive.Size || string(sum[:]) != string(archive.SHA256) || !strings.HasPrefix(archive.ManifestDigest, "sha256:") {
				t.Fatalf("archive identity = %+v, file bytes=%d", archive, len(raw))
			}
			found := false
			reader := tar.NewReader(strings.NewReader(string(raw)))
			for {
				header, err := reader.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				found = found || header.Name == "blobs/sha256/"+strings.TrimPrefix(archive.ManifestDigest, "sha256:")
			}
			if !found {
				t.Fatal("archive does not contain the committed manifest")
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
	directory := t.TempDir()
	for _, test := range []struct {
		reference string
		limit     uint64
		want      error
	}{{amd64, 8 << 30, ErrPlatformUnavailable}, {arm64, 1024, ErrTooLarge}} {
		if _, err := Export(context.Background(), test.reference, directory, test.limit, nil); !errors.Is(err, test.want) {
			t.Fatalf("Export(%s) error = %v, want %v", test.reference, err, test.want)
		}
	}
	if entries, err := filepath.Glob(filepath.Join(directory, "*")); err != nil || len(entries) != 0 {
		t.Fatalf("failed export left files: %v %v", entries, err)
	}
}
