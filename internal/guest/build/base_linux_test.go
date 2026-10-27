//go:build linux

package build

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/plugins/content/local"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestExportedBaseLayoutRejectsPathsAndLinks(t *testing.T) {
	for _, test := range []struct {
		name  string
		kind  byte
		valid bool
	}{
		{"oci-layout", tar.TypeReg, true},
		{"blobs/sha256/" + strings.Repeat("a", 64), tar.TypeReg, true},
		{"../escape", tar.TypeReg, false},
		{"blobs/sha256/" + strings.Repeat("a", 64) + "/../escape", tar.TypeReg, false},
		{"index.json", tar.TypeSymlink, false},
		{"index.json", tar.TypeFifo, false},
	} {
		t.Run(test.name+string(test.kind), func(t *testing.T) {
			var content bytes.Buffer
			writer := tar.NewWriter(&content)
			if err := writer.WriteHeader(&tar.Header{Name: test.name, Typeflag: test.kind, Mode: 0644}); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			err := extractBaseLayout(&content, t.TempDir())
			if (err == nil) != test.valid {
				t.Fatalf("layout error %v, valid %v", err, test.valid)
			}
		})
	}
}

type baseImages struct {
	images.Store
	image images.Image
}

func (store baseImages) Get(context.Context, string) (images.Image, error) { return store.image, nil }

func TestPrepareBaseExportsNamespaceScratchWithOccupiedOCICapacity(t *testing.T) {
	ctx := context.Background()
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, RootFS: ocispec.RootFS{Type: "layers"}})
	if err != nil {
		t.Fatal(err)
	}
	configDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	if err := content.WriteBlob(ctx, store, "config", bytes.NewReader(config), configDescriptor); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, Config: configDescriptor})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	if err := content.WriteBlob(ctx, store, "manifest", bytes.NewReader(manifest), descriptor); err != nil {
		t.Fatal(err)
	}
	client, err := containerd.New("", containerd.WithServices(containerd.WithContentStore(store), containerd.WithImageStore(baseImages{image: images.Image{Name: "base", Target: descriptor}})))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	workspace := &Workspace{root: t.TempDir(), header: &vmmdv1.BuildImageHeader{ActorNamespaceId: "owner", BaseImageReference: "base", BaseManifestDigest: descriptor.Digest.String()}, builder: &Builder{client: client, growthGate: func(context.Context, string, func() error) error { return errors.New("OCI capacity occupied") }}}
	if err := workspace.prepareBase(ctx); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(workspace.baseRoot, "blobs", "sha256", descriptor.Digest.Encoded()))
	if err != nil || !bytes.Equal(actual, manifest) {
		t.Fatalf("exported manifest changed: %q, %v", actual, err)
	}
	if _, err := os.Stat(filepath.Join(workspace.root, "base.tar")); !os.IsNotExist(err) {
		t.Fatalf("scratch archive remains: %v", err)
	}
}
